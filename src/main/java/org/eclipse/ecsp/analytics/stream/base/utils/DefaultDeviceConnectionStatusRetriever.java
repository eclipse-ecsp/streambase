/*
 *
 *
 * ******************************************************************************
 *
 * Copyright (c) 2023-24 Harman International
 *
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 *
 * distributed under the License is distributed on an "AS IS" BASIS,
 *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and
 *
 * limitations under the License.
 *
 *
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * *******************************************************************************
 *
 *
 */

package org.eclipse.ecsp.analytics.stream.base.utils;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.ecsp.analytics.stream.base.http.HttpClient;
import org.eclipse.ecsp.analytics.stream.base.parser.DeviceConnectionStatusParser;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0.ConnectionStatus;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusUtil;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_CONNECTION_STATUS_API_MAX_RETRY_COUNT;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_CONNECTION_STATUS_API_RETRY_INTERVAL_MS;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_CONNECTION_STATUS_PARSER_IMPL;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_CONNECTION_STATUS_RETRIEVER_API_URL;

/**
 * Default implementation of the {@link ConnectionStatusRetriever} interface.
 *
 * <p>
 * This class retrieves the connection status of a VIN/Device through a third-party API.
 * </p>
 *
 * <p>
 * RDNG: 170506, RTC: 433347
 * </p>
 *
 * @author hbadshah
 */
@Service
@Scope("prototype")
public class DefaultDeviceConnectionStatusRetriever implements ConnectionStatusRetriever {

    private static final int MILLISECONDS_IN_SECOND = 1000;

    /**
     * HTTP client used to invoke the connection status API.
     */
    @Autowired
    private HttpClient httpClient;

    /**
     * Spring application context used to load beans dynamically.
     */
    @Autowired
    private ApplicationContext ctx;

    /**
     * Service to retrieve device status from in-memory storage.
     */
    @Qualifier("deviceStatusApiServiceImpl")
    @Autowired
    private DeviceStatusService<VehicleIdDeviceIdStatus> deviceServiceInMemory;

    /**
     * Utility class for device status operations.
     */
    @Autowired
    private DeviceStatusUtil deviceStatusUtil;

    /**
     * URL of the connection status API.
     */
    @Value("${" + DMA_CONNECTION_STATUS_RETRIEVER_API_URL + ":}")
    private String apiUrl;

    /**
     * Maximum number of retry attempts for the connection status API.
     */
    @Value("${" + DMA_CONNECTION_STATUS_API_MAX_RETRY_COUNT + ":3}")
    private int apiMaxRetryCount;

    /**
     * Interval in milliseconds between retry attempts for the connection status API.
     */
    @Value("${" + DMA_CONNECTION_STATUS_API_RETRY_INTERVAL_MS + ":5000}")
    private long apiRetryIntervalMs;

    /**
     * Implementation class name for the connection status parser.
     */
    @Value("${" + DMA_CONNECTION_STATUS_PARSER_IMPL + ":}")
    private String connStatusParserImpl;

    /**
     * Parser to extract connection status from API responses.
     */
    private DeviceConnectionStatusParser parser;

    /**
     * Logger instance for logging messages.
     */
    private static IgniteLogger logger =
            IgniteLoggerFactory.getLogger(DefaultDeviceConnectionStatusRetriever.class);

    /**
     * Retrieves the connection status data for a given vehicle and device.
     *
     * @param requestId Unique identifier for the request.
     * @param vehicleId Identifier of the vehicle.
     * @param deviceId Identifier of the device.
     * @param subService Optional sub-service identifier.
     * @return Connection status data for the vehicle and device.
     */
    @Override
    public VehicleIdDeviceIdStatus getConnectionStatusData(String requestId, String vehicleId,
            String deviceId, Optional<String> subService) {

        if (StringUtils.isEmpty(apiUrl)) {
            throw new IllegalArgumentException("No API URL is configured. Will not be "
                    + "able to request connection status.");
        }
        long startTime = System.currentTimeMillis();

        String url = appendToUrl(vehicleId);
        logger.info("Invoking the connection status API with URL: {} for vehicleId: {}", apiUrl,
                vehicleId);
        // Invoke the API, with no headers and params for now.
        Map<String, Object> responseData =
                httpClient.invokeJsonResource(HttpClient.HttpReqMethod.GET, url, null, null,
                        apiMaxRetryCount, apiRetryIntervalMs);
        long timeTaken = (System.currentTimeMillis() - startTime) / MILLISECONDS_IN_SECOND;
        logger.debug("Time taken to fetch the connection status for vehicleId: {} "
                + " and deviceId: {} is: {} second(s)", vehicleId, deviceId, timeTaken);
        logger.debug(
                "Received connection status data: {} from the API {} for vehicleId: {}, "
                        + " deviceId: {} and requestId: {}",
                responseData, apiUrl, vehicleId, deviceId, requestId);

        String connectionStatus = parser.getConnectionStatus(responseData);
        logger.info("Connection status from the API for vehicleId {} and deviceId {} is {}",
                vehicleId, deviceId, connectionStatus);
        return getStatusData(vehicleId, deviceId, connectionStatus, subService);
    }

    /**
     * Appends the vehicle ID to the API URL.
     *
     * @param vehicleId Identifier of the vehicle.
     * @return Formed API URL with the vehicle ID appended.
     */
    private String appendToUrl(String vehicleId) {
        /*
         * If '/' is already a part of apiUrl configured then just append the vehicleId, else append
         * '/<vehicleId>' to the apiUrl.
         */
        String url = apiUrl;
        if (this.apiUrl.endsWith(String.valueOf(Constants.FORWARD_SLASH))) {
            url += vehicleId;
        } else {
            url += Constants.FORWARD_SLASH + vehicleId;
        }
        logger.debug("Connection status API URL formed is: {}", url);
        return url;
    }

    /**
     * Retrieves the status data for a given vehicle and device.
     *
     * @param vehicleId Identifier of the vehicle.
     * @param deviceId Identifier of the device.
     * @param connectionStatus Connection status of the device.
     * @param subService Optional sub-service identifier.
     * @return Status data for the vehicle and device.
     */
    private VehicleIdDeviceIdStatus getStatusData(String vehicleId, String deviceId,
            String connectionStatus, Optional<String> subService) {
        if (StringUtils.isEmpty(connectionStatus)) {
            return null;
        }
        VehicleIdDeviceIdStatus mapping = deviceServiceInMemory.get(vehicleId, subService);
        if (mapping == null) {
            ConcurrentHashMap<String, ConnectionStatus> statusMappings = new ConcurrentHashMap<>();
            statusMappings.put(deviceId, ConnectionStatus.valueOf(connectionStatus));
            mapping = new VehicleIdDeviceIdStatus(Version.V1_0, statusMappings);
        }
        return mapping;
    }

    /**
     * Initializes the retriever by validating configurations and loading the parser.
     */
    @PostConstruct
    private void setup() {
        if (StringUtils.isNotEmpty(apiUrl)) {
            validate();
            loadConnectionStatusParser();
        }
    }

    /**
     * Loads the connection status parser implementation.
     */
    private void loadConnectionStatusParser() {
        Class<?> classObject = null;
        try {
            classObject = getClass().getClassLoader().loadClass(connStatusParserImpl);
            this.parser = (DeviceConnectionStatusParser) ctx.getBean(classObject);
            logger.info("Class {} loaded as DeviceConnectionStatusParser",
                    parser.getClass().getName());
        } catch (Exception e) {
            try {
                if (classObject == null) {
                    throw new IllegalArgumentException(
                            "Could not load the class " + connStatusParserImpl);
                }
                this.parser = (DeviceConnectionStatusParser) classObject.getDeclaredConstructor()
                        .newInstance();
                logger.info("Class {} loaded as DeviceConnectionStatusParser",
                        parser.getClass().getName());
            } catch (Exception exception) {
                String msg = String.format("Class %s could not be loaded. Not found on classpath.",
                        connStatusParserImpl);
                logger.error(msg + ExceptionUtils.getStackTrace(exception));
                throw new IllegalArgumentException(msg);
            }
        }
    }

    /**
     * Validates the configuration properties for the retriever.
     */
    private void validate() {
        if (apiMaxRetryCount < 0) {
            throw new IllegalArgumentException(
                    "DMA_CONNECTION_STATUS_API_MAX_RETRY_COUNT cannot be less than 0");
        }
        if (apiRetryIntervalMs < 0) {
            throw new IllegalArgumentException(
                    "DMA_CONNECTION_STATUS_API_RETRY_INTERVAL_MS cannot be less than 0");
        }
    }
}
