/*
 *
 *
 *   ******************************************************************************
 *
 *    Copyright (c) 2023-24 Harman International
 *
 *
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *
 *    you may not use this file except in compliance with the License.
 *
 *    You may obtain a copy of the License at
 *
 *
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *    Unless required by applicable law or agreed to in writing, software
 *
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *    See the License for the specific language governing permissions and
 *
 *    limitations under the License.
 *
 *
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    *******************************************************************************
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
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusAPIInMemoryService;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_CONNECTION_STATUS_API_MAX_RETRY_COUNT;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_CONNECTION_STATUS_API_RETRY_INTERVAL_MS;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_CONNECTION_STATUS_PARSER_IMPL;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_CONNECTION_STATUS_RETRIEVER_API_URL;

/**
 *         RDNG: 170506, RTC: 433347 DMA should have the capability of
 *         retrieving the connection status of VIN/Device through a third party
 *         API. This class will hit one API which will return the connection status
 *         of a device as response to DMA, if and only if configured so.
 *
 *  @author hbadshah
 */
@Service
@Scope("prototype")
public class DefaultDeviceConnectionStatusRetriever implements ConnectionStatusRetriever {
    
    /** The http client. */
    @Autowired
    private HttpClient httpClient;

    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /** The device service in memory. */
    @Autowired
    private DeviceStatusAPIInMemoryService deviceServiceInMemory;

    /** The api url. */
    @Value("${" + DMA_CONNECTION_STATUS_RETRIEVER_API_URL + ":}")
    private String apiUrl;

    /** The api max retry count. */
    @Value("${" + DMA_CONNECTION_STATUS_API_MAX_RETRY_COUNT + ":3}")
    private int apiMaxRetryCount;

    /** The api retry interval ms. */
    @Value("${" + DMA_CONNECTION_STATUS_API_RETRY_INTERVAL_MS + ":5000}")
    private long apiRetryIntervalMs;

    /** The conn status parser impl. */
    @Value("${" + DMA_CONNECTION_STATUS_PARSER_IMPL + ":}")
    private String connStatusParserImpl;

    /** The parser. */
    private DeviceConnectionStatusParser parser;

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DefaultDeviceConnectionStatusRetriever.class);

    /**
     * getConnectionStatusData().
     *
     * @param requestId requestId
     * @param vehicleId vehicleId
     * @param deviceId deviceId
     * @return  VehicleIdDeviceIdStatus
     */
    public VehicleIdDeviceIdStatus getConnectionStatusData(String requestId, String vehicleId, String deviceId) {
        if (StringUtils.isEmpty(apiUrl)) {
            throw new IllegalArgumentException("No API URL is configured. Will not be "
                    + "able to request connection status.");
        }

        long startTime = System.currentTimeMillis();

        String url = appendToUrl(vehicleId);
        logger.info("Invoking the connection status API with URL: {} for vehicleId: {}", apiUrl, vehicleId);
        // Invoke the API, with no headers and params for now.
        Map<String, Object> responseData = httpClient.invokeJsonResource(HttpClient.HttpReqMethod.GET, url, null,
                null, apiMaxRetryCount, apiRetryIntervalMs);
        long timeTaken = (System.currentTimeMillis() - startTime) / Constants.THOUSAND;
        logger.debug("Time taken to fetch the connection status for vehicleId: {} and deviceId: {} is: {} second(s)",
                vehicleId, deviceId, timeTaken);
        logger.debug("Received connection status data: {} from the API {} for vehicleId: {}, "
                + "deviceId: {} and requestId: {}", responseData, apiUrl, vehicleId, deviceId, requestId);
        String connectionStatus = parser.getConnectionStatus(responseData);
        logger.info("Connection status from the API for vehicleId {} and deviceId {} is {}", 
                vehicleId, deviceId, connectionStatus);
        return getStatusData(vehicleId, deviceId, connectionStatus);
    }

    /**
     * Append to url.
     *
     * @param vehicleId the vehicle id
     * @return the string
     */
    private String appendToUrl(String vehicleId) {
        /*
         * If '/' is already a part of apiUrl configured then just append the
         * vehicleId, else append '/<vehicleId>' to the apiUrl.
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
     * Gets the status data.
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     * @param connectionStatus the connection status
     * @return the status data
     */
    private VehicleIdDeviceIdStatus getStatusData(String vehicleId, String deviceId, String connectionStatus) {
        if (StringUtils.isEmpty(connectionStatus)) {
            return null;
        }
        VehicleIdDeviceIdStatus mapping = deviceServiceInMemory.get(vehicleId);
        if (mapping == null) {
            ConcurrentHashMap<String, ConnectionStatus> statusMappings = new ConcurrentHashMap<>();
            statusMappings.put(deviceId, ConnectionStatus.valueOf(connectionStatus));
            mapping = new VehicleIdDeviceIdStatus(Version.V1_0, statusMappings);
        }
        return mapping;
    }

    /**
     * Setup.
     */
    @PostConstruct
    private void setup() {
        if (StringUtils.isNotEmpty(apiUrl)) {
            validate();
            loadConnectionStatusParser();
        }
    }

    /**
     * Load connection status parser.
     */
    private void loadConnectionStatusParser() {
        Class<?> classObject = null;
        try {
            classObject = getClass().getClassLoader().loadClass(connStatusParserImpl);
            this.parser = (DeviceConnectionStatusParser) ctx.getBean(classObject);
            logger.info("Class {} loaded as DeviceConnectionStatusParser", parser.getClass().getName());
        } catch (Exception e) {
            try {
                if (classObject == null) {
                    throw new IllegalArgumentException("Could not load the class " + connStatusParserImpl);
                }
                this.parser = (DeviceConnectionStatusParser) classObject.getDeclaredConstructor().newInstance();
                logger.info("Class {} loaded as DeviceConnectionStatusParser", parser.getClass().getName());
            } catch (Exception exception) {
                String msg = String.format("Class %s could not be loaded. Not found on classpath.", 
                        connStatusParserImpl);
                logger.error(msg + ExceptionUtils.getStackTrace(exception));
                throw new IllegalArgumentException(msg);
            }
        }
    }

    /**
     * Validate.
     */
    private void validate() {
        if (apiMaxRetryCount < 0) {
            throw new IllegalArgumentException("DMA_CONNECTION_STATUS_API_MAX_RETRY_COUNT cannot be less than 0");
        }
        if (apiRetryIntervalMs < 0) {
            throw new IllegalArgumentException("DMA_CONNECTION_STATUS_API_RETRY_INTERVAL_MS cannot be less than 0");
        }
    }
}
