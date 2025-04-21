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

package org.eclipse.ecsp.analytics.stream.vehicleprofile.utils;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.http.HttpClient;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Map;


/**
 * Utility class for {@link org.eclipse.ecsp.vehicleprofile.domain.VehicleProfile} API.
 */
@Component
@Scope("prototype")
public class VehicleProfileClientApiUtil {

    /** The Constant VIN_REGEX. */
    private static final String VIN_REGEX = "\"vin\":\s?\"[a-zA-Z0-9]+\"";
    
    /** The Constant VIN_REPLACEMENT_STRING. */
    private static final String VIN_REPLACEMENT_STRING = "\"vin\":\"******\"";

    /** The http client. */
    @Autowired
    private HttpClient httpClient;

    /** The api url. */
    @Value("${" + PropertyNames.VEHICLE_PROFILE_VIN_URL + ":}")
    private String apiUrl;

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(VehicleProfileClientApiUtil.class);
    /**
     * Invoke the vehicle-profile API for the given deviceId.
     *
     * @param deviceId The Device ID.
     * @return Response.
     */
    
    public String callVehicleProfile(String deviceId) {

        if (StringUtils.isEmpty(apiUrl)) {
            logger.error("No URL configured for vehicle profile API against property : {}. "
                    + "Unable to fetch vehicleId against deviceId : {}", 
                    PropertyNames.VEHICLE_PROFILE_VIN_URL, deviceId);
            return null;
        }
        String url = appendToUrl(deviceId);
        long startTime = System.currentTimeMillis();
        logger.debug("Invoking the Vehicle Profile API with URL: {} for deviceId : {}", url, deviceId);
        Map<String, Object> responseData = httpClient.invokeJsonResource(HttpClient.HttpReqMethod.GET, 
                url, null, null, 0, 0);
        long timeTaken = (System.currentTimeMillis() - startTime) / Constants.THOUSAND;
        logger.debug("Time taken to fetch the VP details for  deviceId : {} is: {} second(s)", deviceId, timeTaken);
        logger.info("Received VP data: {} from the API {} for deviceId : {}", responseData, url, deviceId);
        Gson gson =  new Gson();
        if (responseData == null) {
            logger.error("Response returned by vehicle profile API for deviceId : {} is null");
            return null;
        }
        try {
            String responseJson = responseData.get("responseJson").toString();
            logger.info("Received VP responseJson: {} from the API {} for deviceId: {}",
                        responseJson.replaceAll(VIN_REGEX, VIN_REPLACEMENT_STRING), url, deviceId);
            VehicleProfileData vehicle = gson.fromJson(responseJson, VehicleProfileData.class);
            if (null == vehicle || !vehicle.getMessage().equals("SUCCESS")) {
                logger.error("Vehicle profile data in the response returned by vehicle profile API for "
                        + "deviceId : {} is null or not SUCCESS.");
                return null;
            }
            if (vehicle.getData().isEmpty() || null == vehicle.getData().get(0)) {
                logger.error("No data found in response from vehicle profile API for deviceId : {}");
                return null;
            }
            return vehicle.getData().get(0).getVin();
        } catch (Exception e) {
            logger.error("Error while parsing vehicle profile data - {}", e.getMessage());
            return null;
        }
    }

    /**
     * Append to url.
     *
     * @param deviceId the device id
     * @return the string
     */
    private String appendToUrl(String deviceId) {
        String url = apiUrl;
        url += deviceId;
        return url;
    }
}
