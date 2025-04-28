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

import org.eclipse.ecsp.analytics.stream.base.http.HttpClient;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;



/**
 * UT class for {@link VehicleProfileClientApiUtil}.
 **/
public class VehicleProfileClientApiUtilTest {

    /** The http client. */
    @Mock
    HttpClient httpClient;
    
    /** The ctx. */
    @Mock
    ApplicationContext ctx;
    
    /** The vehicle profile. */
    @InjectMocks
    private VehicleProfileClientApiUtil vehicleProfile = new VehicleProfileClientApiUtil();

    /**
     * Testcall vehicle profile.
     */
    @Test
    public void testcallVehicleProfile() {
        IgniteEventImpl igniteEvent = new IgniteEventImpl();
        igniteEvent.setVersion(Version.V1_0);
        igniteEvent.setTimestamp(System.currentTimeMillis());
        igniteEvent.setRequestId("Request123");
        igniteEvent.setSourceDeviceId("12345");
        Map<String, Object> responseData = new HashMap<>();
        String jsontest = "{\"message\":\"SUCCESS\",\"data\":"
                + "[{\"provisionedServices\":[{\"applicationId\":\"LOCATION\"},"
                + "{\"applicationId\":\"GENERIC-SETTINGS\"}],\"capabilities\":"
                + "[{\"applicationId\":\"LOCATION\"},{\"applicationId\":\"AOTA\"},"
                + "{\"applicationId\":\"GENERIC-SETTINGS\"}],\"customParams\":null,\"dummy\":true,"
                + "\"authorizedPartners\":null,\"authorizedUsers\":[{\"userId\":\"admin\",\"role\":"
                + "\"VEHICLE_OWNER\",\"source\":null,\"status\":null,\"tc\":null,\"pp\":null,\"createdOn"
                + "\":null,\"updatedOn\":null}],\"vehicleAttributes\":{\"make\":\"NA\",\"model\":\"NA\","
                + "\"marketingColor\":null,\"baseColor\":null,\"modelYear\":\"NA\",\"destinationCountry\":null,"
                + "\"engineType\":null,\"bodyStyle\":null,\"bodyType\":null,\"name\":\"My Car\",\"trim\":null,"
                + "\"type\":\"UNAVAILABLE\",\"fuelType\":null},\"vehicleId\":\"64e7469b51e8d859ee363c61\",\"vin\""
                + ":\"HCPDOIQ5KRKD12458\"}]}";
        responseData.put("responseJson", jsontest);
        httpClient = Mockito.mock(HttpClient.class);
        ReflectionTestUtils.setField(vehicleProfile, "httpClient", httpClient);
        ReflectionTestUtils.setField(vehicleProfile, "apiUrl", "test/url");
        Mockito.when(httpClient.invokeJsonResource(Mockito.any(), Mockito.anyString(),
                Mockito.anyMap(), Mockito.anyMap(), Mockito.anyInt(), Mockito.anyLong())).thenReturn(responseData);
        String expectedVin = (String) ReflectionTestUtils.invokeMethod(vehicleProfile, "callVehicleProfile", "DQ12345");
        String actualVin = "HCPDOIQ";
        Assert.assertNotEquals(actualVin, expectedVin);
    }

    /**
     * Test append to url.
     */
    @Test
    public void testAppendToUrl() {
        String vehicleId = "vehicle1234";
        String expectedUrl = "http://vehicle-profile-api-int-svc:8080/v1.0/vehicles?clientId=" + vehicleId;
        String actualUrl = "";

        String urlWithoutForwardSlash = "http://vehicle-profile-api-int-svc:8080/v1.0/vehicles?clientId=";
        expectedUrl = "http://vehicle-profile-api-int-svc:8080/v1.0/vehicles?clientId=" + vehicleId;
        ReflectionTestUtils.setField(vehicleProfile, "apiUrl", urlWithoutForwardSlash);
        actualUrl = (String) ReflectionTestUtils.invokeMethod(vehicleProfile, "appendToUrl", vehicleId);
        Assert.assertEquals(expectedUrl, actualUrl);
    }
}
