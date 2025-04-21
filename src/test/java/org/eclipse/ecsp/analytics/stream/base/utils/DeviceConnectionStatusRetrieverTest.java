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

import org.eclipse.ecsp.analytics.stream.base.http.HttpClient;
import org.eclipse.ecsp.analytics.stream.base.parser.DeviceConnectionStatusParser;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0.ConnectionStatus;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusAPIInMemoryService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;



/**
 * class DeviceConnectionStatusRetrieverTest.
 */
public class DeviceConnectionStatusRetrieverTest {

    /** The status retriever. */
    @InjectMocks
    private DefaultDeviceConnectionStatusRetriever statusRetriever = new DefaultDeviceConnectionStatusRetriever();

    /** The http client. */
    @Mock
    HttpClient httpClient;

    /** The parser. */
    @Mock
    DeviceConnectionStatusParser parser;

    /** The status service. */
    @Mock
    DeviceStatusAPIInMemoryService statusService;

    /** The ctx. */
    @Mock
    ApplicationContext ctx;

    /**
     * Test get connection status data.
     */
    @Test
    public void testGetConnectionStatusData() {
        httpClient = Mockito.mock(HttpClient.class);
        parser = Mockito.mock(DeviceConnectionStatusParser.class);
        statusService = Mockito.mock(DeviceStatusAPIInMemoryService.class);

        ReflectionTestUtils.setField(statusRetriever, "httpClient", httpClient);
        ReflectionTestUtils.setField(statusRetriever, "parser", parser);
        ReflectionTestUtils.setField(statusRetriever, "apiUrl", "test/url");
        ReflectionTestUtils.setField(statusRetriever, "deviceServiceInMemory", statusService);

        Mockito.when(httpClient.invokeJsonResource(Mockito.any(), Mockito.anyString(),
                Mockito.anyMap(), Mockito.anyMap(), Mockito.anyInt(), Mockito.anyLong())).thenReturn(new HashMap<>());
        Mockito.when(parser.getConnectionStatus(Mockito.anyMap())).thenReturn(DMAConstants.ACTIVE);
        ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
        String requestId = "request1";
        String vehicleId = "vin123";
        String deviceId = "device123";
        map.put(deviceId, ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, map);

        Mockito.when(statusService.get(vehicleId)).thenReturn(mapping);
        mapping = statusRetriever.getConnectionStatusData(requestId, vehicleId, deviceId);
        Assert.assertEquals(DMAConstants.ACTIVE, mapping.getDeviceIds().get(deviceId).getConnectionStatus());
    }

    /**
     * Test append to URL.
     */
    @Test
    public void testAppendToURL() {
        String vehicleId = "vehicle1234";
        String expectedUrl = "http://test-url.com/api/devices/" + vehicleId;
        String actualUrl = "";
        String urlWithForwardSlash = "http://test-url.com/api/devices/";
        ReflectionTestUtils.setField(statusRetriever, "apiUrl", urlWithForwardSlash);
        actualUrl = (String) ReflectionTestUtils.invokeMethod(statusRetriever, "appendToURL", vehicleId);
        Assert.assertEquals(expectedUrl, actualUrl);

        String vehicleId2 = "vin123";
        expectedUrl = "http://test-url.com/api/devices/" + vehicleId2;
        actualUrl = (String) ReflectionTestUtils.invokeMethod(statusRetriever, "appendToURL", vehicleId2);
        Assert.assertEquals(expectedUrl, actualUrl);

        String urlWithoutForwardSlash = "http://test-url.com/api/devices";
        expectedUrl = "http://test-url.com/api/devices/" + vehicleId;
        ReflectionTestUtils.setField(statusRetriever, "apiUrl", urlWithoutForwardSlash);
        actualUrl = (String) ReflectionTestUtils.invokeMethod(statusRetriever, "appendToURL", vehicleId);
        Assert.assertEquals(expectedUrl, actualUrl);
    }

    /**
     * Test get connection status data with empty API url.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetConnectionStatusDataWithEmptyAPIUrl() {
        ReflectionTestUtils.setField(statusRetriever, "apiUrl", "");
        statusRetriever.getConnectionStatusData("requestId", "vehicleId", "deviceId");
    }

    /**
     * Test validate.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testValidate() {
        ReflectionTestUtils.setField(statusRetriever, "apiUrl", "test/url");
        ReflectionTestUtils.setField(statusRetriever, "apiMaxRetryCount", Constants.INT_MINUS_TWO);
        ReflectionTestUtils.setField(statusRetriever, "apiRetryIntervalMs", Constants.INT_MINUS_TWO);
        ReflectionTestUtils.invokeMethod(statusRetriever, "setup", new Object[0]);
    }

    /**
     * Test load connection status parser.
     */
    @Test
    public void testLoadConnectionStatusParser() {
        ctx = Mockito.mock(ApplicationContext.class);
        ReflectionTestUtils.setField(statusRetriever, "ctx", ctx);
        ReflectionTestUtils.invokeMethod(statusRetriever, "setup", new Object[0]);
    }
}
