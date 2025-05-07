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

package org.eclipse.ecsp.analytics.stream.base.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;



/**
 * class HttpClientTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/stream-base-vehicle-profile-test.properties")
public class HttpClientTest extends KafkaStreamsApplicationTestBase {

    /** The web server. */
    @Rule
    public MockWebServer webServer = new MockWebServer();
    
    /** The http client. */
    @Autowired
    HttpClient httpClient;

    /**
     * Test http client ok.
     */
    @Test
    public void testHttpClientOk() {
        String strResponse = "{\"message\":\"ok\"}";
        webServer.enqueue(new MockResponse().setBody(strResponse));

        JsonNode resNode = httpClient.invokeJsonResource("http://localhost:" + webServer.getPort() + "/");
        Assert.assertEquals(strResponse, resNode.toString());
    }

    /**
     * Test json node.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test
    public void testJsonNode() throws IOException {
        String strResponse = "{\"message\":\"SUCCESS\",\"data\":{\"vin\":\"5A8HR44H08R828625\","
                + "\"vehicleId\":\"5A8HR44H08R828625\",\"createdOn\":\"2018-06-27T01:29:49.225+0000\","
                + "\"updatedOn\":\"2018-06-27T01:29:49.225+0000\",\"productionDate\":"
                + "\"2018-06-26T19:56:37.638+0000\",\"saleDate\":\"2018-06-26T19:56:37.638+0000\""
                + ",\"salesCode\":\"JHG8756\",\"vehicleAttributes\":{\"make\":\"Studebaker\","
                + "\"model\":\"US6\",\"marketingColor\":\"Cherry Blossom\",\"baseColor\":"
                + "\"Pink\",\"destinationCountry\":\"Iceland\",\"engineType\":\"Gasoline\","
                + "\"bodyStyle\":\"Truck\"},\"authorizedUsers\":[{\"oemUserId\":\"sclaus\","
                + "\"role\":\"owner\",\"createdOn\":\"2018-06-26T19:56:37.638+0000\",\"updatedOn"
                + "\":null}],\"modemInfo\":{\"iccid\":\"89 310 410 10 654378930 1\",\"imei\":99000,"
                + "\"msisdn\":\"2484977387\",\"imsi\":30272},\"vehicleArchType\":\"string\","
                + "\"ecus\":[{\"name\":\"TELEMATICS\",\"swVersion\":\"10.0.14\",\"serialNo\":"
                + "\"TBU987979689A\",\"clientId\":\"1234\",\"stockingLogic\":{\"services\""
                + ":[\"SQDF\",\"ECALL\"],\"applications\":[{\"applicationId\":\"Navigation\","
                + "\"version\":\"1.1.2\"}]},\"provisionedServices\":{\"services\":[\"SQDF\"],"
                + "\"applications\":[{\"applicationId\":\"Navigation\",\"version\":\"1.0.2\"}]},"
                + "\"provisioningState\":{\"state\":\"PENDING\",\"datetime\":"
                + "\"2018-06-26T19:56:37.638+0000\"},\"drmBlobSigned\":\"6464A654DF64CDA\","
                + "\"drmType\":\"JAR\",\"productId\":\"JFGJ120987\"},{\"name\":\"HU\","
                + "\"swVersion\":\"10.0.14\",\"serialNo\":\"TBU987979689A\",\"clientId\":\"12345\","
                + "\"stockingLogic\":{\"services\":[\"SQDF\"],\"applications\":"
                + "[{\"applicationId\":\"Navigation\",\"version\":\"1.0.2\"}]},"
                + "\"provisionedServices\":{\"services\":[\"SQDF\"],\"applications\""
                + ":[{\"applicationId\":\"Navigation\",\"version\":\"1.0.2\"}]},\"provisioningState\""
                + ":{\"state\":\"PENDING\",\"datetime\":\"2018-06-26T19:56:37.638+0000\"},"
                + "\"drmBlobSigned\":\"6464A654DF64CDA\",\"drmType\":\"JAR\","
                + "\"productId\":\"JFGJ120987\"}],\"firstTrialDate\":"
                + "\"2018-06-26T19:56:37.638+0000\",\"vehicleType\":\"RENTAL\","
                + "\"subscribedPackages\":[{\"packageName\":\"DEMO\",\"startDate\":"
                + "\"2018-06-26T19:56:37.638+0000\",\"endDate\":\"2018-06-26T19:56:37.638+0000\","
                + "\"subscriptionId\":\"DM-461584\"}],\"schemaVersion\":\"1.0\"}}";

        JsonNode jsonNode = new ObjectMapper().readTree(new StringReader(strResponse));
        JsonNode ecusNode = jsonNode.findValue("ecus");
        for (JsonNode ecus : ecusNode) {
            JsonNode serviceNode = ecus.findValue("services");
            String[] expectedServices = { "SQDF", "ECALL" };
            int i = 0;
            for (JsonNode s : serviceNode) {
                if (s.asText().equals("ECALL")) {
                    System.out.println(ecus.findValuesAsText("clientId"));
                }
                Assert.assertEquals(expectedServices[i++], s.asText());
            }
        }
    }

    /**
     * Test http client GET.
     */
    /*
     * Test GET request with headers and query string
     */
    @Test
    public void testHttpClientGET() {
        MockResponse mockResponse = new MockResponse();
        mockResponse.setResponseCode(Constants.THREAD_SLEEP_TIME_200);

        String strResponse = "{\"message\":\"success\",\"data\":{\"transactionStatus\":\"DELIVERED\"}}";
        mockResponse.setBody(strResponse);

        webServer.enqueue(mockResponse);

        Map<String, String> headers = new HashMap<>();
        headers.put("sessionId", "Session1234");
        headers.put("clientRequestId", "Request1234");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("dummyParam1", "dummyValue1");

        Map<String, Object> resNode = httpClient.invokeJsonResource(HttpClient.HttpReqMethod.GET,
                "http://localhost:" + webServer.getPort() + "/v1.0/m2m/sim/transaction/307631ea-715a-4686-9b2a-d46bf7b99386", headers,
                parameters, Constants.THREE, TestConstants.THREAD_SLEEP_TIME_5000);

        String responseCode = (String) resNode.get(HttpClient.RESPONSE_CODE);
        JsonNode responseJSON = (JsonNode) resNode.get(HttpClient.RESPONSE_JSON);

        Assert.assertEquals("200", responseCode);
        Assert.assertEquals(strResponse, responseJSON.toString());
    }

    /**
     * Test http client GET with retries.
     */
    /*
     * Test GET request with headers and query string
     */
    @Test
    public void testHttpClientGETWithRetries() {
        Map<String, String> headers = new HashMap<>();
        headers.put("sessionId", "Session1234");
        headers.put("clientRequestId", "Request1234");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("dummyParam1", "dummyValue1");

        Map<String, Object> resNode = httpClient.invokeJsonResource(HttpClient.HttpReqMethod.GET,
                "http://localhost:" + webServer.getPort() + "/v1.0/m2m/sim/transaction/307631ea-715a-4686-9b2a-d46bf7b99386", headers,
                parameters, Constants.THREE, TestConstants.THREAD_SLEEP_TIME_5000);

        String responseCode = (String) resNode.get(HttpClient.RESPONSE_CODE);
        JsonNode responseJSON = (JsonNode) resNode.get(HttpClient.RESPONSE_JSON);

        Assert.assertEquals(null, responseCode);
        Assert.assertEquals(null, responseJSON);
    }

    /**
     * Test http client PUT.
     */
    /*
     * Test PUT request with headers, request parameters as sent as JSON string
     */
    @Test
    public void testHttpClientPUT() {
        MockResponse mockResponse = new MockResponse();
        mockResponse.setResponseCode(Constants.INT_202);

        String strResponse = "{\"message\":\"success\",\"data\":{\"transactionId\""
                + ":\"307631ea-715a-4686-9b2a-d46bf7b99386\"}}";
        mockResponse.setBody(strResponse);

        webServer.enqueue(mockResponse);

        Map<String, String> headers = new HashMap<>();
        headers.put("sessionId", "Session1234");
        headers.put("clientRequestId", "Request1234");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("vehicleId", "vehicleId123");
        parameters.put("smsType", "SHOULDER_TAP");

        Map<String, String> addParams = new HashMap<>();
        addParams.put("PRIORITY", "LOW");
        parameters.put("additionalParameters", addParams);

        Map<String, Object> resNode = httpClient.invokeJsonResource(HttpClient.HttpReqMethod.PUT,
                "http://localhost:" + webServer.getPort() + "/v1.0/m2m/sms/send", headers, parameters, Constants.THREE, TestConstants.THREAD_SLEEP_TIME_5000);

        String responseCode = (String) resNode.get(HttpClient.RESPONSE_CODE);
        JsonNode responseJSON = (JsonNode) resNode.get(HttpClient.RESPONSE_JSON);

        Assert.assertEquals("202", responseCode);
        Assert.assertEquals(strResponse, responseJSON.toString());
    }

    /**
     * Test http client POST.
     */
    /*
     * Test POST request with headers, request parameters as sent as JSON string
     */
    @Test
    public void testHttpClientPOST() {
        MockResponse mockResponse = new MockResponse();
        mockResponse.setResponseCode(Constants.INT_202);

        String strResponse = "{\"message\":\"success\",\"data\":"
                + "{\"transactionId\":\"307631ea-715a-4686-9b2a-d46bf7b99386\"}}";
        mockResponse.setBody(strResponse);

        webServer.enqueue(mockResponse);

        Map<String, String> headers = new HashMap<>();
        headers.put("sessionId", "Session1234");
        headers.put("clientRequestId", "Request1234");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("vehicleId", "vehicleId123");
        parameters.put("smsType", "SHOULDER_TAP");

        Map<String, String> addParams = new HashMap<>();
        addParams.put("PRIORITY", "LOW");
        parameters.put("additionalParameters", addParams);

        Map<String, Object> resNode = httpClient.invokeJsonResource(HttpClient.HttpReqMethod.POST,
                "http://localhost:" + webServer.getPort() + "/v1.0/m2m/sms/send", headers, parameters, Constants.THREE, TestConstants.THREAD_SLEEP_TIME_5000);

        String responseCode = (String) resNode.get(HttpClient.RESPONSE_CODE);
        JsonNode responseJSON = (JsonNode) resNode.get(HttpClient.RESPONSE_JSON);

        Assert.assertEquals("202", responseCode);
        Assert.assertEquals(strResponse, responseJSON.toString());
    }
}
