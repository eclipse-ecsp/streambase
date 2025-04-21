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

package org.eclipse.ecsp.stream.dma.shouldertap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.http.HttpClient;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 * test class for {@link ShoulderTapInvokerWAMImpl}.
 */
public class ShoulderTapInvokerWAMImplTest {

    /** The wam send sms url. */
    private static String WAM_SEND_SMS_URL = "https://wam.endpoint.com/v1.0/m2m/sms/send";
    
    /** The wam transaction status url. */
    private static String WAM_TRANSACTION_STATUS_URL = "https://wam.endpoint.com/v1.0/m2m/sim/transaction";

    /** The shoulder tap invoker WAM impl. */
    @InjectMocks
    public ShoulderTapInvokerWAMImpl shoulderTapInvokerWAMImpl;
    
    /** The spc. */
    @Mock
    private StreamProcessingContext spc;

    /** The http client. */
    @Mock
    public HttpClient httpClient;

    /**
     * Sets the up.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test init when WAM send SMS url null.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInitWhenWAMSendSMSUrlNull() {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSUrl", null);
        shoulderTapInvokerWAMImpl.init();
    }

    /**
     * Test init when WAM transaction status url null.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInitWhenWAMTransactionStatusUrlNull() {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamTransactionStatusUrl", null);

        shoulderTapInvokerWAMImpl.init();
    }

    /**
     * Test init when WAM urls not null.
     */
    @Test
    public void testInitWhenWAMUrlsNotNull() {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSUrl", WAM_SEND_SMS_URL);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamTransactionStatusUrl", WAM_TRANSACTION_STATUS_URL);
        Assertions.assertDoesNotThrow(() -> shoulderTapInvokerWAMImpl.init());
    }

    /**
     * Test send wake up message with skip status check.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendWakeUpMessageWithSkipStatusCheck() {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSUrl", WAM_SEND_SMS_URL);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSSkipStatusCheck", true);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamAPIMaxRetryCount", Constants.THREE);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl,
                "wamAPIMaxRetryIntervalMs", TestConstants.THREAD_SLEEP_TIME_5000);

        Map<String, Object> additionalParameters = new HashMap<>();

        Map<String, String> priorityParam = new HashMap<>();
        priorityParam.put("key", "PRIORITY");
        priorityParam.put("value", "HIGH");
        additionalParameters.put("additionalParameters", priorityParam);

        Map<String, Object> responseData = new HashMap<String, Object>();
        responseData.put(HttpClient.RESPONSE_CODE, "202");

        try {
            ObjectMapper responseMapper = new ObjectMapper();
            JsonNode jsonNode = responseMapper.readTree(
                    "{\"message\": \"SUCCESS\",\"failureReasonCode\": null,\"failureReason\": null,"
                            + "\"data\": {\"transactionId\": \"f71e2395-eda2-4de9-ad0a-72e930111736\"}}");
            responseData.put(HttpClient.RESPONSE_JSON, jsonNode);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Mockito.when(httpClient.invokeJsonResource(Mockito.any(HttpClient.HttpReqMethod.class),
                Mockito.any(String.class), Mockito.any(Map.class), Mockito.any(Map.class),
                Mockito.any(Integer.class), Mockito.any(Long.class))).thenReturn(responseData);

        Map<String, Object> extraParameters = new HashMap<>();
        extraParameters.put(DMAConstants.BIZ_TRANSACTION_ID, "bizTransactionId12345");
        String requestId = "Request12345";
        String vehicleId = "Vehicle12345";
        boolean wakeUpStatus = shoulderTapInvokerWAMImpl.sendWakeUpMessage(requestId, vehicleId, extraParameters, spc);
        assertEquals(true, wakeUpStatus);

        Mockito.verify(httpClient, Mockito.times(1)).invokeJsonResource(Mockito.any(HttpClient.HttpReqMethod.class),
                Mockito.any(String.class),
                Mockito.any(Map.class), Mockito.any(Map.class), Mockito.any(Integer.class), Mockito.any(Long.class));
    }

    /**
     * Test send wake up message with skip status check invalid send SMS call.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendWakeUpMessageWithSkipStatusCheckInvalidSendSMSCall() {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSUrl", WAM_SEND_SMS_URL);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSSkipStatusCheck", true);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamAPIMaxRetryCount", TestConstants.THREE);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl,
                "wamAPIMaxRetryIntervalMs", TestConstants.THREAD_SLEEP_TIME_5000);
        Map<String, Object> additionalParameters = new HashMap<>();

        Map<String, String> priorityParam = new HashMap<>();
        priorityParam.put("key", "PRIORITY");
        priorityParam.put("value", "HIGH");
        additionalParameters.put("additionalParameters", priorityParam);

        Map<String, Object> responseData = new HashMap<String, Object>();
        responseData.put(HttpClient.RESPONSE_CODE, "400");

        try {
            ObjectMapper responseMapper = new ObjectMapper();
            JsonNode jsonNode = responseMapper.readTree(
                    "{\"message\": \"FAILURE\",\"Bad Request\": null,\"failureReason\": "
                            + "\"Missing vehicleId parameter\",\"data\": null}");
            responseData.put(HttpClient.RESPONSE_JSON, jsonNode);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Mockito.when(httpClient.invokeJsonResource(Mockito.any(HttpClient.HttpReqMethod.class),
                        Mockito.any(String.class), Mockito.any(Map.class), Mockito.any(Map.class),
                        Mockito.any(Integer.class), Mockito.any(Long.class)))
                .thenReturn(responseData);

        Map<String, Object> extraParameters = new HashMap<>();
        extraParameters.put(DMAConstants.BIZ_TRANSACTION_ID, "bizTransactionId12345");
        String requestId = "Request12345";
        String vehicleId = "";
        boolean wakeUpStatus = shoulderTapInvokerWAMImpl.sendWakeUpMessage(requestId, vehicleId, extraParameters, spc);
        assertEquals(false, wakeUpStatus);

        Mockito.verify(httpClient, Mockito.times(1)).invokeJsonResource(Mockito.any(HttpClient.HttpReqMethod.class),
                Mockito.any(String.class),
                Mockito.any(Map.class), Mockito.any(Map.class), Mockito.any(Integer.class), Mockito.any(Long.class));
    }

    /**
     * Test send wake up message with status as pending.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSendWakeUpMessageWithStatusAsPending() {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSUrl", WAM_SEND_SMS_URL);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamTransactionStatusUrl", WAM_TRANSACTION_STATUS_URL);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSSkipStatusCheck", false);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamAPIMaxRetryCount", Constants.THREE);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl,
                "wamAPIMaxRetryIntervalMs", TestConstants.THREAD_SLEEP_TIME_5000);

        Map<String, Object> additionalParameters = new HashMap<>();

        Map<String, String> priorityParam = new HashMap<>();
        priorityParam.put("key", "PRIORITY");
        priorityParam.put("value", "HIGH");
        additionalParameters.put("additionalParameters", priorityParam);

        Map<String, Object> transactionStatusResponseData = getObjectMap();

        Map<String, Object> sendSMSResponseData = new HashMap<String, Object>();
        sendSMSResponseData.put(HttpClient.RESPONSE_CODE, "202");

        try {
            ObjectMapper responseMapper = new ObjectMapper();
            JsonNode sendSMSJsonNode = responseMapper.readTree(
                    "{\"message\": \"SUCCESS\",\"failureReasonCode\": null,\"failureReason\": null,"
                            + "\"data\": {\"transactionId\": \"f71e2395-eda2-4de9-ad0a-72e930111736\"}}");
            sendSMSResponseData.put(HttpClient.RESPONSE_JSON, sendSMSJsonNode);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Mockito.when(httpClient.invokeJsonResource(
                Mockito.any(HttpClient.HttpReqMethod.class), Mockito.any(String.class),
                        Mockito.any(Map.class), Mockito.any(Map.class),
                        Mockito.any(Integer.class), Mockito.any(Long.class)))
                .then(new Answer() {
                    private int count = 0;

                    public Object answer(InvocationOnMock invocation) {
                        if (++count == 1) {
                            return transactionStatusResponseData;
                        }
                        return sendSMSResponseData;
                    }
                });

        String bizTransactionId = "bizTransactionId12345";
        Map<String, Object> extraParameters = new HashMap<>();
        extraParameters.put(DMAConstants.BIZ_TRANSACTION_ID, bizTransactionId);
        String requestId = "Request12345";
        String vehicleId = "Vehicle12345";
        extraParameters.put(ShoulderTapInvokerWAMImpl.shoulderTapSmsTransactionId, 
                "f71e2395-eda2-4de9-ad0a-72e930111736");
        boolean wakeUpStatus = shoulderTapInvokerWAMImpl.sendWakeUpMessage(requestId, vehicleId, extraParameters, spc);
        assertEquals(false, wakeUpStatus);

        Mockito.verify(httpClient, Mockito.times(1)).invokeJsonResource(Mockito.any(HttpClient.HttpReqMethod.class),
                Mockito.any(String.class),
                Mockito.any(Map.class), Mockito.any(Map.class), Mockito.any(Integer.class), Mockito.any(Long.class));
    }

    /**
     * Gets the object map.
     *
     * @return the object map
     */
    @NotNull
    private static Map<String, Object> getObjectMap() {
        Map<String, Object> transactionStatusResponseData = new HashMap<String, Object>();
        transactionStatusResponseData.put(HttpClient.RESPONSE_CODE, "200");

        try {
            ObjectMapper responseMapper = new ObjectMapper();
            JsonNode transactionStatusJsonNode = responseMapper.readTree(
                    "{ \"message\": \"SUCCESS\", \"failureReasonCode\": null, \"failureReason\": null, "
                            + "\"data\": { \"transactionId\": \"bde7fcda-3b62-46f5-8a24-1bee2e482546\", \"status\": "
                            + "\"PENDING\", \"transitionDate\": \"2018-10-22 10:17:46.758\", \"originalStatus\": null,"
                            + " \"destinationStatus\": null, \"error_code\": null, \"error_msg\": null, \"smsData\": "
                            + "{ \"firstShippingDate\": 1540203339389, \"iccid\": \"8932999901000000003\", "
                            + "\"lastShippingDate\": null, \"text\": "
                            + "\"MEUCIQCr52F+t/rp6OWzwBw+ZBU/dK/u6h/HhV4hGMkRBkOiCQIgS2zdcsy"
                            + "/ZYMhxVXtJj9eLF+vP75aN7vQhd31xtCnRKE=\", "
                            + "\"transactionId\": \"bde7fcda-3b62-46f5-8a24-1bee2e482546\", "
                            + "\"validityHours\": \"36\", \"priority\": "
                            + "\"LOW\", \"schemaVersion\": null } } } ");
            transactionStatusResponseData.put(HttpClient.RESPONSE_JSON, transactionStatusJsonNode);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return transactionStatusResponseData;
    }

    /**
     * Test send wake up message with status as error.
     */
    @Test
    public void testSendWakeUpMessageWithStatusAsError() {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl,
                "wamSendSMSUrl", WAM_SEND_SMS_URL);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl,
                "wamTransactionStatusUrl", WAM_TRANSACTION_STATUS_URL);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl,
                "wamSendSMSSkipStatusCheck", false);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl,
                "wamAPIMaxRetryCount", Constants.THREE);
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl,
                "wamAPIMaxRetryIntervalMs", TestConstants.THREAD_SLEEP_TIME_5000);


        Map<String, Object> additionalParameters = new HashMap<>();

        Map<String, String> priorityParam = new HashMap<>();
        priorityParam.put("key", "PRIORITY");
        priorityParam.put("value", "HIGH");
        additionalParameters.put("additionalParameters", priorityParam);

        Map<String, Object> transactionStatusResponseData = getStringObjectMap();

        Map<String, Object> sendSMSResponseData = new HashMap<String, Object>();
        sendSMSResponseData.put(HttpClient.RESPONSE_CODE, "202");

        try {
            ObjectMapper responseMapper = new ObjectMapper();
            JsonNode sendSMSJsonNode = responseMapper.readTree(
                    "{\"message\": \"SUCCESS\",\"failureReasonCode\": null,\"failureReason\": null,\"data\": "
                            + "{\"transactionId\": \"f71e2395-eda2-4de9-ad0a-72e930111736\"}}");
            sendSMSResponseData.put(HttpClient.RESPONSE_JSON, sendSMSJsonNode);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Mockito.when(httpClient.invokeJsonResource(Mockito.any(HttpClient.HttpReqMethod.class),
                        Mockito.any(String.class), Mockito.any(Map.class), Mockito.any(Map.class),
                        Mockito.any(Integer.class), Mockito.any(Long.class)))
                .then(new Answer() {
                    private int count = 0;

                    public Object answer(InvocationOnMock invocation) {
                        if (++count == 1) {
                            return transactionStatusResponseData;
                        }
                        return sendSMSResponseData;
                    }
                });

        Map<String, Object> extraParameters = new HashMap<>();
        extraParameters.put(DMAConstants.BIZ_TRANSACTION_ID, "bizTransactionId12345");
        String requestId = "Request12345";
        String vehicleId = "Vehicle12345";
        extraParameters.put(ShoulderTapInvokerWAMImpl.shoulderTapSmsTransactionId, 
                "f71e2395-eda2-4de9-ad0a-72e930111736");
        boolean wakeUpStatus = shoulderTapInvokerWAMImpl.sendWakeUpMessage(requestId, vehicleId, extraParameters, spc);
        assertEquals(true, wakeUpStatus);

        Mockito.verify(httpClient, Mockito.times(Constants.TWO))
                .invokeJsonResource(Mockito.any(HttpClient.HttpReqMethod.class), Mockito.any(String.class),
                Mockito.any(Map.class), Mockito.any(Map.class), Mockito.any(Integer.class), Mockito.any(Long.class));
    }

    /**
     * Gets the string object map.
     *
     * @return the string object map
     */
    @NotNull
    private static Map<String, Object> getStringObjectMap() {
        Map<String, Object> transactionStatusResponseData = new HashMap<String, Object>();
        transactionStatusResponseData.put(HttpClient.RESPONSE_CODE, "200");

        try {
            ObjectMapper responseMapper = new ObjectMapper();
            JsonNode transactionStatusJsonNode = responseMapper.readTree(
                    "{ \"message\": \"SUCCESS\", \"failureReasonCode\": null, \"failureReason\": null, \"data\": "
                            + "{ \"transactionId\": \"bde7fcda-3b62-46f5-8a24-1bee2e482546\", \"status\": \"ERROR\", "
                            + "\"transitionDate\": \"2018-10-22 10:17:46.758\", "
                            + "\"originalStatus\": null, \"destinationStatus\": null, "
                            + "\"error_code\": null, \"error_msg\": null, \"smsData\": "
                            + "{ \"firstShippingDate\": 1540203339389, "
                            + "\"iccid\": \"8932999901000000003\", \"lastShippingDate\": null, \"text\": "
                            + "\"MEUCIQCr52F+t/rp6OWzwBw+ZBU/dK/u6h/HhV4hGMkRBkOiCQIgS2zdcsy"
                            + "/ZYMhxVXtJj9eLF+vP75aN7vQhd31xtCnRKE=\", "
                            + "\"transactionId\": \"bde7fcda-3b62-46f5-8a24-1bee2e482546\", "
                            + "\"validityHours\": \"36\", \"priority\": "
                            + "\"LOW\", \"schemaVersion\": null } } } ");
            transactionStatusResponseData.put(HttpClient.RESPONSE_JSON, transactionStatusJsonNode);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return transactionStatusResponseData;
    }
}
