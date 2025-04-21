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
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.http.HttpClient;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_SHOULDER_TAP_INVOKER_WAM_SEND_SMS_URL;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_SHOULDER_TAP_INVOKER_WAM_SMS_TRANSACTION_STATUS_URL;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_SHOULDER_TAP_WAM_API_MAX_RETRY_COUNT;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_SHOULDER_TAP_WAM_API_MAX_RETRY_INTERVAL_MS;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_SHOULDER_TAP_WAM_SEND_SMS_SKIP_STATUS_CHECK;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_SHOULDER_TAP_WAM_SMS_PRIORITY;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_SHOULDER_TAP_WAM_SMS_VALIDITY_HOURS;
import static org.eclipse.ecsp.stream.dma.dao.DMAConstants.BIZ_TRANSACTION_ID;


/**
 * ShoulderTapInvokerWAMImpl invokes device shoulder tap SMS request via WAM API.
 *
 * @author KJalawadi
 */

@Component
class ShoulderTapInvokerWAMImpl implements DeviceShoulderTapInvoker {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(ShoulderTapInvokerWAMImpl.class);

    /** The api header key session id. */
    // API header
    private static String apiHeaderKeySessionId = "SessionId";
    
    /** The api header key client request id. */
    private static String apiHeaderKeyClientRequestId = "ClientRequestId";

    /** The api param key vehicle id. */
    // API params
    private static String apiParamKeyVehicleId = "vehicleId";
    
    /** The api param key sms type. */
    private static String apiParamKeySmsType = "smsType";
    
    /** The api param value shoulder tap sms type. */
    private static String apiParamValueShoulderTapSmsType = "SHOULDER_TAP";
    
    /** The api param key additional. */
    private static String apiParamKeyAdditional = "additionalParameters";
    
    /** The api nested param name key. */
    private static String apiNestedParamNameKey = "key";
    
    /** The api nested param name value. */
    private static String apiNestedParamNameValue = "value";
    
    /** The api nested param value priority. */
    private static String apiNestedParamValuePriority = "priority";
    
    /** The api nested param value validity hours. */
    private static String apiNestedParamValueValidityHours = "validityHours";

    /** The api response data key. */
    // API response
    private static String apiResponseDataKey = "data";
    
    /** The api response message key. */
    private static String apiResponseMessageKey = "message";
    
    /** The api response message value success. */
    private static String apiResponseMessageValueSuccess = "SUCCESS";
    
    /** The api response value transid. */
    private static String apiResponseValueTransid = "transactionId";
    
    /** The api response status key. */
    private static String apiResponseStatusKey = "status";

    /** The send sms api http resp code. */
    private static String sendSmsApiHttpRespCode = "202";
    
    /** The trans status api http resp code. */
    private static String transStatusApiHttpRespCode = "200";

    /**
     * The Enum SmsTransactionStatus.
     */
    private enum SmsTransactionStatus {
        
        /** The new. */
        NEW, 
 /** The pending. */
 PENDING, 
 /** The error. */
 ERROR, 
 /** The success. */
 SUCCESS;
    }

    /** The shoulder tap sms transaction id. */
    static String shoulderTapSmsTransactionId = "shoulderTapSMSTransactionId";

    /** The wam send SMS url. */
    @Value("${" + DMA_SHOULDER_TAP_INVOKER_WAM_SEND_SMS_URL + "}")
    private String wamSendSMSUrl;
    
    /** The wam transaction status url. */
    @Value("${" + DMA_SHOULDER_TAP_INVOKER_WAM_SMS_TRANSACTION_STATUS_URL + "}")
    private String wamTransactionStatusUrl;
    
    /** The shoulder tap SMS priority. */
    @Value("${" + DMA_SHOULDER_TAP_WAM_SMS_PRIORITY + "}")
    private String shoulderTapSMSPriority;
    
    /** The shoulder tap SMS validity hours. */
    @Value("${" + DMA_SHOULDER_TAP_WAM_SMS_VALIDITY_HOURS + "}")
    private String shoulderTapSMSValidityHours;
    
    /** The wam send SMS skip status check. */
    @Value("${" + DMA_SHOULDER_TAP_WAM_SEND_SMS_SKIP_STATUS_CHECK + "}")
    private boolean wamSendSMSSkipStatusCheck;
    
    /** The wam API max retry count. */
    @Value("${" + DMA_SHOULDER_TAP_WAM_API_MAX_RETRY_COUNT + "}")
    private int wamAPIMaxRetryCount;
    
    /** The wam API max retry interval ms. */
    @Value("${" + DMA_SHOULDER_TAP_WAM_API_MAX_RETRY_INTERVAL_MS + "}")
    private long wamAPIMaxRetryIntervalMs;
    
    /** The http client. */
    @Autowired
    private HttpClient httpClient;

    /**
     * Inits the.
     */
    @PostConstruct
    public void init() {
        if (StringUtils.isEmpty(wamSendSMSUrl) || StringUtils.isEmpty(wamTransactionStatusUrl)) {
            String msg = String.format("Failed to initialize ShoulderTapInvokerWAMImpl."
                            + " Missing property configuration: %s, %s.",
                    DMA_SHOULDER_TAP_INVOKER_WAM_SEND_SMS_URL, DMA_SHOULDER_TAP_INVOKER_WAM_SMS_TRANSACTION_STATUS_URL);
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Send shoulder tap SMS request to wake up device. This saves the
     * transactionId of the request in extraParameters, which is used to get
     * the SMS delivery status. Send SMS is invoked if the transaction
     * status of the previous request is invalid or error, or if SMS is
     * delivered but device status is still inactive.
     *
     * @param requestId requestId
     * @param vehicleId vehicleId
     * @param extraParameters extraParameters
     *         both in and out parameters
     * @param spc the spc
     * @return true, if successful
     */
    
    @Override
    public boolean sendWakeUpMessage(String requestId, String vehicleId, 
            Map<String, Object> extraParameters, StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        logger.info("Calling WAM Send SMS endpoint: requestId={} vehicleId={} extraParameters={}", 
                requestId, vehicleId, extraParameters);
        boolean wakeUpStatus = false;
        try {
            boolean invokeSendSMS = true;
            if (!wamSendSMSSkipStatusCheck && extraParameters.containsKey(shoulderTapSmsTransactionId)) {
                String transactionId = extraParameters.get(shoulderTapSmsTransactionId).toString();

                logger.debug("Calling WAM endpoint to get the transaction status of already sent shouldertap SMS: "
                        + "wamTransactionStatusUrl={} requestId={} vehicleId={} transactionId={} extraParameters={}", 
                        wamTransactionStatusUrl, requestId, vehicleId, transactionId, extraParameters);

                SmsTransactionStatus smsTransStatus = 
                        getSMSTransactionStatus(requestId, vehicleId, transactionId, extraParameters);

                // Don't send SMS if the transaction status is PENDING.
                // Send SMS again for possible cases:
                // 1) NEW (SMS created but not dispatched)
                // 2) ERROR (API internal/external/network error) or
                // 3) SUCCESS (delivered to device)
                // For #3 above, DeviceShoulderTapRetryHandler
                // retries device wake up until either DeviceStatusService has
                // not signaled ACTIVE device status or max retries attempted.
                if (SmsTransactionStatus.PENDING.equals(smsTransStatus)) {
                    invokeSendSMS = false;
                }
            }

            if (invokeSendSMS) {
                logger.debug("Calling WAM Send SMS endpoint to for shouldertap request: wamSendSMSUrl={} requestId={} "
                        + "-vehicleId={} extraParameters={}", wamSendSMSUrl, requestId, vehicleId, extraParameters);
                Map<String, String> requestHeaders = getRequestHeaders(requestId, extraParameters);
                Map<String, Object> requestParams = getRequestParams(vehicleId);
                Map<String, String> additionalParamPriority = getAddtionalParamPriority();
                
                getAdditionalParamValidityHours(requestParams, additionalParamPriority);
                // Invoke SMS send request
                Map<String, Object> responseData = httpClient.invokeJsonResource(
                        HttpClient.HttpReqMethod.PUT, wamSendSMSUrl,
                        requestHeaders, requestParams, wamAPIMaxRetryCount, wamAPIMaxRetryIntervalMs);
                String responseCode = (String) responseData.get(HttpClient.RESPONSE_CODE);
                JsonNode responseJson = (JsonNode) responseData.get(HttpClient.RESPONSE_JSON);

                if (sendSmsApiHttpRespCode.equals(responseCode) && responseJson != null) {
                    logger.info("Received WAM Send SMS endpoint response: wamSendSMSUrl={} requestHeaders={} "
                            + "requestParams={} responseJson={}", wamSendSMSUrl, requestHeaders, requestParams, 
                            responseJson);
                    String responseMsg = responseJson.findValue(apiResponseMessageKey).asText();
                    String transactionId = null;
                    if (apiResponseMessageValueSuccess.equalsIgnoreCase(responseMsg)) {
                        JsonNode dataNode = responseJson.findPath(apiResponseDataKey);
                        wakeUpStatus = addParamForShoulderTapTxn(dataNode, wakeUpStatus, extraParameters);
                    }
                    logger.debug("WAM Send SMS endpoint called: wamSendSMSUrl={} requestId={} vehicleId={} "
                            + "extraParameters={} responseData={} transactionId={} wakeUpStatus={}", wamSendSMSUrl, 
                            requestId, vehicleId, extraParameters, responseData, transactionId, wakeUpStatus);
                } else {
                    logger.error(
                            "WAM Send SMS request has failed: wamSendSMSUrl={} requestId={} vehicleId={} "
                            + "extraParameters={} responseData={} wakeUpStatus={}", 
                            wamSendSMSUrl, requestId, vehicleId, extraParameters, responseData, wakeUpStatus);
                }
            }
        } catch (Exception e) {
            logger.error("ShoulderTapInvokerWAMImpl has encountered an error while sending wake up message: "
                    + "requestId={} vehicleId={} extraParameters={}  error={}",
                    requestId, vehicleId, extraParameters, e.getMessage());
        }
        return wakeUpStatus;
    }

    /**
     * Gets the additional param validity hours.
     *
     * @param requestParams the request params
     * @param additionalParamPriority the additional param priority
     * @return the additional param validity hours
     */
    private void getAdditionalParamValidityHours(Map<String, Object> requestParams,
            Map<String, String> additionalParamPriority) {
        Map<String, String> additionalParamValidityHours = new HashMap<>();
        additionalParamValidityHours.put(apiNestedParamNameKey, apiNestedParamValueValidityHours);
        // VALDITY HOURS -> 72 hours
        additionalParamValidityHours.put(apiNestedParamNameValue, shoulderTapSMSValidityHours);

        requestParams.put(apiParamKeyAdditional, Arrays.asList(additionalParamPriority,
                additionalParamValidityHours));
    }
    
    /**
     * Gets the addtional param priority.
     *
     * @return the addtional param priority
     */
    private Map<String, String> getAddtionalParamPriority() {
        Map<String, String> additionalParamPriority = new HashMap<>();
        additionalParamPriority.put(apiNestedParamNameKey, apiNestedParamValuePriority);
        // PRIORITY -> HIGH
        additionalParamPriority.put(apiNestedParamNameValue, shoulderTapSMSPriority);
        return additionalParamPriority;
    }
    
    /**
     * Gets the request params.
     *
     * @param vehicleId the vehicle id
     * @return the request params
     */
    private Map<String, Object> getRequestParams(String vehicleId) {
        Map<String, Object> requestParams = new HashMap<>();
        // SMS_TYPE -> SHOULDER_TAP
        requestParams.put(apiParamKeyVehicleId, vehicleId);
        requestParams.put(apiParamKeySmsType, apiParamValueShoulderTapSmsType);
        return requestParams;
    }
    
    /**
     * Gets the request headers.
     *
     * @param requestId the request id
     * @param extraParameters the extra parameters
     * @return the request headers
     */
    private Map<String, String> getRequestHeaders(String requestId, Map<String, Object> extraParameters) {
        Map<String, String> requestHeaders = new HashMap<>();
        // ClientRequestId -> requestId
        requestHeaders.put(apiHeaderKeyClientRequestId, requestId);
        // SessionId -> bizTransactionId
        requestHeaders.put(apiHeaderKeySessionId, extraParameters.get(BIZ_TRANSACTION_ID).toString());
        return requestHeaders;
    }

    /**
     * Adds the param for shoulder tap txn.
     *
     * @param dataNode the data node
     * @param wakeUpStatus the wake up status
     * @param extraParameters the extra parameters
     * @return true, if successful
     */
    private boolean addParamForShoulderTapTxn(JsonNode dataNode, boolean wakeUpStatus, 
            Map<String, Object> extraParameters) {
        if (dataNode != null) {
            JsonNode transIdNode = dataNode.findValue(apiResponseValueTransid);

            if (transIdNode != null) {
                wakeUpStatus = true;
                String transId = transIdNode.asText();
                extraParameters.put(shoulderTapSmsTransactionId, transId);
            }
        }
        return wakeUpStatus;
    }

    /**
     * Get SMS transaction status based on the transactionId.
     *
     * @param requestId requestId
     * @param vehicleId vehicleId
     * @param transactionId transactionId
     * @param extraParameters extraParameters
     * @return the SMS transaction status
     */
    private SmsTransactionStatus getSMSTransactionStatus(String requestId, String vehicleId, String transactionId,
            Map<String, Object> extraParameters) {
        logger.info("Calling WAM SMS Transaction Status endpoint: requestId={} vehicleId={} transactionId={} "
                + "extraParameters={}", requestId, vehicleId, transactionId, extraParameters);

        SmsTransactionStatus transactionStatus = null;
        try {
            String transStatusUrl = wamTransactionStatusUrl
                    + (wamTransactionStatusUrl.endsWith("/") ? "" : "/") + transactionId;

            Map<String, String> requestHeaders = new HashMap<>();
            // ClientRequestId -> requestId
            requestHeaders.put(apiHeaderKeyClientRequestId, requestId);
            // SessionId -> bizTransactionId
            requestHeaders.put(apiHeaderKeySessionId, extraParameters.get(DMAConstants.BIZ_TRANSACTION_ID).toString());

            Map<String, Object> requestBody = new HashMap<>();
            // Invoke SMS Transaction Status request
            Map<String, Object> responseData = httpClient.invokeJsonResource(
                    HttpClient.HttpReqMethod.GET, transStatusUrl, requestHeaders,
                    requestBody, wamAPIMaxRetryCount, wamAPIMaxRetryIntervalMs);

            String responseCode = (String) responseData.get(HttpClient.RESPONSE_CODE);
            JsonNode responseJson = (JsonNode) responseData.get(HttpClient.RESPONSE_JSON);
            if (transStatusApiHttpRespCode.equals(responseCode) && responseJson != null) {
                logger.info("Received WAM Transaction Status endpoint response: transStatusUrl={} requestHeaders={} "
                        + "requestBody={} responseJson={}", transStatusUrl, requestHeaders, requestBody, responseJson);

                String responseMsg = responseJson.findValue(apiResponseMessageKey).asText();

                if (apiResponseMessageValueSuccess.equalsIgnoreCase(responseMsg)) {
                    JsonNode dataNode = responseJson.findPath(apiResponseDataKey);
                    if (dataNode != null) {
                        JsonNode statusNode = dataNode.findValue(apiResponseStatusKey);

                        if (statusNode != null) {
                            String status = statusNode.asText();
                            transactionStatus = SmsTransactionStatus.valueOf(status);
                        }
                    }
                }
                

                logger.debug(
                        "WAM SMS Transaction Status endpoint called: transStatusUrl={} requestId={} vehicleId={} "
                        + "extraParameters={} responseMsg={} transactionId={} transactionStatus={}", transStatusUrl, 
                        requestId, vehicleId, extraParameters, responseMsg, transactionId, transactionStatus);
            } else {
                transactionStatus = SmsTransactionStatus.ERROR;
                logger.error("WAM Transaction Status request has failed: transStatusUrl={} requestId={} vehicleId={} "
                        + "extraParameters={} transactionStatus={}", transStatusUrl, requestId, 
                        vehicleId, extraParameters, transactionStatus);
            }

        } catch (Exception e) {
            transactionStatus = SmsTransactionStatus.ERROR;
            logger.error(
                    "ShoulderTapInvokerWAMImpl has encountered an error while retrieving SMS transaction status: "
                    + "requestId={} vehicleId={} transactionId={} extraParameters={} error={}", requestId, vehicleId, 
                    transactionId, extraParameters, e.getMessage());
        }
        return transactionStatus;
    }
}
