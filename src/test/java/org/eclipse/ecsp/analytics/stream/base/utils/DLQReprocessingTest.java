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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.IgniteBaseException;
import org.eclipse.ecsp.domain.IgniteExceptionDataV1_1;
import org.eclipse.ecsp.domain.SpeedV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;



/**
 * This class tests the DLQ re-processing logic.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@TestPropertySource("/dlq-reprocessing-test.properties")
public class DLQReprocessingTest<K, V> {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The dlq handler. */
    @InjectMocks
    private DLQHandler dlqHandler;

    /** The value. */
    private IgniteEventImpl value;

    /** The ignite event impl. */
    private IgniteEventImpl igniteEventImpl;

    /** The ignite exception data. */
    private IgniteExceptionDataV1_1 igniteExceptionData;

    /** The retryable ignite base exception. */
    private IgniteBaseException retryableIgniteBaseException;

    /** The non retryable ignite base exception. */
    private IgniteBaseException nonRetryableIgniteBaseException;

    /** The max retyr count. */
    @Value("${" + PropertyNames.DLQ_MAX_RETRY_COUNT + ":5}")
    private int maxRetyrCount = 5;

    /** The service context. */
    private Map<String, Object> serviceContext;

    /** The key. */
    private K key;

    /** The exception message. */
    private String exceptionMessage;

    /** The deatiled exception message. */
    private String deatiledExceptionMessage;

    /** The internal exception. */
    private Exception internalException;

    /** The speed. */
    private SpeedV1_0 speed;

    /** The time zone. */
    private short timeZone;

    /**
     * setUp().
     *
     * @throws Exception Exception
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        timeZone = Constants.TEN;
        exceptionMessage = "Stream closed (through reference chain ...)";
        deatiledExceptionMessage = "Json mapping exception error";
        serviceContext = new HashMap<String, Object>();
        speed = new SpeedV1_0();
        value = new IgniteEventImpl();
        igniteEventImpl = new IgniteEventImpl();
        igniteExceptionData = new IgniteExceptionDataV1_1();
        key = (K) new String("key");
        internalException = new RuntimeException(deatiledExceptionMessage);
        retryableIgniteBaseException = new IgniteBaseException(exceptionMessage, true, internalException, 
             null, serviceContext);
        nonRetryableIgniteBaseException = new IgniteBaseException(exceptionMessage, false, internalException);
        speed.setValue(Constants.TEN);
        serviceContext.put("someProperty", "somePropertyContextInfo");
        value.setEventId(EventID.SPEED);
        value.setVersion(Version.V1_0);
        igniteEventImpl.setEventId(EventID.SPEED);
        igniteEventImpl.setVersion(Version.V1_0);
        igniteEventImpl.setRequestId("Request-1");
        igniteEventImpl.setBizTransactionId("bizTransactionId-1");
        igniteEventImpl.setMessageId("messageId-1");
        igniteEventImpl.setVehicleId("vehicleId-1");
        igniteEventImpl.setTimestamp(System.currentTimeMillis());
        igniteEventImpl.setTimezone(timeZone);
        igniteEventImpl.setEventData(speed);
        igniteExceptionData.setIgniteEvent(igniteEventImpl);
        dlqHandler.setReprocessingEnabled(true);
        dlqHandler.setMaxRetryCount(maxRetyrCount);
    }

    /**
     * Tests success for DLQReprocessing criteria.
     */
    @Test
    public void checkIfDLQReprocessingRequiredTestForSuccess() {
        Assert.assertTrue(dlqHandler.checkIfDLQReprocessingRequired(key,
                igniteEventImpl, retryableIgniteBaseException));
    }

    /**
     * Tests DLQReprocessing criteria where event is retried once.
     */
    @Test
    public void checkIfDLQReprocessingRequiredTestForRetryCountOne() {
        igniteExceptionData.setRetryCount(1);
        value.setEventData(igniteExceptionData);
        dlqHandler.setMaxRetryCount(maxRetyrCount);
        Assert.assertTrue(dlqHandler.checkIfDLQReprocessingRequired(key, value, retryableIgniteBaseException));
    }

    /**
     * Tests for the failed scenario where exception is retried once and doesn't
     * require further re-processing (based on some business
     * logic) or non-retryable exception occurred.
     */
    @Test
    public void checkIfDLQReprocessingRequiredTestForNonRetryableException() {
        igniteExceptionData.setRetryCount(1);
        value.setEventData(igniteExceptionData);
        Assert.assertFalse(dlqHandler.checkIfDLQReprocessingRequired(key, value, nonRetryableIgniteBaseException));
    }

    /**
     * This method tests for DLQ re-processing for null key.
     */
    @Test
    public void testDLQReprocessingForNullKey() {
        igniteExceptionData.setRetryCount(Constants.TWO);
        value.setEventData(igniteExceptionData);
        Assert.assertFalse(dlqHandler.checkIfDLQReprocessingRequired(null, value, retryableIgniteBaseException));
    }

    /**
     * This method tests for DLQ reprocessing for null value.
     */
    @Test
    public void testDLQReprocessingForNullValue() {
        Assert.assertFalse(dlqHandler.checkIfDLQReprocessingRequired(key, null, retryableIgniteBaseException));
    }

    /**
     * Tests the DLQ re-processing where the max retry attempt is exceeded.
     */
    @Test
    public void testDLQReprocessingTestForRetryCountMax() {
        igniteExceptionData.setRetryCount(maxRetyrCount);
        value.setEventData(igniteExceptionData);
        Assert.assertFalse(dlqHandler.checkIfDLQReprocessingRequired(key, value, retryableIgniteBaseException));
    }

    /**
     * This method tests the backward compatibility for the services which are not using this feature.
     */
    @Test
    public void testNonDLQReprocessingExceptionCase() {
        Assert.assertFalse(dlqHandler.checkIfDLQReprocessingRequired(key, value, internalException));
    }

}
