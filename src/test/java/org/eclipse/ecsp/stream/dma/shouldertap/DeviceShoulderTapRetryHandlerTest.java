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

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.idgen.MessageIdGenerator;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.entities.dma.RetryRecord;
import org.eclipse.ecsp.entities.dma.RetryRecordIds;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.ShoulderTapRetryBucketDAO;
import org.eclipse.ecsp.stream.dma.dao.ShoulderTapRetryRecordDAOCacheImpl;
import org.eclipse.ecsp.stream.dma.dao.key.RetryVehicleIdKey;
import org.eclipse.ecsp.stream.dma.dao.key.ShoulderTapRetryBucketKey;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageUtils;
import org.eclipse.ecsp.stream.dma.handler.RetryTestEvent;
import org.eclipse.ecsp.stream.dma.handler.RetryTestKey;
import org.eclipse.ecsp.stream.dma.handler.TestKVIterator;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertTrue;



/**
 * {@link DeviceShoulderTapRetryHandler} UT class {@link DeviceShoulderTapRetryHandlerTest}.
 */
public class DeviceShoulderTapRetryHandlerTest {

    /** The vehicle id. */
    private final String vehicleId = "Vehicle12345";
    
    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    
    /** The shoulder tap retry handler. */
    @InjectMocks
    private DeviceShoulderTapRetryHandler shoulderTapRetryHandler;
    
    /** The shoulder tap retry bucket DAO. */
    @Mock
    private ShoulderTapRetryBucketDAO shoulderTapRetryBucketDAO;
    
    /** The msg id generator. */
    @Mock
    private MessageIdGenerator msgIdGenerator;
    
    /** The device message utils. */
    @Mock
    private DeviceMessageUtils deviceMessageUtils;
    
    /** The shoulder tap retry record DAO. */
    @Mock
    private ShoulderTapRetryRecordDAOCacheImpl shoulderTapRetryRecordDAO;
    
    /** The device shoulder tap invoker. */
    @Mock
    private DeviceShoulderTapInvoker deviceShoulderTapInvoker;
    
    /** The spc. */
    @Mock
    private StreamProcessingContext spc;
    
    /** The event. */
    private RetryTestEvent event;

    /** The retry test key. */
    private RetryTestKey retryTestKey = new RetryTestKey();

    /** The task id. */
    private String taskId = "taskId";

    /** The extra parameters. */
    private Map<String, Object> extraParameters = new HashMap<String, Object>();

    /** The transformer. */
    private DeviceMessageIgniteEventTransformer transformer = new DeviceMessageIgniteEventTransformer();
    
    /** The source topic. */
    private String sourceTopic = "testTopic";

    /**
     * setup(): to set up retry event.
     */
    @Before
    public void setUp() {
        shoulderTapRetryHandler.setServiceName("service");
        retryTestKey.setKey(vehicleId);
        MockitoAnnotations.initMocks(this);
        event = new RetryTestEvent();
        event.setVehicleId(vehicleId);
        event.setRequestId("Req123");
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        // Class classObject =
        // getClass().getClassLoader()
        // .loadClass("org.eclipse.ecsp.stream.dma.shouldertap.DummyShoulderTapInvokerImpl");
        // Mockito.when(ctx.getBean(classObject)).thenReturn((DeviceShoulderTapInvoker)
        // deviceShoulderTapInvoker);
        Mockito.when(deviceShoulderTapInvoker.sendWakeUpMessage("Req123", vehicleId, extraParameters, spc))
                .thenReturn(true);
    }

    /**
     * Test ST retry handle when max retry less than zero.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testSTRetryHandleWhenMaxRetryLessThanZero() throws InterruptedException {
        shoulderTapRetryHandler.close();
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setMaxRetry(Constants.NEGATIVE_ONE);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_500);
        shoulderTapRetryHandler.setRetryInterval(Constants.THREAD_SLEEP_TIME_1000);
        shoulderTapRetryHandler.setup(taskId);

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event),
                Version.V1_0, event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        shoulderTapRetryHandler.registerDevice(retryTestKey, entity, extraParameters);
        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(Mockito.any(DeviceMessageFailureEventDataV1_0.class),
                        Mockito.any(IgniteKey.class), Mockito.any(StreamProcessingContext.class), Mockito.anyString());

        ArgumentCaptor<DeviceMessageFailureEventDataV1_0> failDataArg = ArgumentCaptor
                .forClass(DeviceMessageFailureEventDataV1_0.class);
        Mockito.verify(deviceMessageUtils).postFailureEvent(failDataArg.capture(), Mockito.any(IgniteKey.class),
                Mockito.any(StreamProcessingContext.class), Mockito.anyString());

        DeviceMessageFailureEventDataV1_0 actual = failDataArg.getValue();
        Assert.assertEquals(DeviceMessageErrorCode.RETRYING_SHOULDER_TAP, actual.getErrorCode());
        Assert.assertEquals(0, actual.getShoudlerTapRetryAttempts());
        Assert.assertEquals(0, actual.getRetryAttempts());
        Assert.assertEquals(true, actual.isDeviceStatusInactive());
        Assert.assertEquals(false, actual.isDeviceDeliveryCutoffExceeded());

        Mockito.verify(shoulderTapRetryBucketDAO, Mockito.times(0))
                .update(Mockito.anyString(), Mockito.any(ShoulderTapRetryBucketKey.class),
                Mockito.anyString());
        Mockito.verify(deviceShoulderTapInvoker, Mockito.times(1))
                .sendWakeUpMessage("Req123", vehicleId, extraParameters, spc);

    }

    /**
     * Test setup when retry interval less than retry threshold.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetupWhenRetryIntervalLessThanRetryThreshold() {
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setRetryInterval(TestConstants.THREAD_SLEEP_TIME_100);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_200);
        shoulderTapRetryHandler.setup(taskId);
    }

    /**
     * Test setup when retry interval less than zero.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetupWhenRetryIntervalLessThanZero() {
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setRetryInterval(-TestConstants.THREAD_SLEEP_TIME_100);
        shoulderTapRetryHandler.setup(taskId);
    }

    /**
     * Test setup when retry threshold less than zero.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetupWhenRetryThresholdLessThanZero() {
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setRetryMinThreshold(-Constants.THREAD_SLEEP_TIME_200);
        shoulderTapRetryHandler.setup(taskId);
    }

    /**
     * Test scheduled thread delay.
     */
    @Test
    public void testScheduledThreadDelay() {
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setRetryInterval(TestConstants.THREAD_SLEEP_TIME_40000);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_500);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_4000, shoulderTapRetryHandler.getScheduledThreadDelay());
        shoulderTapRetryHandler.close();
        shoulderTapRetryHandler.setRetryInterval(TestConstants.THREAD_SLEEP_TIME_900);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_500);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_500, shoulderTapRetryHandler.getScheduledThreadDelay());
    }

    /**
     * Test retry handle when max retry threshold has not been reached.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testRetryHandleWhenMaxRetryThresholdHasNotBeenReached() throws InterruptedException {
        ConcurrentHashSet<String> vehicleIds = new ConcurrentHashSet<String>();
        vehicleIds.add(vehicleId);

        // It should be able to retry past keys, hence we are subtracting Constants.TEN
        // seconds
        long currentTime = System.currentTimeMillis() - TestConstants.THREAD_SLEEP_TIME_10000;
        ConcurrentSkipListMap<ShoulderTapRetryBucketKey, RetryRecordIds> map =
                new ConcurrentSkipListMap<ShoulderTapRetryBucketKey, RetryRecordIds>();
        map.put((new ShoulderTapRetryBucketKey(currentTime)), new RetryRecordIds(Version.V1_0, vehicleIds));
        TestKVIterator<ShoulderTapRetryBucketKey, RetryRecordIds> itr =
                new TestKVIterator<ShoulderTapRetryBucketKey, RetryRecordIds>(map);
        Mockito.when(shoulderTapRetryBucketDAO.getHead(Mockito.any(ShoulderTapRetryBucketKey.class)))
                .thenReturn(itr);

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event),
                Version.V1_0, event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        RetryRecord record = new RetryRecord(retryTestKey, entity, currentTime);
        record.addAttempt(currentTime + Constants.TEN);
        record.setExtraParameters(extraParameters);
        Mockito.when(spc.streamName()).thenReturn("topic");
        Mockito.when(shoulderTapRetryRecordDAO.get(Mockito.any(RetryVehicleIdKey.class))).thenReturn(record);

        // Max Retry is set to Constants.TWO here.
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setMaxRetry(Constants.TWO);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_500);
        shoulderTapRetryHandler.setRetryInterval(Constants.THREAD_SLEEP_TIME_1000);
        shoulderTapRetryHandler.setup(taskId);
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_200, MILLISECONDS)).join();
        shoulderTapRetryHandler.close();

        Mockito.verify(shoulderTapRetryRecordDAO, Mockito.times(1))
                .putToMap(Mockito.anyString(), Mockito.any(RetryVehicleIdKey.class),
                Mockito.any(RetryRecord.class), Mockito.any(Optional.class), Mockito.anyString());
        ArgumentCaptor<RetryVehicleIdKey> recordKeyArgument = ArgumentCaptor.forClass(RetryVehicleIdKey.class);
        ArgumentCaptor<RetryRecord> recordArgument = ArgumentCaptor.forClass(RetryRecord.class);
        ArgumentCaptor<String> parentKeyArgument = ArgumentCaptor.forClass(String.class);
        Mockito.verify(shoulderTapRetryRecordDAO).putToMap(parentKeyArgument.capture(), recordKeyArgument.capture(),
                recordArgument.capture(), Mockito.any(Optional.class), Mockito.anyString());
        StringBuilder key = new StringBuilder(Constants.OFFSET_VALUE);
        key.append(DMAConstants.SHOULDER_TAP_RETRY_VEHICLEID).append(DMAConstants.COLON)
                .append("service").append(DMAConstants.COLON)
                .append(taskId);
        RetryVehicleIdKey actualKey = recordKeyArgument.getValue();
        Assert.assertEquals(key.toString(), parentKeyArgument.getValue());
        Assert.assertEquals(vehicleId, actualKey.convertToString());
        RetryRecord actualValue = recordArgument.getValue();
        Assert.assertEquals(event, actualValue.getDeviceMessage().getEvent());
        Assert.assertEquals(Constants.TWO, actualValue.getAttempts());
        Mockito.verify(deviceShoulderTapInvoker, Mockito.times(1))
                .sendWakeUpMessage("Req123", vehicleId, extraParameters, spc);

        ArgumentCaptor<DeviceMessageFailureEventDataV1_0> failDataArg = ArgumentCaptor
                .forClass(DeviceMessageFailureEventDataV1_0.class);
        Mockito.verify(deviceMessageUtils).postFailureEvent(failDataArg.capture(), Mockito.any(IgniteKey.class),
                Mockito.any(StreamProcessingContext.class), Mockito.anyString());

        DeviceMessageFailureEventDataV1_0 actual = failDataArg.getValue();
        Assert.assertEquals(DeviceMessageErrorCode.RETRYING_SHOULDER_TAP, actual.getErrorCode());
        Assert.assertEquals(Constants.TWO, actual.getShoudlerTapRetryAttempts());
        Assert.assertEquals(0, actual.getRetryAttempts());
        Assert.assertEquals(true, actual.isDeviceStatusInactive());
        Assert.assertEquals(false, actual.isDeviceDeliveryCutoffExceeded());
    }

    /**
     * Test retry handle when max retry threshold has reached.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testRetryHandleWhenMaxRetryThresholdHasReached() throws InterruptedException {
        ConcurrentHashSet<String> vehicleIds = new ConcurrentHashSet<String>();
        vehicleIds.add(vehicleId);

        // It should be able to retry past keys, hence we are subtracting Constants.TEN
        // seconds
        long currentTime = System.currentTimeMillis() - TestConstants.THREAD_SLEEP_TIME_10000;
        ConcurrentSkipListMap<ShoulderTapRetryBucketKey, RetryRecordIds> map =
                new ConcurrentSkipListMap<ShoulderTapRetryBucketKey, RetryRecordIds>();
        map.put((new ShoulderTapRetryBucketKey(currentTime)), new RetryRecordIds(Version.V1_0, vehicleIds));
        TestKVIterator<ShoulderTapRetryBucketKey, RetryRecordIds> itr =
                new TestKVIterator<ShoulderTapRetryBucketKey, RetryRecordIds>(map);
        Mockito.when(shoulderTapRetryBucketDAO.getHead(Mockito.any(ShoulderTapRetryBucketKey.class)))
                .thenReturn(itr);

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event),
                Version.V1_0, event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        RetryRecord record = new RetryRecord(retryTestKey, entity, currentTime);
        record.addAttempt(currentTime + Constants.TEN);
        record.addAttempt(currentTime + Constants.TWENTY);
        record.setExtraParameters(extraParameters);
        Mockito.when(spc.streamName()).thenReturn("topic");
        Mockito.when(shoulderTapRetryRecordDAO.get(Mockito.any(RetryVehicleIdKey.class))).thenReturn(record);

        // Max Retry is set to Constants.TWO here.
        int maxRetry = Constants.TWO;
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setMaxRetry(maxRetry);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_500);
        shoulderTapRetryHandler.setRetryInterval(Constants.THREAD_SLEEP_TIME_1000);
        shoulderTapRetryHandler.setup(taskId);
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_200, MILLISECONDS)).join();
        shoulderTapRetryHandler.close();

        Mockito.verify(shoulderTapRetryRecordDAO, Mockito.times(1))
                .deleteFromMap(Mockito.anyString(), Mockito.any(RetryVehicleIdKey.class),
                Mockito.any(Optional.class), Mockito.anyString());
        ArgumentCaptor<RetryVehicleIdKey> recordKeyArgument = ArgumentCaptor
                .forClass(RetryVehicleIdKey.class);
        ArgumentCaptor<String> parentKeyArgument = ArgumentCaptor.forClass(String.class);
        Mockito.verify(shoulderTapRetryRecordDAO).deleteFromMap(parentKeyArgument.capture(),
                recordKeyArgument.capture(),
                Mockito.any(Optional.class), Mockito.anyString());
        RetryVehicleIdKey actualKey = recordKeyArgument.getValue();
        StringBuilder key = new StringBuilder(Constants.OFFSET_VALUE);
        key.append(DMAConstants.SHOULDER_TAP_RETRY_VEHICLEID).append(DMAConstants.COLON)
                .append("service").append(DMAConstants.COLON)
                .append(taskId);
        Assert.assertEquals(key.toString(), parentKeyArgument.getValue());
        Assert.assertEquals(vehicleId, actualKey.convertToString());
        Mockito.verify(deviceShoulderTapInvoker, Mockito.times(0))
                .sendWakeUpMessage("Req123", vehicleId, extraParameters, spc);
        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(Mockito.any(DeviceMessageFailureEventDataV1_0.class),
                Mockito.any(IgniteKey.class), Mockito.any(StreamProcessingContext.class),
                        Mockito.anyString());
        ArgumentCaptor<DeviceMessageFailureEventDataV1_0> failDataArg = ArgumentCaptor
                .forClass(DeviceMessageFailureEventDataV1_0.class);
        Mockito.verify(deviceMessageUtils).postFailureEvent(failDataArg.capture(), Mockito.any(IgniteKey.class),
                Mockito.any(StreamProcessingContext.class), Mockito.anyString());

        DeviceMessageFailureEventDataV1_0 actual = failDataArg.getValue();
        Assert.assertEquals(DeviceMessageErrorCode.SHOULDER_TAP_RETRY_ATTEMPTS_EXCEEDED, actual.getErrorCode());
        Assert.assertEquals(maxRetry, actual.getShoudlerTapRetryAttempts());
        Assert.assertEquals(0, actual.getRetryAttempts());
        Assert.assertEquals(true, actual.isDeviceStatusInactive());
        Assert.assertEquals(false, actual.isDeviceDeliveryCutoffExceeded());
    }

    /**
     * Test deregister device.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testDeregisterDevice() throws InterruptedException {
        shoulderTapRetryHandler.close();
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setMaxRetry(1);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_500);
        shoulderTapRetryHandler.setRetryInterval(Constants.THREAD_SLEEP_TIME_1000);
        shoulderTapRetryHandler.setup(taskId);
        shoulderTapRetryHandler.deregisterDevice(vehicleId);

        Mockito.verify(shoulderTapRetryRecordDAO, Mockito.times(1))
                .deleteFromMap(Mockito.anyString(), Mockito.any(RetryVehicleIdKey.class),
                Mockito.any(Optional.class), Mockito.anyString());

        ArgumentCaptor<RetryVehicleIdKey> recordKeyArgument = ArgumentCaptor.forClass(RetryVehicleIdKey.class);
        ArgumentCaptor<String> parentKeyArgument = ArgumentCaptor.forClass(String.class);
        Mockito.verify(shoulderTapRetryRecordDAO).deleteFromMap(parentKeyArgument.capture(),
                recordKeyArgument.capture(),
                Mockito.any(Optional.class), Mockito.anyString());
        RetryVehicleIdKey actualKey = recordKeyArgument.getValue();

        StringBuilder key = new StringBuilder(Constants.OFFSET_VALUE);
        key.append(DMAConstants.SHOULDER_TAP_RETRY_VEHICLEID).append(DMAConstants.COLON)
                .append("service").append(DMAConstants.COLON)
                .append(taskId);
        Assert.assertEquals(key.toString(), parentKeyArgument.getValue());
        Assert.assertEquals(vehicleId, actualKey.convertToString());

    }

    /**
     * Test set stream processing context.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testSetStreamProcessingContext() throws InterruptedException {
        StreamProcessingContext ctx = Mockito.mock(StreamProcessingContext.class);
        shoulderTapRetryHandler.setStreamProcessingContext(ctx);

        StreamProcessingContext ctxRetrieved = (StreamProcessingContext)
                ReflectionTestUtils.getField(shoulderTapRetryHandler, "spc");
        Assert.assertNotNull(ctxRetrieved);
    }

    /**
     * Test init invalid device shoulder tap invoker impl config.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInitInvalidDeviceShoulderTapInvokerImplConfig() throws InterruptedException {
        String deviceShoulderTapInvoker =
                "org.eclipse.ecsp.stream.dma.shouldertap.ShoulderTapInvokerWAMImp";
        ReflectionTestUtils.setField(shoulderTapRetryHandler,
                "deviceShoulderTapInvokerImplClass", deviceShoulderTapInvoker);

        shoulderTapRetryHandler.init();
    }

    /**
     * Test setup invalid retry interal equal to zero.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetupInvalidRetryInteralEqualToZero() throws InterruptedException {
        ReflectionTestUtils.setField(shoulderTapRetryHandler, "retryIntervalDividend", Constants.TEN);
        ReflectionTestUtils.setField(shoulderTapRetryHandler, "retryMinThreshold", Constants.THREAD_SLEEP_TIME_500);
        ReflectionTestUtils.setField(shoulderTapRetryHandler, "retryInterval", 0);
        shoulderTapRetryHandler.setup(taskId);
    }

    /**
     * Test retry handle when device message vehicle id registered is first attempt.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testRetryHandleWhenDeviceMessageVehicleIdRegisteredIsFirstAttempt() throws InterruptedException {
        shoulderTapRetryHandler.close();
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setMaxRetry(Constants.THREE);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_500);
        shoulderTapRetryHandler.setRetryInterval(Constants.THREAD_SLEEP_TIME_1000);
        shoulderTapRetryHandler.setup(taskId);

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        Mockito.when(shoulderTapRetryRecordDAO.get(Mockito.any(RetryVehicleIdKey.class))).thenReturn(null);

        boolean registered = shoulderTapRetryHandler.registerDevice(retryTestKey, entity, extraParameters);
        assertTrue(registered);

        Mockito.verify(shoulderTapRetryRecordDAO, Mockito.times(1))
                .putToMap(Mockito.anyString(), Mockito.any(RetryVehicleIdKey.class),
                Mockito.any(RetryRecord.class), Mockito.any(Optional.class), Mockito.anyString());
        ArgumentCaptor<RetryVehicleIdKey> recordKeyArgument = ArgumentCaptor.forClass(RetryVehicleIdKey.class);
        ArgumentCaptor<RetryRecord> recordArgument = ArgumentCaptor.forClass(RetryRecord.class);
        ArgumentCaptor<String> parentKeyArgument = ArgumentCaptor.forClass(String.class);
        Mockito.verify(shoulderTapRetryRecordDAO).putToMap(parentKeyArgument.capture(), recordKeyArgument.capture(),
                recordArgument.capture(), Mockito.any(Optional.class), Mockito.anyString());

        RetryVehicleIdKey actualKey = recordKeyArgument.getValue();
        StringBuilder retryRecordKey = new StringBuilder(Constants.OFFSET_VALUE);
        retryRecordKey.append(DMAConstants.SHOULDER_TAP_RETRY_VEHICLEID).append(DMAConstants.COLON).append("service")
                .append(DMAConstants.COLON).append(taskId);
        Assert.assertEquals(retryRecordKey.toString(), parentKeyArgument.getValue());
        RetryRecord actualValue = recordArgument.getValue();
        Assert.assertEquals(vehicleId, actualKey.convertToString());
        Assert.assertEquals(event, actualValue.getDeviceMessage().getEvent());
        Assert.assertEquals(0, actualValue.getAttempts());

        Mockito.verify(shoulderTapRetryBucketDAO, Mockito.times(1)).update(Mockito.anyString(),
                Mockito.any(ShoulderTapRetryBucketKey.class),
                Mockito.any(String.class));

        Mockito.verify(deviceShoulderTapInvoker, Mockito.times(1))
                .sendWakeUpMessage("Req123", vehicleId, extraParameters, spc);

        ArgumentCaptor<DeviceMessageFailureEventDataV1_0> failDataArg = ArgumentCaptor
                .forClass(DeviceMessageFailureEventDataV1_0.class);
        Mockito.verify(deviceMessageUtils).postFailureEvent(failDataArg.capture(), Mockito.any(IgniteKey.class),
                Mockito.any(StreamProcessingContext.class), Mockito.anyString());

        DeviceMessageFailureEventDataV1_0 actual = failDataArg.getValue();
        Assert.assertEquals(DeviceMessageErrorCode.RETRYING_SHOULDER_TAP, actual.getErrorCode());
        Assert.assertEquals(0, actual.getShoudlerTapRetryAttempts());
        Assert.assertEquals(0, actual.getRetryAttempts());
        Assert.assertEquals(true, actual.isDeviceStatusInactive());
        Assert.assertEquals(false, actual.isDeviceDeliveryCutoffExceeded());
    }

    /**
     * Test retry handle when device message vehicle id is already registered.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testRetryHandleWhenDeviceMessageVehicleIdIsAlreadyRegistered() throws InterruptedException {
        shoulderTapRetryHandler.setRetryIntervalDivisor(Constants.TEN);
        shoulderTapRetryHandler.setMaxRetry(Constants.TWO);
        shoulderTapRetryHandler.setRetryMinThreshold(Constants.THREAD_SLEEP_TIME_500);
        shoulderTapRetryHandler.setRetryInterval(Constants.THREAD_SLEEP_TIME_1000);
        shoulderTapRetryHandler.setup(taskId);

        ConcurrentHashSet<String> vehicleIds = new ConcurrentHashSet<String>();
        vehicleIds.add(vehicleId);

        long currentTime = System.currentTimeMillis();
        ConcurrentSkipListMap<ShoulderTapRetryBucketKey, RetryRecordIds> map =
                new ConcurrentSkipListMap<ShoulderTapRetryBucketKey, RetryRecordIds>();
        map.put((new ShoulderTapRetryBucketKey(currentTime)),
                new RetryRecordIds(Version.V1_0, vehicleIds));
        TestKVIterator<ShoulderTapRetryBucketKey, RetryRecordIds> itr =
                new TestKVIterator<ShoulderTapRetryBucketKey, RetryRecordIds>(map);
        Mockito.when(shoulderTapRetryBucketDAO.getHead(Mockito.any(ShoulderTapRetryBucketKey.class)))
                .thenReturn(itr);

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event),
                Version.V1_0, event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        RetryRecord record = new RetryRecord(retryTestKey, entity, currentTime);
        record.addAttempt(currentTime + Constants.TEN);
        record.setExtraParameters(extraParameters);
        Mockito.when(spc.streamName()).thenReturn("topic");
        Mockito.when(shoulderTapRetryRecordDAO.get(Mockito.any(RetryVehicleIdKey.class))).thenReturn(record);

        boolean registered = shoulderTapRetryHandler.registerDevice(retryTestKey, entity, extraParameters);
        assertTrue(registered);
    }

    /**
     * Close.
     */
    @After
    public void close() {
        shoulderTapRetryHandler.close();
    }
}
