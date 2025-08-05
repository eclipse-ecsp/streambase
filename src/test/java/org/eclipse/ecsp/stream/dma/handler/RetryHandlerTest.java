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

package org.eclipse.ecsp.stream.dma.handler;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.GlobalMessageIdGenerator;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.entities.dma.RetryRecord;
import org.eclipse.ecsp.entities.dma.RetryRecordIds;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.config.EventConfig;
import org.eclipse.ecsp.stream.dma.dao.DMARetryBucketDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.DMARetryRecordDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.dao.key.RetryBucketKey;
import org.eclipse.ecsp.stream.dma.dao.key.RetryRecordKey;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.jetbrains.annotations.NotNull;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;



/**
 * UT class {@link RetryHandlerTest} for {@link RetryHandler}.
 */
public class RetryHandlerTest {
    
    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    
    /** The retry test key. */
    private RetryTestKey retryTestKey = new RetryTestKey();

    /** The max retry. */
    private int maxRetry = 3;

    /** The retry interval. */
    private long retryInterval = 5000;

    /** The task id. */
    private String taskId = "taskId";

    /** The retry min threshold. */
    private int retryMinThreshold = 100;

    /** The retry handler. */
    @InjectMocks
    private RetryHandler retryHandler;

    /** The source topic. */
    private String sourceTopic = "testTopic";
    
    /** The retry bucket DAO. */
    @Mock
    private DMARetryBucketDAOCacheBackedInMemoryImpl retryBucketDAO;

    /** The msg id generator. */
    @Mock
    private GlobalMessageIdGenerator msgIdGenerator;

    /** The device message utils. */
    @Mock
    private DeviceMessageUtils deviceMessageUtils;

    /** The retry event DAO. */
    @Mock
    private DMARetryRecordDAOCacheBackedInMemoryImpl retryEventDAO;

    /** The connection status handler. */
    @Mock
    private DeviceConnectionStatusHandler connectionStatusHandler;

    /** The transformer. */
    private DeviceMessageIgniteEventTransformer transformer = new DeviceMessageIgniteEventTransformer();

    /** The spc. */
    @Mock
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /** The event config map. */
    @Mock
    private ConcurrentMap<String, EventConfig> eventConfigMap = new ConcurrentHashMap<>();

    /** The offline buffer DAO. */
    @Mock
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;

    /** The service name. */
    private String serviceName = "service";

    /**
     * setup().
     */
    @Before
    public void setUp() {
        retryTestKey.setKey("Vehicle12345");
        MockitoAnnotations.initMocks(this);
        retryHandler.close();
        retryHandler.setMaxPollingInterval(TestConstants.TWENTY_FIVE);
        retryHandler.setMaxRetry(maxRetry);
        retryHandler.setup(taskId);
    }

    /**
     * Close.
     */
    @After
    public void close() {
        retryHandler.close();
    }

    /**
     * Test retry handle when max retry less than zero.
     */
    @Test
    public void testRetryHandleWhenMaxRetryLessThanZero() {
        TestHandler handler = new TestHandler();
        retryHandler.close();
        retryHandler.setMaxRetry(Constants.INT_MINUS_ONE);
        retryHandler.setNextHandler(handler);
        retryHandler.setup(taskId);

        RetryTestEvent event = new RetryTestEvent();
        event.setVehicleId("vehicleId");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, 
                entity.getDeviceMessageHeader(), "vehicleId"))
                .thenReturn(Optional.of("vehicleId"));
        retryHandler.handle(retryTestKey, entity);

        Mockito.verify(retryBucketDAO, Mockito.times(0)).update(Mockito.anyString(), Mockito.any(RetryBucketKey.class),
                Mockito.anyString());
        // DMA will not try to attempt retry, event will be send to device just
        // once.
        Assert.assertEquals(1, handler.getEventList().size());

    }

    /**
     * Test retry handle when global topic is provided.
     */
    @Test
    public void testRetryHandleWhenGlobalTopicIsProvided() {
        TestHandler handler = new TestHandler();
        retryHandler.close();
        retryHandler.setMaxRetry(Constants.INT_MINUS_ONE);
        retryHandler.setNextHandler(handler);
        retryHandler.setup(taskId);

        RetryTestEvent event = new RetryTestEvent();
        event.setVehicleId("vehicleId");
        event.setDevMsgGlobalTopic("test");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, 
                entity.getDeviceMessageHeader(), "vehicleId"))
                .thenReturn(Optional.of("vehicleId"));
        retryHandler.handle(retryTestKey, entity);

        Mockito.verify(retryBucketDAO, Mockito.times(0)).update(Mockito.anyString(), Mockito.any(RetryBucketKey.class),
                Mockito.anyString());
        // DMA will not try to attempt retry, event will be send to device just
        // once.
        Assert.assertEquals(1, handler.getEventList().size());

    }

    /**
     * Test retry handle when fallback to TTL on max retry exhausted.
     */
    @Test
    public void testRetryHandleWhenFallbackToTTLOnMaxRetryExhausted() {
        TestHandler handler = new TestHandler();
        retryHandler.close();
        retryHandler.setMaxRetry(Constants.INT_MINUS_ONE);
        retryHandler.setNextHandler(handler);
        retryHandler.setup(taskId);

        RetryTestEvent event = new RetryTestEvent();
        event.setVehicleId("vehicleId");
        event.setEventId("123");
        event.setResponseExpected(true);
        EventConfig config = new EventConfigTestImpl();
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event),
                Version.V1_0, event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        Mockito.when(eventConfigMap.get(Mockito.any())).thenReturn(config);
        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, 
                entity.getDeviceMessageHeader(), "vehicleId"))
                .thenReturn(Optional.of("vehicleId"));
        retryHandler.handle(retryTestKey, entity);

        Assert.assertEquals(Constants.TWO, handler.getEventList().size());

    }

    /**
     * Test retry handle save to offline buffer and delete from cache.
     */
    @Test
    public void testRetryHandleSaveToOfflineBufferAndDeleteFromCache() {
        TestHandler handler = new TestHandler();
        retryHandler.close();
        retryHandler.setMaxRetry(Constants.INT_MINUS_ONE);
        retryHandler.setNextHandler(handler);
        retryHandler.setup(taskId);

        RetryTestEvent event = new RetryTestEvent();
        event.setVehicleId("vehicleId");
        event.setEventId("123");
        event.setResponseExpected(true);
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        long currentTime = System.currentTimeMillis() - TestConstants.THREAD_SLEEP_TIME_10000;
        RetryRecord retryRecord = new RetryRecord(retryTestKey, entity, currentTime);
        retryRecord.addAttempt(currentTime + Constants.TEN);
        DeviceMessage message = retryRecord.getDeviceMessage();
        message.setDeviceMessageHeader(message.getDeviceMessageHeader().withPendingRetries(1));
        retryRecord.setDeviceMessage(message);
        EventConfig config = new EventConfigTestImpl();
        Mockito.when(eventConfigMap.get(Mockito.any())).thenReturn(config);
        Mockito.when(retryEventDAO.get(Mockito.any())).thenReturn(retryRecord);
        Mockito.doNothing().when(offlineBufferDAO).addOfflineBufferEntry(Mockito.anyString(),
                Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, entity.getDeviceMessageHeader(), 
                "vehicleId")).thenReturn(Optional.of("vehicleId"));
        retryHandler.handle(retryTestKey, entity);

        Assert.assertEquals(1, handler.getEventList().size());

    }

    /**
     * Test retry handle when attempts are less than max retry.
     */
    @Test
    public void testRetryHandleWhenAttemptsAreLessThanMaxRetry() {
        TestHandler handler = new TestHandler();
        retryHandler.close();
        retryHandler.setMaxRetry(Constants.FIVE);
        retryHandler.setNextHandler(handler);
        retryHandler.setup(taskId);

        RetryTestEvent event = new RetryTestEvent();
        event.setVehicleId("vehicleId");
        event.setEventId("123");
        event.setResponseExpected(true);
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        long currentTime = System.currentTimeMillis() - TestConstants.THREAD_SLEEP_TIME_10000;
        RetryRecord retryRecord = new RetryRecord(retryTestKey, entity, currentTime);
        retryRecord.addAttempt(currentTime + Constants.TEN);
        DeviceMessage message = retryRecord.getDeviceMessage();
        message.setDeviceMessageHeader(message.getDeviceMessageHeader().withPendingRetries(1));
        retryRecord.setDeviceMessage(message);
        EventConfig config = new EventConfigTestImpl();
        Mockito.when(eventConfigMap.get(Mockito.any())).thenReturn(config);
        Mockito.when(retryEventDAO.get(Mockito.any())).thenReturn(retryRecord);
        Mockito.doNothing().when(offlineBufferDAO).addOfflineBufferEntry(Mockito.anyString(),
                Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, entity.getDeviceMessageHeader(), 
                "vehicleId")).thenReturn(Optional.of("vehicleId"));
        retryHandler.handle(retryTestKey, entity);
        Assert.assertEquals(1, handler.getEventList().size());

    }

    /**
     * Test retry handle when response expected is not set.
     */
    @Test
    public void testRetryHandleWhenResponseExpectedIsNotSet() {
        TestHandler handler = new TestHandler();
        retryHandler.close();
        retryHandler.setNextHandler(handler);
        retryHandler.setup(taskId);

        RetryTestEvent event = new RetryTestEvent();
        event.setVehicleId("vehicleId");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, 
                entity.getDeviceMessageHeader(), "vehicleId"))
                .thenReturn(Optional.of("vehicleId"));
        retryHandler.handle(retryTestKey, entity);

        Mockito.verify(retryBucketDAO, Mockito.times(0)).update(Mockito.anyString(), Mockito.any(RetryBucketKey.class),
                Mockito.anyString());
        // DMA will not try to attempt retry, event will be send to device just
        // once.
        Assert.assertEquals(1, handler.getEventList().size());

    }

    /**
     * Test retry handle when max retry threshold has not been reached.
     */
    @Test
    public void testRetryHandleWhenMaxRetryThresholdHasNotBeenReached() {
        retryHandler.close();
        String retryRecordKey = "Vehicle12345;msg123";
        ConcurrentHashSet<String> retryRecordKeys = new ConcurrentHashSet<String>();
        retryRecordKeys.add(retryRecordKey);
        // It should be able to retry past keys, hence we are subtracting 10
        // seconds
        long currentTime = System.currentTimeMillis() - TestConstants.THREAD_SLEEP_TIME_10000;
        ConcurrentSkipListMap<RetryBucketKey, RetryRecordIds> map =
                new ConcurrentSkipListMap<RetryBucketKey, RetryRecordIds>();
        map.put((new RetryBucketKey(currentTime)), new RetryRecordIds(Version.V1_0, retryRecordKeys));
        TestKVIterator<RetryBucketKey, RetryRecordIds> itr = new TestKVIterator<RetryBucketKey, RetryRecordIds>(map);
        Mockito.when(retryBucketDAO.getHead(Mockito.any(RetryBucketKey.class))).thenReturn(itr);
        RetryTestEvent event = getRetryTestEvent();
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0, event, 
                sourceTopic, TestConstants.THREAD_SLEEP_TIME_60000);
        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, 
                entity.getDeviceMessageHeader(), "vehicleId"))
                .thenReturn(Optional.of("vehicleId"));
        // We are creating a RetryRecord in which attempts is 1 less than
        // maxretry.
        RetryRecord retryRecord = getRetryRecord(currentTime, entity);
        Mockito.when(retryEventDAO.get(new RetryRecordKey(retryRecordKey, taskId))).thenReturn(retryRecord);
        // Max Retry is set to 2 here.
        getRetryHandler();
        TestHandler handler = new TestHandler();
        retryHandler.setNextHandler(handler);

        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_200, MILLISECONDS)).join();
        retryHandler.close();
        Mockito.verify(retryEventDAO, Mockito.times(1)).putToMap(Mockito.anyString(),
                Mockito.any(RetryRecordKey.class), Mockito.any(RetryRecord.class), Mockito.any(Optional.class),
                Mockito.anyString());
        ArgumentCaptor<RetryRecordKey> recordKeyArgument = ArgumentCaptor.forClass(RetryRecordKey.class);
        ArgumentCaptor<RetryRecord> recordArgument = ArgumentCaptor.forClass(RetryRecord.class);
        ArgumentCaptor<String> parentKeyArgument = ArgumentCaptor.forClass(String.class);
        Mockito.verify(retryEventDAO).putToMap(parentKeyArgument.capture(), recordKeyArgument.capture(),
                recordArgument.capture(), Mockito.any(Optional.class), Mockito.anyString());
        RetryRecordKey actualKey = recordKeyArgument.getValue();
        RetryRecord actualValue = recordArgument.getValue();
        Assert.assertEquals(RetryRecordKey.getMapKey(serviceName, taskId), parentKeyArgument.getValue());
        Assert.assertEquals(retryRecordKey, actualKey.getKey());
        Assert.assertEquals(event, actualValue.getDeviceMessage().getEvent());
        Assert.assertEquals(Constants.TWO, actualValue.getAttempts());

        ArgumentCaptor<String> retryBucketMapKeyArgument = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> retryRecordKeyArgument = ArgumentCaptor.forClass(String.class);
        Mockito.verify(retryBucketDAO, Mockito.times(1))
                .update(retryBucketMapKeyArgument.capture(), Mockito.any(RetryBucketKey.class),
                retryRecordKeyArgument.capture());
        String actualRetryBucketMapKey = retryBucketMapKeyArgument.getValue();
        String actualRetryRecordKey = retryRecordKeyArgument.getValue();
        Assert.assertEquals(RetryBucketKey.getMapKey(serviceName, taskId), actualRetryBucketMapKey);
        Assert.assertEquals(retryRecordKey, actualRetryRecordKey);

        // Event will be retried only once
        Assert.assertEquals(1, handler.getEventList().size());

        ArgumentCaptor<DeviceMessageFailureEventDataV1_0> failDataArg = ArgumentCaptor
                .forClass(DeviceMessageFailureEventDataV1_0.class);
        Mockito.verify(deviceMessageUtils).postFailureEvent(failDataArg.capture(), Mockito.any(IgniteKey.class),
                Mockito.any(StreamProcessingContext.class), Mockito.anyString());

        DeviceMessageFailureEventDataV1_0 actual = failDataArg.getValue();
        Assert.assertEquals(DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE, actual.getErrorCode());
        Assert.assertEquals(0, actual.getShoudlerTapRetryAttempts());
        Assert.assertEquals(Constants.TWO, actual.getRetryAttempts());
        Assert.assertEquals(false, actual.isDeviceStatusInactive());
        Assert.assertEquals(false, actual.isDeviceDeliveryCutoffExceeded());
    }

    /**
     * Gets the retry record.
     *
     * @param currentTime the current time
     * @param entity the entity
     * @return the retry record
     */
    @NotNull
    private RetryRecord getRetryRecord(long currentTime, DeviceMessage entity) {
        RetryRecord retryRecord = new RetryRecord(retryTestKey, entity, currentTime);
        retryRecord.addAttempt(currentTime + Constants.TEN);
        DeviceMessage message = retryRecord.getDeviceMessage();
        message.setDeviceMessageHeader(message.getDeviceMessageHeader().withPendingRetries(1));
        retryRecord.setDeviceMessage(message);
        return retryRecord;
    }

    /**
     * Gets the retry handler.
     *
     * @return the retry handler
     */
    private void getRetryHandler() {
        retryHandler.setMaxRetry(Constants.TWO);
        retryHandler.setServiceName(serviceName);
        retryHandler.setup(taskId);
    }

    /**
     * Gets the retry test event.
     *
     * @return the retry test event
     */
    @NotNull
    private static RetryTestEvent getRetryTestEvent() {
        RetryTestEvent event = new RetryTestEvent();
        event.setResponseExpected(true);
        String msgId = "msg123";
        event.setMessageId(msgId);
        event.setVehicleId("vehicleId");
        return event;
    }

    /**
     * Test retry handle when max retry threshold has reached.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testRetryHandleWhenMaxRetryThresholdHasReached() throws InterruptedException {
        retryHandler.close();
        String retryRecordKey = "Vehicle12345;msg123";
        ConcurrentHashSet<String> retryRecordKeys = new ConcurrentHashSet<String>();
        retryRecordKeys.add(retryRecordKey);

        // It should be able to retry past keys, hence we are subtracting 10
        // seconds
        long currentTime = System.currentTimeMillis() - TestConstants.THREAD_SLEEP_TIME_10000;
        ConcurrentSkipListMap<RetryBucketKey, RetryRecordIds> map =
                new ConcurrentSkipListMap<RetryBucketKey, RetryRecordIds>();
        map.put((new RetryBucketKey(currentTime)), new RetryRecordIds(Version.V1_0, retryRecordKeys));
        TestKVIterator<RetryBucketKey, RetryRecordIds> itr = new TestKVIterator<RetryBucketKey, RetryRecordIds>(map);
        Mockito.when(retryBucketDAO.getHead(Mockito.any(RetryBucketKey.class))).thenReturn(itr);

        RetryTestEvent event = new RetryTestEvent();
        event.setResponseExpected(true);
        String msgId = "msg123";
        event.setMessageId(msgId);
        event.setVehicleId("vehicleId");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event),
                Version.V1_0, event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        Mockito.when(spc.streamName()).thenReturn("topic");
        String failMsgId = "fail123";
        Mockito.when(msgIdGenerator.generateUniqueMsgId(Mockito.anyString())).thenReturn(failMsgId);
        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, 
                entity.getDeviceMessageHeader(), "vehicleId")).thenReturn(Optional.of("vehicleId"));
        // We are creating a RetryRecord in which attempts is 2 to match the
        // maxretry.
        RetryRecord retryRecord = new RetryRecord(retryTestKey, entity, currentTime);
        retryRecord.addAttempt(currentTime + Constants.TEN);
        retryRecord.addAttempt(currentTime + Constants.TWENTY);

        Mockito.when(retryEventDAO.get(new RetryRecordKey(retryRecordKey, taskId))).thenReturn(retryRecord);

        // Max Retry is set to 2 here.
        retryHandler.setMaxRetry(Constants.TWO);
        TestHandler handler = new TestHandler();
        retryHandler.setNextHandler(handler);
        retryHandler.setServiceName(serviceName);
        retryHandler.setup(taskId);
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_200, MILLISECONDS)).join();
        retryHandler.close();

        ArgumentCaptor<String> retryEventMapKeyArgument = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RetryRecordKey> retryRecordKeyArgument = ArgumentCaptor.forClass(RetryRecordKey.class);
        Mockito.verify(retryEventDAO, Mockito.times(1))
                .deleteFromMap(retryEventMapKeyArgument.capture(), retryRecordKeyArgument.capture(),
                Mockito.any(Optional.class), Mockito.anyString());
        String actualRetryEventMapKey = retryEventMapKeyArgument.getValue();
        RetryRecordKey actualRetryRecordKey = retryRecordKeyArgument.getValue();
        Assert.assertEquals(RetryRecordKey.getMapKey(serviceName, taskId), actualRetryEventMapKey);
        Assert.assertEquals(retryRecordKey, actualRetryRecordKey.getKey());

        ArgumentCaptor<DeviceMessageFailureEventDataV1_0> failDataArg = ArgumentCaptor
                .forClass(DeviceMessageFailureEventDataV1_0.class);
        Mockito.verify(deviceMessageUtils).postFailureEvent(failDataArg.capture(), Mockito.any(IgniteKey.class),
                Mockito.any(StreamProcessingContext.class), Mockito.anyString());

        DeviceMessageFailureEventDataV1_0 actual = failDataArg.getValue();
        Assert.assertEquals(DeviceMessageErrorCode.RETRY_ATTEMPTS_EXCEEDED, actual.getErrorCode());
        Assert.assertEquals(0, actual.getShoudlerTapRetryAttempts());
        Assert.assertEquals(Constants.TWO, actual.getRetryAttempts());
        Assert.assertEquals(false, actual.isDeviceStatusInactive());
        Assert.assertEquals(false, actual.isDeviceDeliveryCutoffExceeded());

        // Here in eventDao we have set that the event has already been retried
        // twice and max retry is also set 2 hence no more retrires will be
        // attempted.
        Assert.assertEquals(0, handler.getEventList().size());

    }

    /**
     * Test retry handle when no data present in retry event DAO.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testRetryHandleWhenNoDataPresentInRetryEventDAO() throws InterruptedException {
        retryHandler.close();
        String retryRecordKey = "Vehicle12345;msg123";
        ConcurrentHashSet<String> retryRecordKeys = new ConcurrentHashSet<String>();
        retryRecordKeys.add(retryRecordKey);

        long currentTime = System.currentTimeMillis();
        ConcurrentSkipListMap<RetryBucketKey, RetryRecordIds> map =
                new ConcurrentSkipListMap<RetryBucketKey, RetryRecordIds>();
        map.put((new RetryBucketKey(currentTime)), new RetryRecordIds(Version.V1_0, retryRecordKeys));
        TestKVIterator<RetryBucketKey, RetryRecordIds> itr = new TestKVIterator<RetryBucketKey, RetryRecordIds>(map);
        Mockito.when(retryBucketDAO.getHead(Mockito.any(RetryBucketKey.class))).thenReturn(itr);

        retryHandler.setMaxRetry(Constants.THREE);
        retryHandler.setMaxPollingInterval(TestConstants.TWENTY_FIVE);

        TestHandler handler = new TestHandler();
        retryHandler.setNextHandler(handler);
        retryHandler.setup(taskId);
        runAsync(() -> {}, delayedExecutor(TestConstants.THREAD_SLEEP_TIME_200, MILLISECONDS)).join();
        Mockito.verify(retryBucketDAO, Mockito.atLeast(TestConstants.THREE)).getHead(Mockito.any(RetryBucketKey.class));
        Mockito.verify(retryEventDAO, Mockito.times(1)).get(Mockito.any(RetryRecordKey.class));
        Assert.assertEquals(0, handler.getEventList().size());
    }

    /**
     * Test delivery cut off exceeded.
     */
    @Test
    public void testDeliveryCutOffExceeded() {
        String msgId = "msg123";
        RetryTestEvent event = new RetryTestEvent();
        event.setResponseExpected(true);
        event.setMessageId(msgId);
        event.setDeviceDeliveryCutoff(System.currentTimeMillis() - TestConstants.THREAD_SLEEP_TIME_10000);

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event),
                Version.V1_0, event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        RetryRecord rr = new RetryRecord();
        rr.setAttempts(Constants.TWO);
        Mockito.when(retryEventDAO.get(Mockito.any(RetryRecordKey.class))).thenReturn(rr);
        retryHandler.handle(retryTestKey, entity);
        Mockito.verify(retryEventDAO, Mockito.times(1))
                .deleteFromMap(Mockito.anyString(), Mockito.any(RetryRecordKey.class),
                Mockito.any(Optional.class), Mockito.anyString());
    }

    /**
     * Test device inactive.
     */
    @Test
    public void testDeviceInactive() {
        String msgId = "msg123";
        RetryTestEvent event = new RetryTestEvent();
        event.setResponseExpected(true);
        event.setMessageId(msgId);

        String vehicleId = event.getVehicleId();

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event),
                Version.V1_0, event, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(retryTestKey, entity.getDeviceMessageHeader(), 
                vehicleId)).thenReturn(Optional.empty());
        retryHandler.handle(retryTestKey, entity);
        Mockito.verify(connectionStatusHandler, Mockito.times(1)).handleDeviceInactiveState(retryTestKey, entity);
        Mockito.verify(retryEventDAO, Mockito.times(1))
                .deleteFromMap(Mockito.anyString(), Mockito.any(RetryRecordKey.class),
                Mockito.any(Optional.class), Mockito.anyString());
    }

    /**
     * Test dynamic retry scheduler rescheduling when earlier retry event is received.
     * EventA is received with 200ms retry interval, then EventB with 100ms retry interval.
     * The scheduler should reschedule to process EventB earlier.
     */
    @Test
    public void testDynamicRetrySchedulerReschedulingWithEarlierEvent() throws InterruptedException {
        TestHandler handler = new TestHandler();
        retryHandler.close();
        retryHandler.setMaxRetry(Constants.THREE);
        retryHandler.setNextHandler(handler);
        retryHandler.setup(taskId);

        // Setup test events
        DeviceMessage[] entities = setupTestEvents();
        final DeviceMessage entityA = entities[0];

        // Mock connection status as active for both events
        Mockito.when(connectionStatusHandler.getDeviceIdIfActive(Mockito.any(),
                Mockito.any(), Mockito.eq("vehicleId"))).thenReturn(Optional.of("vehicleId"));

        // Record the current time for timing calculations
        final long startTime = System.currentTimeMillis();

        // Handle EventA - should schedule retry with 200ms delay
        retryHandler.handle(retryTestKey, entityA);

        // Verify EventA was added to retry buckets with ~200ms delay
        ArgumentCaptor<RetryBucketKey> bucketKeyCaptor = ArgumentCaptor.forClass(RetryBucketKey.class);
        ArgumentCaptor<String> recordKeyCaptor = ArgumentCaptor.forClass(String.class);

        // Wait a brief moment to ensure first event is processed
        Thread.sleep(TestConstants.LONG_50);

        // Handle EventB - should reschedule with 100ms delay (earlier than EventA)
        final DeviceMessage entityB = entities[1];
        retryHandler.handle(retryTestKey, entityB);

        // Verify timing and processing
        verifyRetryBucketUpdates(bucketKeyCaptor, recordKeyCaptor, startTime);
        verifyEventProcessing(handler);
    }

    /**
     * Setup test events for dynamic retry scheduler test.
     *
     * @return array containing entityA and entityB
     */
    private DeviceMessage[] setupTestEvents() {
        // Create EventA with 200ms retry interval
        RetryTestEvent eventA = new RetryTestEvent();
        eventA.setVehicleId("vehicleId");
        eventA.setEventId("eventA");
        eventA.setMessageId("msgA123");
        eventA.setResponseExpected(true);

        // Create EventB with 100ms retry interval
        RetryTestEvent eventB = new RetryTestEvent();
        eventB.setVehicleId("vehicleId");
        eventB.setEventId("eventB");
        eventB.setMessageId("msgB123");
        eventB.setResponseExpected(true);

        DeviceMessage entityA = new DeviceMessage(transformer.toBlob(eventA), Version.V1_0,
                eventA, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        DeviceMessage entityB = new DeviceMessage(transformer.toBlob(eventB), Version.V1_0,
                eventB, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        final DeviceMessage spyEntityA = Mockito.spy(entityA);
        final DeviceMessage spyEntityB = Mockito.spy(entityB);

        Mockito.doReturn(TestConstants.THREAD_SLEEP_TIME_200).when(spyEntityA).getEventLevelRetryInterval();
        Mockito.doReturn(TestConstants.THREAD_SLEEP_TIME_100).when(spyEntityB).getEventLevelRetryInterval();

        return new DeviceMessage[]{spyEntityA, spyEntityB};
    }

    /**
     * Verify retry bucket updates and timing.
     *
     * @param bucketKeyCaptor the bucket key captor
     * @param recordKeyCaptor the record key captor
     * @param startTime the start time
     */
    private void verifyRetryBucketUpdates(ArgumentCaptor<RetryBucketKey> bucketKeyCaptor,
            ArgumentCaptor<String> recordKeyCaptor, long startTime) {
        // Verify that retry buckets were updated for both events
        Mockito.verify(retryBucketDAO, Mockito.atLeast(TestConstants.TWO)).update(
                Mockito.anyString(), bucketKeyCaptor.capture(), recordKeyCaptor.capture());

        // Get the captured bucket keys to verify timing
        List<RetryBucketKey> capturedKeys = bucketKeyCaptor.getAllValues();
        List<String> capturedRecords = recordKeyCaptor.getAllValues();

        // Verify we have entries for both events
        Assert.assertTrue("Should have at least 2 retry bucket entries",
                capturedKeys.size() >= TestConstants.TWO);

        // Find the bucket keys for our events
        RetryBucketKey eventABucketKey = null;
        RetryBucketKey eventBBucketKey = null;

        for (int i = 0; i < capturedKeys.size(); i++) {
            String recordKey = capturedRecords.get(i);
            if (recordKey.contains("msgA123")) {
                eventABucketKey = capturedKeys.get(i);
            } else if (recordKey.contains("msgB123")) {
                eventBBucketKey = capturedKeys.get(i);
            }
        }
        Assert.assertNotNull("EventA should have a retry bucket entry", eventABucketKey);
        Assert.assertNotNull("EventB should have a retry bucket entry", eventBBucketKey);

        // Verify timing: Events should be scheduled based on their retry intervals
        long eventAScheduledTime = eventABucketKey.getTimestamp();
        long eventBScheduledTime = eventBBucketKey.getTimestamp();

        // Verify that both events have valid scheduled times (in the future)
        Assert.assertTrue("EventA should be scheduled in the future",
                eventAScheduledTime > startTime);
        Assert.assertTrue("EventB should be scheduled in the future",
                eventBScheduledTime > startTime);

        // Core assertion: EventB (100ms interval) should be scheduled before EventA (200ms interval)
        Assert.assertTrue("EventB should be scheduled before EventA due to shorter retry interval",
                eventBScheduledTime < eventAScheduledTime);

        // Verify that both events are scheduled with reasonable delays (not too far in the future)
        long maxReasonableDelay = TestConstants.THREAD_SLEEP_TIME_500; // 500ms should be more than enough
        Assert.assertTrue("EventA should be scheduled within reasonable time",
                eventAScheduledTime - startTime <= maxReasonableDelay);
        Assert.assertTrue("EventB should be scheduled within reasonable time",
                eventBScheduledTime - startTime <= maxReasonableDelay);

        // Core verification: Test the retry interval behavior
        // EventA has 200ms retry interval, EventB has 100ms retry interval
        // Since both events need to be retried, verify they are handled appropriately
        // The exact scheduling may depend on implementation details, so we focus on
        // verifying that the retry mechanism is working rather than exact timing

        // Verify that the retry intervals are being respected by checking
        // that the events are scheduled with appropriate intervals
        long eventADelay = eventAScheduledTime - startTime;
        long eventBBaseTime = startTime + TestConstants.LONG_50; // EventB processed 50ms later
        long eventBDelay = eventBScheduledTime - eventBBaseTime;

        // Instead of exact timing, verify that the delays are positive and reasonable
        Assert.assertTrue("EventA should have positive delay", eventADelay > 0);
        Assert.assertTrue("EventB should have positive delay", eventBDelay > 0);

        // Most importantly: verify that both events were scheduled for retry
        // This confirms the dynamic retry scheduler is working
        Assert.assertTrue("Both events should be scheduled for retry",
                eventAScheduledTime > startTime && eventBScheduledTime > startTime);
    }

    /**
     * Verify event processing.
     *
     * @param handler the test handler
     */
    private void verifyEventProcessing(TestHandler handler) {
        // Both events should be forwarded to next handler immediately
        Assert.assertEquals("Both events should be forwarded to next handler",
                TestConstants.TWO, handler.getEventList().size());

        // Verify that both events were processed
        boolean foundEventA = false;
        boolean foundEventB = false;
        for (DeviceMessage processedEvent : handler.getEventList()) {
            if ("msgA123".equals(processedEvent.getDeviceMessageHeader().getMessageId())) {
                foundEventA = true;
            } else if ("msgB123".equals(processedEvent.getDeviceMessageHeader().getMessageId())) {
                foundEventB = true;
            }
        }

        Assert.assertTrue("EventA should be processed", foundEventA);
        Assert.assertTrue("EventB should be processed", foundEventB);
    }

    /**
     * inner class TestHandler implements DeviceMessageHandler.
     */
    public static final class TestHandler implements DeviceMessageHandler {

        /** The event list. */
        List<DeviceMessage> eventList;

        /**
         * Instantiates a new test handler.
         */
        public TestHandler() {
            eventList = new ArrayList<DeviceMessage>();
        }

        /**
         * Handle.
         *
         * @param key the key
         * @param value the value
         */
        @Override
        public void handle(IgniteKey<?> key, DeviceMessage value) {
            eventList.add(value);

        }

        /**
         * Sets the next handler.
         *
         * @param handler the new next handler
         */
        @Override
        public void setNextHandler(DeviceMessageHandler handler) {
            // Nothing to do.
        }

        /**
         * Gets the event list.
         *
         * @return the event list
         */
        public List<DeviceMessage> getEventList() {
            return eventList;
        }

        /**
         * Close.
         */
        @Override
        public void close() {
            // Nothing to do.
        }

    }

    /**
     * The Class TestKVIterator.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    private class TestKVIterator<K, V> implements KeyValueIterator<K, V> {

        /** The sorted map. */
        private ConcurrentSkipListMap<K, V> sortedMap;

        /** The key iter. */
        private Iterator<K> keyIter;

        /**
         * Since we have to iterate over the original map, make a deep copy
         * of this map.
         *
         * @param map the map
         */
        public TestKVIterator(Map<K, V> map) {

            this.sortedMap = new ConcurrentSkipListMap<K, V>();
            this.sortedMap.putAll(map);
            keyIter = map.keySet().iterator();

        }

        /**
         * Checks for next.
         *
         * @return true, if successful
         */
        @Override
        public boolean hasNext() {
            return keyIter.hasNext();
        }

        /**
         * Next.
         *
         * @return the key value
         */
        @Override
        public KeyValue<K, V> next() {
            if (hasNext()) {
                K key = this.keyIter.next();
                V val = this.sortedMap.get(key);
                return new KeyValue<K, V>(key, val);
            }
            return null;
        }

        /**
         * Close.
         */
        @Override
        public void close() {
            if (sortedMap != null) {
                sortedMap.clear();
            }

        }

        /**
         * Peek next key.
         *
         * @return the k
         */
        @Override
        public K peekNextKey() {
            throw new UnsupportedOperationException("Method peekNextKey not supported in KeyValueMapIterator");
        }

    }

}
