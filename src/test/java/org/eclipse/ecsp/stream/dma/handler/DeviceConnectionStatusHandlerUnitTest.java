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

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0.ConnectionStatus;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.SpeedV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceMessagingException;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusAPIInMemoryService;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoInMemoryCache;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.stream.dma.shouldertap.DeviceShoulderTapService;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;



/**
 * UT class for {@link DeviceConnectionStatusHandler}.
 */
public class DeviceConnectionStatusHandlerUnitTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    
    /** The test filter DM offline buffer entry impl. */
    @Mock
    TestFilterDMOfflineBufferEntryImpl testFilterDMOfflineBufferEntryImpl;
    
    /** The no filter DM offline buffer entry impl. */
    @Mock
    NoFilterDMOfflineBufferEntryImpl noFilterDMOfflineBufferEntryImpl;
    
    /** The status API in memory service. */
    @Mock
    DeviceStatusAPIInMemoryService statusAPIInMemoryService;
    
    /** The status retriever. */
    @Mock
    DefaultDeviceConnectionStatusRetriever statusRetriever;
    
    /** The in memory dao. */
    @Mock
    DeviceStatusDaoInMemoryCache inMemoryDao;
    
    /** The device status dao. */
    @Mock
    DeviceStatusDaoCacheBackedInMemoryImpl deviceStatusDao;
    
    /** The device connection status handler. */
    @InjectMocks
    private DeviceConnectionStatusHandler deviceConnectionStatusHandler;
    
    /** The next handler. */
    @Mock
    private DeviceMessageHandler nextHandler;
    
    /** The spc. */
    @Mock
    private StreamProcessingContext spc;
    
    /** The device service. */
    @Mock
    private DeviceStatusService deviceService;
    
    /** The device message utils. */
    @Mock
    private DeviceMessageUtils deviceMessageUtils;
    
    /** The offline buffer DAO. */
    @Mock
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;
    
    /** The device shoulder tap service. */
    @Mock
    private DeviceShoulderTapService deviceShoulderTapService;
    
    /** The test key. */
    private RetryTestKey testKey = new RetryTestKey();

    /**
     * setup().
     */

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        testKey.setKey("Vehicle12345");
        deviceConnectionStatusHandler.setNextHandler(nextHandler);
    }

    /**
     * Test get connection status if found active in memory.
     */
    @Test
    public void testGetConnectionStatusIfFoundActiveInMemory() {
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0, event,
                "topic", Constants.THREAD_SLEEP_TIME_60000);
        msg.isOtherBrokerConfigured(true);

        ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
        map.put(deviceId, ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, map);

        Mockito.when(statusAPIInMemoryService.get(vehicleId)).thenReturn(mapping);
        deviceConnectionStatusHandler.handle(testKey, msg);
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);
    }

    /**
     * Test get connection status if found inactive in memory.
     */
    @Test
    public void testGetConnectionStatusIfFoundInactiveInMemory() {
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0, event,
                "topic", Constants.THREAD_SLEEP_TIME_60000);
        msg.isOtherBrokerConfigured(true);

        ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
        map.put(deviceId, ConnectionStatus.INACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, map);

        Mockito.when(statusAPIInMemoryService.get(vehicleId)).thenReturn(mapping);
        deviceConnectionStatusHandler.handle(testKey, msg);

        Mockito.verify(statusAPIInMemoryService, Mockito.times(1)).get(Mockito.anyString());
        Mockito.verify(nextHandler, Mockito.times(0)).handle(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(data, testKey, spc, msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1))
                .addOfflineBufferEntry(vehicleId, testKey, msg, null);
        String service = "ecall";
        Mockito.verify(deviceShoulderTapService, Mockito.times(0))
                .wakeUpDevice("req",
                vehicleId, service, testKey, msg, new HashMap<>());
    }

    /**
     * Test get connection status with one vehicle multiple devices.
     */
    @Test
    public void testGetConnectionStatusWithOneVehicleMultipleDevices() {
        String requestId = "req124";
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setRequestId(requestId);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String vehicleId = "Vehicle12345";
        String deviceId1 = "Device12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId1);

        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        msg.isOtherBrokerConfigured(true);

        Mockito.when(statusAPIInMemoryService.get(vehicleId)).thenReturn(null);
        ConcurrentHashMap<String, ConnectionStatus> deviceStatus = new ConcurrentHashMap<>();
        deviceStatus.put(deviceId1, ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, deviceStatus);
        Mockito.when(statusRetriever.getConnectionStatusData(requestId, vehicleId, deviceId1)).thenReturn(mapping);

        deviceConnectionStatusHandler.handle(testKey, msg);

        Mockito.verify(statusRetriever, Mockito.times(1))
                .getConnectionStatusData(requestId, vehicleId, deviceId1);
        Mockito.verify(statusAPIInMemoryService, Mockito.times(1))
                .update(vehicleId, deviceId1, ConnectionStatus.ACTIVE.toString());
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);

        //prepare one more event with another deviceId
        speed.setValue(Constants.THREAD_SLEEP_TIME_200);
        event.setRequestId("requestId2");
        event.setMessageId("Msg12");
        event.setBizTransactionId("Biz12");
        event.setEventData(speed);
        String deviceId2 = "Device786";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId2);
        msg = new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        msg.isOtherBrokerConfigured(true);

        Mockito.when(statusAPIInMemoryService.get(vehicleId)).thenReturn(mapping);
        // Mock returning of the existing mapping in-memory cache which is:
        // Vehicle12345={Device12345=ACTIVE}
        Mockito.when(inMemoryDao.get(Mockito.any(DeviceStatusKey.class))).thenReturn(mapping);
        // prepare new mapping for deviceId: Device786 and same VIN
        ConcurrentHashMap<String, ConnectionStatus> deviceStatus2 = new ConcurrentHashMap<>();
        deviceStatus2.put(deviceId2, ConnectionStatus.INACTIVE);
        VehicleIdDeviceIdStatus mapping2 = new VehicleIdDeviceIdStatus(Version.V1_0, deviceStatus2);
        Mockito.when(statusRetriever.getConnectionStatusData("requestId2", vehicleId, deviceId2)).thenReturn(mapping2);
        //send new request for Vehicle12345 and Device786
        deviceConnectionStatusHandler.handle(testKey, msg);

        //verify that for Device786 too API got invoked, even though
        // data for VIN already existed in in-memory but it was
        //for the other deviceId. 
        Mockito.verify(statusRetriever, Mockito.times(1))
                .getConnectionStatusData("requestId2", vehicleId, deviceId2);
        //verify for Device786 existing mapping got updated.
        Mockito.verify(statusAPIInMemoryService, Mockito.times(1))
                .update(vehicleId, deviceId2, ConnectionStatus.INACTIVE.toString());
    }

    /**
     * Test get connection status redisdevice ids in cache is null.
     */
    @Test
    public void testGetConnectionStatusRedisdeviceIdsInCacheIsNull() {
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        msg.isOtherBrokerConfigured(false);

        ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
        map.put(deviceId, ConnectionStatus.INACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, map);

        Mockito.when(statusAPIInMemoryService.get(vehicleId)).thenReturn(mapping);
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<>();
        Mockito.when(deviceService.get(Mockito.any(), Mockito.any())).thenReturn(deviceIdsInCache);
        deviceConnectionStatusHandler.handle(testKey, msg);
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).addOfflineBufferEntry(vehicleId, testKey, msg, null);
    }

    /**
     * Test get connection status if not found in memory.
     */
    @Test
    public void testGetConnectionStatusIfNotFoundInMemory() {
        String requestId = "req124";
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setRequestId(requestId);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        msg.isOtherBrokerConfigured(true);

        Mockito.when(statusAPIInMemoryService.get(vehicleId)).thenReturn(null);
        ConcurrentHashMap<String, ConnectionStatus> deviceStatus = new ConcurrentHashMap<>();
        deviceStatus.put(deviceId, ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, deviceStatus);
        Mockito.when(statusRetriever.getConnectionStatusData(requestId, vehicleId, deviceId)).thenReturn(mapping);

        deviceConnectionStatusHandler.handle(testKey, msg);

        Mockito.verify(statusRetriever, Mockito.times(1))
                .getConnectionStatusData(requestId, vehicleId, deviceId);
        Mockito.verify(statusAPIInMemoryService, Mockito.times(1))
                .update(vehicleId, deviceId, ConnectionStatus.ACTIVE.toString());
    }

    /**
     * Test broad cast message.
     */
    @Test
    public void testBroadCastMessage() {
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        String deviceId = "Device12345";
        deviceIdsInCache.add(deviceId);
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String vehicleId = "Vehicle12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setDevMsgGlobalTopic("GlobalMqttTopics");

        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);

        deviceConnectionStatusHandler.handle(testKey, msg);
        // Here we want to verify that this handler acted as a passthrough. One
        // way is to test if deviceService ever gets invoked because if its not
        // a passthrough the deviceService will definitely get invoked.
        Mockito.verify(deviceService, Mockito.times(0)).get(vehicleId, Optional.empty());
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);
    }

    /**
     * Test handle device active state for sub service.
     */
    @Test
    public void testHandleDeviceActiveStateForSubService() {
        String deviceId = "Device12345";
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        deviceIdsInCache.add(deviceId);
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        String vehicleId = "Vehicle12345";
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setDevMsgTopicSuffix("Ecall/test_service/ubi");

        deviceConnectionStatusHandler.setSubServicesList(
                Arrays.asList("ecall/test_service/ubi", "ecall/test_service/ftd"));
        deviceConnectionStatusHandler.setProcessPerSubService(true);

        Mockito.when(deviceService.get(vehicleId, Optional.of("ecall/test_service/ubi")))
                .thenReturn(deviceIdsInCache);
        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        deviceConnectionStatusHandler.handle(testKey, msg);
        Mockito.verify(nextHandler, Mockito.times(1))
                .handle(testKey, msg);
    }

    /**
     * Test handle device active state.
     */
    @Test
    public void testHandleDeviceActiveState() {
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        String deviceId = "Device12345";
        deviceIdsInCache.add(deviceId);
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String vehicleId = "Vehicle12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        Mockito.when(deviceService.get(vehicleId, Optional.empty()))
                .thenReturn(deviceIdsInCache);
        deviceConnectionStatusHandler.handle(testKey, msg);
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);
    }

    /**
     * Test handle device inactive state with sub service.
     */
    @Test
    public void testHandleDeviceInactiveStateWithSubService() {

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        String subService = "ecall/test_service/ubi";
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setDevMsgTopicSuffix(subService);

        String payload = "payload";
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
             "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        String service = "ecall";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setProcessPerSubService(true);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);
        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(data, testKey, spc, msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1))
                .addOfflineBufferEntry(vehicleId, testKey, msg, subService);
        Mockito.verify(deviceShoulderTapService, Mockito.times(0))
                .wakeUpDevice("req", vehicleId, service, testKey, msg, new HashMap<>());
    }

    /**
     * Test handle device inactive state.
     */
    @Test
    public void testHandleDeviceInactiveState() {

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
             "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");

        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        deviceConnectionStatusHandler.setup("0", null);
        String service = "ecall";
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(data, testKey, spc, msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1))
                .addOfflineBufferEntry(vehicleId, testKey, msg, null);
        Mockito.verify(deviceShoulderTapService, Mockito.times(0))
                .wakeUpDevice("req", vehicleId, service, testKey, msg, new HashMap<>());

    }

    /**
     * Test handle device inactive state with shoulder tap.
     */
    @Test
    public void testHandleDeviceInactiveStateWithShoulderTap() {

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        String reqId = "Req123";
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setShoulderTapEnabled(true);
        event.setRequestId(reqId);

        String payload = "payload";
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
            "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        String service = "ecall";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Map<String, Object> extraParameters = new HashMap<>();
        String bizTransactionId = msg.getEvent().getBizTransactionId();
        extraParameters.put(DMAConstants.BIZ_TRANSACTION_ID, bizTransactionId);

        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(data, testKey, spc, msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1))
                .addOfflineBufferEntry(vehicleId, testKey, msg, null);
        Mockito.verify(deviceShoulderTapService, Mockito.times(1))
                .wakeUpDevice(reqId, vehicleId, service, testKey, msg, extraParameters);

    }

    /**
     * Test perform action when status active for sub service.
     */
    @Test
    public void testPerformActionWhenStatusActiveForSubService() {
        String deviceId = "Device12345";
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        deviceIdsInCache.add(deviceId);
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
            "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        String service = "ecall";
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setOfflineBufferPerDevice(false);
        deviceConnectionStatusHandler.setFilteredBufferEntry(noFilterDMOfflineBufferEntryImpl);
        deviceConnectionStatusHandler.setSubServicesList(Arrays
                .asList("ecall/test_service/ubi", "ecall/test_service/ftd"));
        String vehicleId = "Vehicle12345";
        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        IgniteEventImpl connStatusEvent = new IgniteEventImpl();
        connStatusEvent.setVehicleId(vehicleId);
        connStatusEvent.setSourceDeviceId(deviceId);
        String subService = "ecall/test_service/ubi";
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        Mockito.when(deviceService.get(vehicleId, Optional.of(subService))).thenReturn(deviceIdsInCache);
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.of(subService))).thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null, subService, true, false);

        Mockito.verify(deviceService, Mockito.times(1))
                .put(vehicleId, deviceIdsInCache, Optional.empty(), Optional.of(subService));
        Mockito.verify(offlineBufferDAO, Mockito.times(1))
                .getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.of(subService));
        Mockito.verify(offlineBufferDAO, Mockito.times(0))
                .getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.ofNullable(deviceId), Optional.of(subService));
    }

    /**
     * Test perform action when status active.
     */
    @Test
    public void testPerformActionWhenStatusActive() {

        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        String deviceId = "Device12345";
        deviceIdsInCache.add(deviceId);
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
             "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        String service = "ecall";
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setOfflineBufferPerDevice(false);
        deviceConnectionStatusHandler.setFilteredBufferEntry(noFilterDMOfflineBufferEntryImpl);

        String vehicleId = "Vehicle12345";
        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        IgniteEventImpl connStatusEvent = new IgniteEventImpl();
        connStatusEvent.setVehicleId(vehicleId);
        connStatusEvent.setSourceDeviceId(deviceId);
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.empty())).thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null, null, false, false);

        Mockito.verify(offlineBufferDAO, Mockito.times(1)).getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.empty());
        Mockito.verify(offlineBufferDAO, Mockito.times(0)).getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.ofNullable(deviceId), Optional.empty());
    }

    /**
     * Test perform action when status active for ecu type.
     */
    @Test
    public void testPerformActionWhenStatusActiveForEcuType() {
        String service = "ecall";

        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setOfflineBufferPerDevice(false);
        deviceConnectionStatusHandler.setFilteredBufferEntry(noFilterDMOfflineBufferEntryImpl);

        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        IgniteEventImpl connStatusEvent = new IgniteEventImpl();
        connStatusEvent.setVehicleId(vehicleId);
        connStatusEvent.setSourceDeviceId(deviceId);
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.empty())).thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null, null, false, true);

        Mockito.verify(statusAPIInMemoryService, Mockito.times(1)).update(vehicleId, deviceId, DMAConstants.ACTIVE);
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.empty());
        Mockito.verify(offlineBufferDAO, Mockito.times(0)).getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.ofNullable(deviceId), Optional.empty());
        // verify that the other flow is not getting executed.
        Mockito.verify(deviceService, Mockito.times(0)).get(vehicleId, null);
        Mockito.verify(deviceService, Mockito.times(0)).put(vehicleId, new ConcurrentHashSet<String>(), null, null);
    }

    /**
     * Test perform action when status inactive for ecu type.
     */
    @Test
    public void testPerformActionWhenStatusInactiveForEcuType() {
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        String service = "ecall";

        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.performActionWhenStatusInactive(vehicleId, deviceId, null, null, false, true);

        Mockito.verify(statusAPIInMemoryService, Mockito.times(1)).update(vehicleId, deviceId, DMAConstants.INACTIVE);
        Mockito.verify(deviceService, Mockito.times(0)).get(vehicleId, null);
        Mockito.verify(deviceService, Mockito.times(0)).delete(vehicleId, deviceId, null, null);
    }

    /**
     * Test perform action when status active one vehicle to many device.
     */
    @Test
    public void testPerformActionWhenStatusActiveOneVehicleToManyDevice() {
        String deviceId = "Device12345";
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        deviceIdsInCache.add(deviceId);
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
            "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        String service = "ecall";
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setOfflineBufferPerDevice(true);
        deviceConnectionStatusHandler.setFilteredBufferEntry(noFilterDMOfflineBufferEntryImpl);
        String vehicleId = "Vehicle12345";
        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        IgniteEventImpl connStatusEvent = new IgniteEventImpl();
        connStatusEvent.setVehicleId(vehicleId);
        connStatusEvent.setSourceDeviceId(deviceId);
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.ofNullable(deviceId), Optional.empty())).thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null, null, false, false);

        Mockito.verify(offlineBufferDAO, Mockito.times(0)).getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.empty());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.ofNullable(deviceId), Optional.empty());
    }

    /**
     * Test filter DM off line entry.
     */
    @Test
    public void testFilterDMOffLineEntry() {
        String deviceId = "Device12345";
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        deviceIdsInCache.add(deviceId);
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
             "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        String service = "ecall";
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);

        DMOfflineBufferEntry bufferEntry = new DMOfflineBufferEntry();
        bufferEntry.setDeviceId("vehicle1");
        DeviceMessage event = new DeviceMessage();
        IgniteEventImpl eventImpl = new IgniteEventImpl();
        String eventId = "eventId1";
        eventImpl.setEventId(eventId);
        event.setEvent(eventImpl);
        String vehicleId = "Vehicle12345";
        DeviceMessageHeader deviceMessageHeader = new DeviceMessageHeader();
        deviceMessageHeader.withRequestId("reqId1").withVehicleId(vehicleId);
        event.setDeviceMessageHeader(deviceMessageHeader);
        bufferEntry.setEvent(event);
        LocalDateTime eventTs = LocalDateTime.now();
        bufferEntry.setEventTs(eventTs);
        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey("Vehicle12345");
        bufferEntry.setIgniteKey(igniteKey);
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        bufferEntry.setVehicleId(vehicleId);
        bufferedEntries.add(bufferEntry);

        DMOfflineBufferEntry bufferEntry2 = new DMOfflineBufferEntry();
        bufferEntry2.setDeviceId("vehicle2");
        DeviceMessage event2 = new DeviceMessage();
        IgniteEventImpl eventImpl2 = new IgniteEventImpl();
        String eventId2 = "eventId2";
        eventImpl2.setEventId(eventId2);
        DeviceMessageHeader deviceMessageHeader2 = new DeviceMessageHeader();
        deviceMessageHeader2.withRequestId("reqId2").withVehicleId(vehicleId);
        event2.setDeviceMessageHeader(deviceMessageHeader2);
        event2.setEvent(eventImpl2);
        bufferEntry2.setEvent(event2);
        LocalDateTime eventTs2 = LocalDateTime.now();
        bufferEntry2.setEventTs(eventTs2);
        IgniteStringKey igniteKey2 = new IgniteStringKey();
        igniteKey.setKey("Vehicle12345");
        bufferEntry2.setIgniteKey(igniteKey2);
        bufferEntry2.setVehicleId(vehicleId);
        bufferedEntries.add(bufferEntry2);

        getBufferedEntries("vehicle3", "eventId3", igniteKey, bufferedEntries);

        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.empty())).thenReturn(bufferedEntries);
        Mockito.when(testFilterDMOfflineBufferEntryImpl.filterAndUpdateDmOfflineBufferEntries(bufferedEntries))
             .thenReturn(bufferedEntries);
        
        deviceConnectionStatusHandler.setFilteredBufferEntry(testFilterDMOfflineBufferEntryImpl);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null, null, false, false);

        Mockito.verify(testFilterDMOfflineBufferEntryImpl,
                Mockito.times(1)).filterAndUpdateDmOfflineBufferEntries(bufferedEntries);
        Mockito.verify(offlineBufferDAO, Mockito.times(Constants.THREE))
                .removeOfflineBufferEntry(Mockito.any(String.class));
    }

    /**
     * Gets the buffered entries.
     *
     * @param vehicle3 the vehicle 3
     * @param eventId3 the event id 3
     * @param reqId3 the req id 3
     * @param igniteKey the ignite key
     * @param bufferedEntries the buffered entries
     * @return the buffered entries
     */
    private static void getBufferedEntries(String vehicle3, String eventId3, IgniteStringKey igniteKey,
            List<DMOfflineBufferEntry> bufferedEntries) {
        DMOfflineBufferEntry bufferEntry3 = new DMOfflineBufferEntry();
        bufferEntry3.setDeviceId(vehicle3);
        IgniteEventImpl eventImpl3 = new IgniteEventImpl();
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        eventImpl3.setEventId(eventId3);
        eventImpl3.setVehicleId(vehicleId);
        eventImpl3.setSourceDeviceId(deviceId);
        DeviceMessage event3 = new DeviceMessage();
        DeviceMessageHeader deviceMessageHeader3 = new DeviceMessageHeader();
        deviceMessageHeader3.withRequestId("reqId3").withVehicleId(vehicleId);
        event3.setDeviceMessageHeader(deviceMessageHeader3);
        event3.setEvent(eventImpl3);
        bufferEntry3.setEvent(event3);
        LocalDateTime eventTs3 = LocalDateTime.now();
        bufferEntry3.setEventTs(eventTs3);
        IgniteStringKey igniteKey3 = new IgniteStringKey();
        igniteKey.setKey("Vehicle12345");
        bufferEntry3.setIgniteKey(igniteKey3);
        bufferEntry3.setVehicleId(vehicleId);
        bufferedEntries.add(bufferEntry3);
    }

    /**
     * Test skip offline buffer with skip events.
     */
    @Test
    public void testSkipOfflineBufferWithSkipEvents() {
        deviceConnectionStatusHandler.setSkipOfflineBufferEvents(
                Arrays.asList("Speed", "RPM", "RemoteOperationEngine"));
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setEventId("RPM");

        String payload = "payload";
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
             "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        String service = "ecall";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);
        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(data, testKey, spc, msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(0))
                .addOfflineBufferEntry(vehicleId, testKey, msg, null);
    }

    /**
     * Test skip offline buffer with not to skip events.
     */
    @Test
    public void testSkipOfflineBufferWithNotToSkipEvents() {
        deviceConnectionStatusHandler.setSkipOfflineBufferEvents(
                Arrays.asList("Speed", "RPM", "RemoteOperationEngine"));
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String deviceId = "Device12345";
        String vehicleId = "Vehicle12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setEventId("Collision");

        String payload = "payload";
        String service = "ecall";
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass", 
             "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(data, testKey, spc, msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1))
                .addOfflineBufferEntry(vehicleId, testKey, msg, null);
    }

    /**
     * Test skip offline buffer with empty event list.
     */
    @Test
    public void testSkipOfflineBufferWithEmptyEventList() {
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setEventId("Collision");

        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(), Version.V1_0,
                event, "topic", Constants.THREAD_SLEEP_TIME_60000);

        deviceConnectionStatusHandler.setup("0", null);
        String service = "ecall";
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1))
                .postFailureEvent(data, testKey, spc, msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1))
                .addOfflineBufferEntry(vehicleId, testKey, msg, null);
    }

    /**
     * Test get device id if active if target device id absent.
     */
    @Test(expected = DeviceMessagingException.class)
    public void testGetDeviceIdIfActiveIfTargetDeviceIdAbsent() {
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        String vehicleId = "Vehicle12345";
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setEventId("Collision");

        ConcurrentHashSet<String> deviceIds = new ConcurrentHashSet<>();
        deviceIds.add("device123");
        deviceIds.add("device456");

        Mockito.when(deviceService.get(vehicleId, Optional.empty())).thenReturn(deviceIds);
        String payload = "payload";
        DeviceMessage msg = new DeviceMessage(payload.getBytes(),
                Version.V1_0, event, "topic", Constants.THREAD_SLEEP_TIME_60000);
        deviceConnectionStatusHandler.handle(testKey, msg);
    }
}
