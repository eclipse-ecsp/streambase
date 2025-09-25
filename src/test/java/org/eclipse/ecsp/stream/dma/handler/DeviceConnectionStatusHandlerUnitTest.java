package org.eclipse.ecsp.stream.dma.handler;

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
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
import org.eclipse.ecsp.stream.dma.dao.DeviceConnStatusDao;
import org.eclipse.ecsp.stream.dma.dao.DeviceMessagingException;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusUtil;
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
 * Unit tests for {@link DeviceConnectionStatusHandler}.
 */
public class DeviceConnectionStatusHandlerUnitTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The device connection status handler. */
    @InjectMocks
    private DeviceConnectionStatusHandler deviceConnectionStatusHandler;

    /** The next handler. */
    @Mock
    private DeviceMessageHandler nextHandler;

    /** The stream processing context. */
    @Mock
    private StreamProcessingContext spc;

    /** The device status service implementation. */
    @Mock
    private DeviceStatusService deviceStatusServiceImpl;

    /** The device message utilities. */
    @Mock
    private DeviceMessageUtils deviceMessageUtils;

    /** The offline buffer DAO. */
    @Mock
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;

    /** The device shoulder tap service. */
    @Mock
    private DeviceShoulderTapService deviceShoulderTapService;

    /** The test filter for offline buffer entries. */
    @Mock
    private TestFilterDMOfflineBufferEntryImpl testFilterDMOfflineBufferEntryImpl;

    /** The no filter for offline buffer entries. */
    @Mock
    private NoFilterDMOfflineBufferEntryImpl noFilterDMOfflineBufferEntryImpl;

    /** The device status API service implementation. */
    @Mock
    private DeviceStatusService deviceStatusApiServiceImpl;

    /** The device status DAO implementation. */
    @Mock
    private DeviceStatusDaoImpl deviceStatusDaoImpl;

    /** The default device connection status retriever. */
    @Mock
    private DefaultDeviceConnectionStatusRetriever statusRetriever;

    /** The in-memory DAO. */
    @Mock
    private DeviceConnStatusDao inMemoryDao;

    /** The device status DAO. */
    @Mock
    private DeviceStatusDaoImpl deviceStatusDao;

    /** The device status utility. */
    @Mock
    private DeviceStatusUtil deviceStatusUtil;

    /** The retry test key. */
    private RetryTestKey testKey = new RetryTestKey();

    // Added constants for magic numbers.
    private static final int SPEED_VALUE = 100;
    private static final int TIMEOUT = 60000;
    private static final int ALTERNATE_SPEED_VALUE = 200;
    private static final int DEVICE_COUNT = 3;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        testKey.setKey("Vehicle12345");
        deviceConnectionStatusHandler.setNextHandler(nextHandler);
    }

    /**
     * Tests getting connection status if found active in memory.
     */
    @Test
    public void testGetConnectionStatusIfFoundActiveInMemory() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        msg.isOtherBrokerConfigured(true);

        ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
        map.put(deviceId, ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, map);

        Mockito.when(deviceStatusApiServiceImpl.get(Mockito.anyString(), Mockito.any()))
                .thenReturn(mapping);
        deviceConnectionStatusHandler.handle(testKey, msg);
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);
    }

    /**
     * Tests getting connection status if found inactive in memory.
     */
    @Test
    public void testGetConnectionStatusIfFoundInactiveInMemory() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        msg.isOtherBrokerConfigured(true);

        ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
        map.put(deviceId, ConnectionStatus.INACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, map);

        Mockito.when(deviceStatusApiServiceImpl.get(Mockito.anyString(), Mockito.any()))
                .thenReturn(mapping);
        deviceConnectionStatusHandler.handle(testKey, msg);

        Mockito.verify(deviceStatusApiServiceImpl, Mockito.times(1)).get(Mockito.anyString(),
                Mockito.any());
        Mockito.verify(nextHandler, Mockito.times(0)).handle(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1)).postFailureEvent(data, testKey, spc,
                msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).addOfflineBufferEntry(vehicleId, testKey,
                msg, null);
        Mockito.verify(deviceShoulderTapService, Mockito.times(0)).wakeUpDevice("req", vehicleId,
                service, testKey, msg, new HashMap<>());
    }

    /**
     * Tests getting connection status with one vehicle and multiple devices.
     */
    @Test
    public void testGetConnectionStatusWithOneVehicleMultipleDevices() {
        final String vehicleId = "Vehicle12345";
        final String deviceId1 = "Device12345";
        final String deviceId2 = "Device786";
        final String requestId = "req124";
        final String service = "ecall";
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setRequestId(requestId);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId1);

        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        msg.isOtherBrokerConfigured(true);

        Mockito.when(deviceStatusApiServiceImpl.get(Mockito.anyString(), Mockito.any()))
                .thenReturn(null);
        ConcurrentHashMap<String, ConnectionStatus> deviceStatus = new ConcurrentHashMap<>();
        deviceStatus.put(deviceId1, ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, deviceStatus);
        Mockito.when(statusRetriever.getConnectionStatusData(requestId, vehicleId, deviceId1,
                Optional.empty())).thenReturn(mapping);

        deviceConnectionStatusHandler.handle(testKey, msg);

        Mockito.verify(statusRetriever, Mockito.times(1)).getConnectionStatusData(requestId,
                vehicleId, deviceId1, Optional.empty());
        Mockito.verify(deviceStatusApiServiceImpl, Mockito.times(1)).update(vehicleId, deviceId1,
                ConnectionStatus.ACTIVE.toString());
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);

        // prepare one more event with another deviceId
        speed.setValue(ALTERNATE_SPEED_VALUE);
        event.setRequestId("requestId2");
        event.setMessageId("Msg12");
        event.setBizTransactionId("Biz12");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId2);
        msg = new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        msg.isOtherBrokerConfigured(true);

        Mockito.when(deviceStatusApiServiceImpl.get(Mockito.anyString(), Mockito.any()))
                .thenReturn(mapping);
        // Mock returning of the existing mapping in-memory cache which is:
        // Vehicle12345={Device12345=ACTIVE}
        Mockito.when(inMemoryDao.get(Mockito.any(DeviceStatusKey.class))).thenReturn(mapping);
        // prepare new mapping for deviceId: Device786 and same VIN
        ConcurrentHashMap<String, ConnectionStatus> deviceStatus2 = new ConcurrentHashMap<>();
        deviceStatus2.put(deviceId2, ConnectionStatus.INACTIVE);
        VehicleIdDeviceIdStatus mapping2 = new VehicleIdDeviceIdStatus(Version.V1_0, deviceStatus2);
        Mockito.when(statusRetriever.getConnectionStatusData("requestId2", vehicleId, deviceId2,
                Optional.empty())).thenReturn(mapping2);

        // send new request for Vehicle12345 and Device786
        deviceConnectionStatusHandler.handle(testKey, msg);

        // verify that for Device786 too API got invoked, even though data for
        // VIN already existed in in-memory but it was
        // for the other deviceId.
        Mockito.verify(statusRetriever, Mockito.times(1)).getConnectionStatusData("requestId2",
                vehicleId, deviceId2, Optional.empty());
        // verify for Device786 existing mapping got updated.
        Mockito.verify(deviceStatusApiServiceImpl, Mockito.times(1)).update(vehicleId, deviceId2,
                ConnectionStatus.INACTIVE.toString());
    }

    /**
     * Tests getting connection status from Redis if device IDs in cache is null.
     */
    @Test
    public void testGetConnectionStatusRedisdeviceIdsInCacheIsNull() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        msg.isOtherBrokerConfigured(false);

        ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
        map.put(deviceId, ConnectionStatus.INACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, map);

        Mockito.when(deviceStatusApiServiceImpl.get(vehicleId, Optional.of("vehicleIdSubServive")))
                .thenReturn(mapping);
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<>();
        Mockito.when(deviceStatusServiceImpl.get(Mockito.any(), Mockito.any()))
                .thenReturn(deviceIdsInCache);
        deviceConnectionStatusHandler.handle(testKey, msg);
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).addOfflineBufferEntry(vehicleId, testKey,
                msg, null);
    }

    /**
     * Tests getting connection status if not found in memory.
     */
    @Test
    public void testGetConnectionStatusIfNotFoundInMemory() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String requestId = "req124";
        final String service = "ecall";
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setRequestId(requestId);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        msg.isOtherBrokerConfigured(true);

        Mockito.when(deviceStatusApiServiceImpl.get(Mockito.anyString(), Mockito.any()))
                .thenReturn(null);
        Mockito.when(deviceStatusApiServiceImpl.forceGet(Mockito.any(), Mockito.any(String.class)))
                .thenReturn(null);
        ConcurrentHashMap<String, ConnectionStatus> deviceStatus = new ConcurrentHashMap<>();
        deviceStatus.put(deviceId, ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, deviceStatus);
        Mockito.when(statusRetriever.getConnectionStatusData(requestId, vehicleId, deviceId,
                Optional.empty())).thenReturn(mapping);

        deviceConnectionStatusHandler.handle(testKey, msg);

        Mockito.verify(statusRetriever, Mockito.times(1)).getConnectionStatusData(requestId,
                vehicleId, deviceId, Optional.empty());
        Mockito.verify(deviceStatusApiServiceImpl, Mockito.times(1)).update(vehicleId, deviceId,
                ConnectionStatus.ACTIVE.toString());
    }

    /**
     * Tests broadcasting message.
     */
    @Test
    public void testBroadCastMessage() {
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        deviceIdsInCache.add(deviceId);
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setDevMsgGlobalTopic("GlobalMqttTopics");

        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);

        deviceConnectionStatusHandler.handle(testKey, msg);
        // Here we want to verify that this handler acted as a passthrough. One
        // way is to test if deviceService ever gets invoked because if its not
        // a passthrough the deviceService will definitely get invoked.
        Mockito.verify(deviceStatusServiceImpl, Mockito.times(0)).get(vehicleId, Optional.empty());
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);
    }

    /**
     * Tests handling device active state for sub-service.
     */
    @Test
    public void testHandleDeviceActiveStateForSubService() {
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        deviceIdsInCache.add(deviceId);
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setDevMsgTopicSuffix("Ecall/test_service/ubi");

        deviceConnectionStatusHandler.setSubServicesList(
                Arrays.asList("ecall/test_service/ubi", "ecall/test_service/ftd"));
        deviceConnectionStatusHandler.setProcessPerSubService(true);
        Mockito.when(deviceStatusUtil
                .getSubServiceNameFromHeader(Mockito.any(DeviceMessageHeader.class)))
                .thenReturn("ecall/test_service/ubi");
        Mockito.when(deviceStatusApiServiceImpl.get(Mockito.anyString(), Mockito.any()))
                .thenReturn("null");
        Mockito.when(deviceStatusServiceImpl.get(vehicleId, Optional.of("ecall/test_service/ubi")))
                .thenReturn(deviceIdsInCache);
        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        deviceConnectionStatusHandler.handle(testKey, msg);
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);
    }

    /**
     * Tests handling device active state.
     */
    @Test
    public void testHandleDeviceActiveState() {
        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        deviceIdsInCache.add(deviceId);
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);

        Mockito.when(deviceStatusServiceImpl.get(vehicleId, Optional.empty()))
                .thenReturn(deviceIdsInCache);
        deviceConnectionStatusHandler.handle(testKey, msg);
        Mockito.verify(nextHandler, Mockito.times(1)).handle(testKey, msg);
    }

    /**
     * Tests handling device inactive state with sub-service.
     */
    @Test
    public void testHandleDeviceInactiveStateWithSubService() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";
        final String subService = "ecall/test_service/ubi";

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setDevMsgTopicSuffix(subService);

        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setProcessPerSubService(true);
        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1)).postFailureEvent(data, testKey, spc,
                msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).addOfflineBufferEntry(vehicleId, testKey,
                msg, subService);
        Mockito.verify(deviceShoulderTapService, Mockito.times(0)).wakeUpDevice("req", vehicleId,
                service, testKey, msg, new HashMap<>());
    }

    /**
     * Tests handling device inactive state.
     */
    @Test
    public void testHandleDeviceInactiveState() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1)).postFailureEvent(data, testKey, spc,
                msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).addOfflineBufferEntry(vehicleId, testKey,
                msg, null);
        Mockito.verify(deviceShoulderTapService, Mockito.times(0)).wakeUpDevice("req", vehicleId,
                service, testKey, msg, new HashMap<>());

    }

    /**
     * Tests handling device inactive state with shoulder tap.
     */
    @Test
    public void testHandleDeviceInactiveStateWithShoulderTap() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";
        final String reqId = "Req123";

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setShoulderTapEnabled(true);
        event.setRequestId(reqId);

        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Map<String, Object> extraParameters = new HashMap<>();
        String bizTransactionId = msg.getEvent().getBizTransactionId();
        extraParameters.put(DMAConstants.BIZ_TRANSACTION_ID, bizTransactionId);

        Mockito.verify(deviceMessageUtils, Mockito.times(1)).postFailureEvent(data, testKey, spc,
                msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).addOfflineBufferEntry(vehicleId, testKey,
                msg, null);
        Mockito.verify(deviceShoulderTapService, Mockito.times(1)).wakeUpDevice(reqId, vehicleId,
                service, testKey, msg, extraParameters);

    }

    /**
     * Tests performing action when status is active for sub-service.
     */
    @Test
    public void testPerformActionWhenStatusActiveForSubService() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";
        final String subService = "ecall/test_service/ubi";

        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        deviceIdsInCache.add(deviceId);
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setOfflineBufferPerDevice(false);
        deviceConnectionStatusHandler.setFilteredBufferEntry(noFilterDMOfflineBufferEntryImpl);
        deviceConnectionStatusHandler.setSubServicesList(
                Arrays.asList("ecall/test_service/ubi", "ecall/test_service/ftd"));

        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        IgniteEventImpl connStatusEvent = new IgniteEventImpl();
        connStatusEvent.setVehicleId(vehicleId);
        connStatusEvent.setSourceDeviceId(deviceId);
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        Mockito.when(deviceStatusServiceImpl.get(vehicleId, Optional.of(subService)))
                .thenReturn(deviceIdsInCache);
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.empty(), Optional.of(subService))).thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null,
                subService, true, false);

        Mockito.verify(deviceStatusServiceImpl, Mockito.times(1)).put(vehicleId, deviceIdsInCache,
                Optional.empty(), Optional.of(subService));
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).getOfflineBufferEntriesSortedByPriority(
                vehicleId, true, Optional.empty(), Optional.of(subService));
        Mockito.verify(offlineBufferDAO, Mockito.times(0)).getOfflineBufferEntriesSortedByPriority(
                vehicleId, true, Optional.ofNullable(deviceId), Optional.of(subService));
    }

    /**
     * Tests performing action when status is active.
     */
    @Test
    public void testPerformActionWhenStatusActive() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";

        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        deviceIdsInCache.add(deviceId);
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setOfflineBufferPerDevice(false);
        deviceConnectionStatusHandler.setFilteredBufferEntry(noFilterDMOfflineBufferEntryImpl);

        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        IgniteEventImpl connStatusEvent = new IgniteEventImpl();
        connStatusEvent.setVehicleId(vehicleId);
        connStatusEvent.setSourceDeviceId(deviceId);
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.empty(), Optional.empty())).thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null, null,
                false, false);

        Mockito.verify(offlineBufferDAO, Mockito.times(1)).getOfflineBufferEntriesSortedByPriority(
                vehicleId, true, Optional.empty(), Optional.empty());
        Mockito.verify(offlineBufferDAO, Mockito.times(0)).getOfflineBufferEntriesSortedByPriority(
                vehicleId, true, Optional.ofNullable(deviceId), Optional.empty());
    }

    /**
     * Tests performing action when status is active for ECU type.
     */
    @Test
    public void testPerformActionWhenStatusActiveForEcuType() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";

        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setOfflineBufferPerDevice(false);
        deviceConnectionStatusHandler.setFilteredBufferEntry(noFilterDMOfflineBufferEntryImpl);

        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        IgniteEventImpl connStatusEvent = new IgniteEventImpl();
        connStatusEvent.setVehicleId(vehicleId);
        connStatusEvent.setSourceDeviceId(deviceId);
        Map<String, ConnectionStatus> mapping = new ConcurrentHashMap<>();
        mapping.put("Device12345", ConnectionStatus.ACTIVE);
        mapping.put("deviceId2", ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus vehicleIdDeviceIdStatus =
                new VehicleIdDeviceIdStatus(Version.V1_0, mapping);
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        Mockito.when(deviceStatusApiServiceImpl.forceGet(Mockito.any(), Mockito.anyString()))
                .thenReturn(vehicleIdDeviceIdStatus);
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.empty(), Optional.empty())).thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null, null,
                false, true);

        Mockito.verify(deviceStatusApiServiceImpl, Mockito.times(1)).update(vehicleId, deviceId,
                DMAConstants.ACTIVE);
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).getOfflineBufferEntriesSortedByPriority(
                vehicleId, true, Optional.empty(), Optional.empty());
        Mockito.verify(offlineBufferDAO, Mockito.times(0)).getOfflineBufferEntriesSortedByPriority(
                vehicleId, true, Optional.ofNullable(deviceId), Optional.empty());
        // verify that the other flow is not getting executed.
        Mockito.verify(deviceStatusServiceImpl, Mockito.times(0)).get(vehicleId, null);
        Mockito.verify(deviceStatusServiceImpl, Mockito.times(0)).put(vehicleId,
                new ConcurrentHashSet<String>(), null, null);
    }

    /**
     * Tests performing action when status is inactive for ECU type.
     */
    @Test
    public void testPerformActionWhenStatusInactiveForEcuType() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";

        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.performActionWhenStatusInactive(vehicleId, deviceId, null,
                null, false, true);

        Mockito.verify(deviceStatusApiServiceImpl, Mockito.times(1)).update(vehicleId, deviceId,
                DMAConstants.INACTIVE);
        Mockito.verify(deviceStatusServiceImpl, Mockito.times(0)).get(vehicleId, null);
        Mockito.verify(deviceStatusServiceImpl, Mockito.times(0)).delete(vehicleId, deviceId, null,
                null);
    }

    /**
     * Tests performing action when status is active for one vehicle with many devices.
     */
    @Test
    public void testPerformActionWhenStatusActiveOneVehicleToManyDevice() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";

        ConcurrentHashSet<String> deviceIdsInCache = new ConcurrentHashSet<String>();
        deviceIdsInCache.add(deviceId);
        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        deviceConnectionStatusHandler.setOfflineBufferPerDevice(true);
        deviceConnectionStatusHandler.setFilteredBufferEntry(noFilterDMOfflineBufferEntryImpl);

        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        IgniteEventImpl connStatusEvent = new IgniteEventImpl();
        connStatusEvent.setVehicleId(vehicleId);
        connStatusEvent.setSourceDeviceId(deviceId);
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.ofNullable(deviceId), Optional.empty())).thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, deviceId, null, null,
                false, false);

        Mockito.verify(offlineBufferDAO, Mockito.times(0)).getOfflineBufferEntriesSortedByPriority(
                vehicleId, true, Optional.empty(), Optional.empty());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).getOfflineBufferEntriesSortedByPriority(
                vehicleId, true, Optional.ofNullable(deviceId), Optional.empty());
    }

    /**
     * Tests filtering DM offline entry.
     */
    @Test
    public void testFilterDMOffLineEntry() {
        String vehicleId = "Vehicle12345";
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<>();
        setupBufferEntries(bufferedEntries, vehicleId);

        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.empty(), Optional.empty())).thenReturn(bufferedEntries);
        Mockito.when(testFilterDMOfflineBufferEntryImpl
                .filterAndUpdateDmOfflineBufferEntries(bufferedEntries))
                .thenReturn(bufferedEntries);
        deviceConnectionStatusHandler.setFilteredBufferEntry(testFilterDMOfflineBufferEntryImpl);

        deviceConnectionStatusHandler.performActionWhenStatusActive(vehicleId, "Device12345", null,
                null, false, false);

        Mockito.verify(testFilterDMOfflineBufferEntryImpl, Mockito.times(1))
                .filterAndUpdateDmOfflineBufferEntries(bufferedEntries);
        Mockito.verify(offlineBufferDAO, Mockito.times(DEVICE_COUNT))
                .removeOfflineBufferEntry(Mockito.any(String.class));
    }

    /**
     * Tests skipping offline buffer with skip events.
     */
    @Test
    public void testSkipOfflineBufferWithSkipEvents() {

        deviceConnectionStatusHandler
                .setSkipOfflineBufferEvents(Arrays.asList("Speed", "RPM", "RemoteOperationEngine"));

        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setEventId("RPM");

        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1)).postFailureEvent(data, testKey, spc,
                msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(0)).addOfflineBufferEntry(vehicleId, testKey,
                msg, null);
    }

    /**
     * Tests skipping offline buffer with not to skip events.
     */
    @Test
    public void testSkipOfflineBufferWithNotToSkipEvents() {

        deviceConnectionStatusHandler
                .setSkipOfflineBufferEvents(Arrays.asList("Speed", "RPM", "RemoteOperationEngine"));

        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setEventId("Collision");

        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1)).postFailureEvent(data, testKey, spc,
                msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).addOfflineBufferEntry(vehicleId, testKey,
                msg, null);
    }

    /**
     * Tests skipping offline buffer with empty event list.
     */
    @Test
    public void testSkipOfflineBufferWithEmptyEventList() {

        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String service = "ecall";

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        event.setEventId("Collision");

        ReflectionTestUtils.setField(deviceConnectionStatusHandler, "connStatusRetrieverImplClass",
                "com.harman.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever");
        deviceConnectionStatusHandler.setup("0", null);
        deviceConnectionStatusHandler.setServiceName(service);
        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        deviceConnectionStatusHandler.handleDeviceInactiveState(testKey, msg);

        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(msg.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);

        Mockito.verify(deviceMessageUtils, Mockito.times(1)).postFailureEvent(data, testKey, spc,
                msg.getFeedBackTopic());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).addOfflineBufferEntry(vehicleId, testKey,
                msg, null);
    }

    /**
     * Tests getting device ID if active if target device ID is absent.
     */
    @Test(expected = DeviceMessagingException.class)
    public void testGetDeviceIdIfActiveIfTargetDeviceIdAbsent() {
        final String vehicleId = "Vehicle12345";

        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setEventId("Collision");

        ConcurrentHashSet<String> deviceIds = new ConcurrentHashSet<>();
        deviceIds.add("device123");
        deviceIds.add("device456");
        Mockito.when(deviceStatusServiceImpl.get(vehicleId, Optional.empty()))
                .thenReturn(deviceIds);
        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        deviceConnectionStatusHandler.handle(testKey, msg);
    }

    /**
     * Tests getting connection status from Redis if not found in memory.
     */
    @Test
    public void testGetConnectionStatusFromRedisIfNotFoundInMemory() {
        final String vehicleId = "Vehicle12345";
        final String deviceId = "Device12345";
        final String requestId = "req124";
        final String service = "ecall";
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(SPEED_VALUE);
        event.setRequestId(requestId);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);

        String payload = "payload";
        DeviceMessage msg =
                new DeviceMessage(payload.getBytes(), Version.V1_0, event, "topic", TIMEOUT);
        msg.isOtherBrokerConfigured(true);

        Mockito.when(deviceStatusApiServiceImpl.get(Mockito.anyString(), Mockito.any()))
                .thenReturn(null);
        ConcurrentHashMap<String, ConnectionStatus> deviceIds = new ConcurrentHashMap<>();
        deviceIds.put("Device12345", ConnectionStatus.ACTIVE);
        VehicleIdDeviceIdStatus mapping = new VehicleIdDeviceIdStatus(Version.V1_0, deviceIds);
        Mockito.when(deviceStatusUtil.getMapParentKey(Mockito.any(), Mockito.any()))
                .thenReturn("Vehicle12345");
        Mockito.when(deviceStatusApiServiceImpl.get(Mockito.anyString(), Mockito.any()))
                .thenReturn(null);
        Mockito.when(deviceStatusApiServiceImpl.forceGet(Mockito.any(), Mockito.any(String.class)))
                .thenReturn(mapping);
        deviceConnectionStatusHandler.handle(testKey, msg);

        Mockito.verify(statusRetriever, Mockito.times(0)).getConnectionStatusData(requestId,
                vehicleId, deviceId, Optional.of("subService"));
        Mockito.verify(deviceStatusApiServiceImpl, Mockito.times(1)).update(vehicleId, deviceId,
                ConnectionStatus.ACTIVE.toString());
    }

    // Refactored the long method `testFilterDMOffLineEntry` into smaller helper methods.
    private void setupBufferEntries(List<DMOfflineBufferEntry> bufferedEntries, String vehicleId) {
        DMOfflineBufferEntry bufferEntry = new DMOfflineBufferEntry();
        bufferEntry.setDeviceId("vehicle1");
        DeviceMessage event = new DeviceMessage();
        IgniteEventImpl eventImpl = new IgniteEventImpl();
        eventImpl.setEventId("eventId1");
        event.setEvent(eventImpl);
        DeviceMessageHeader deviceMessageHeader = new DeviceMessageHeader();
        deviceMessageHeader.withRequestId("reqId1").withVehicleId(vehicleId);
        event.setDeviceMessageHeader(deviceMessageHeader);
        bufferEntry.setEvent(event);
        bufferEntry.setEventTs(LocalDateTime.now());
        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey(vehicleId);
        bufferEntry.setIgniteKey(igniteKey);
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

        DMOfflineBufferEntry bufferEntry3 = new DMOfflineBufferEntry();
        bufferEntry3.setDeviceId("vehicle3");
        IgniteEventImpl eventImpl3 = new IgniteEventImpl();
        String eventId3 = "eventId3";
        eventImpl3.setEventId(eventId3);
        eventImpl3.setVehicleId(vehicleId);
        eventImpl3.setSourceDeviceId("Device12345");
        DeviceMessageHeader deviceMessageHeader3 = new DeviceMessageHeader();
        deviceMessageHeader3.withRequestId("reqId3").withVehicleId(vehicleId);
        DeviceMessage event3 = new DeviceMessage();
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
}
