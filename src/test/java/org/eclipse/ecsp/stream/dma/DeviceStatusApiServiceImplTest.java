package org.eclipse.ecsp.stream.dma;

import org.eclipse.ecsp.domain.DeviceConnStatusV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.DeviceConnStatusDao;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusApiServiceImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusUtil;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.eclipse.ecsp.stream.dma.dao.DMAConstants.ACTIVE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for DeviceStatusApiServiceImpl.
 */
public class DeviceStatusApiServiceImplTest {

    @InjectMocks
    private DeviceStatusApiServiceImpl deviceStatusAPIInMemoryService;
    private String key = "Vehicle12345";

    @Mock
    private DeviceConnStatusDao<VehicleIdDeviceIdStatus> deviceConnStatusDao;
    @Mock
    private DeviceConnStatusDao<VehicleIdDeviceIdMapping> deviceStatusDao;

    @Mock
    private DeviceStatusUtil deviceStatusUtil;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetDeviceIdStatusFromInMemory() {
        VehicleIdDeviceIdStatus vehicleIdDeviceIdStatus = new VehicleIdDeviceIdStatus();
        when(deviceConnStatusDao.get(any())).thenReturn(vehicleIdDeviceIdStatus);
        Assert.assertEquals(vehicleIdDeviceIdStatus,
                deviceStatusAPIInMemoryService.get(key, Optional.of("subService")));
    }

    @Test
    public void testGetDeviceIdStatusWhenStatusNotPresentInMemory() {
        VehicleIdDeviceIdStatus vehicleIdDeviceIdStatus = new VehicleIdDeviceIdStatus();
        Assert.assertNull(deviceStatusAPIInMemoryService.get(key, Optional.of("subService")));
    }

    @Test
    public void testUpdateDeviceIdStatusWhenStatusNotPresentInMemory() {
        deviceStatusAPIInMemoryService.update(key, "d1", ACTIVE);
        verify(deviceConnStatusDao, times(1)).putIfAbsent(any(), any(), any(), any());
    }

    @Test
    public void testPutInMemory() {
        Map<String, DeviceConnStatusV1_0.ConnectionStatus> deviceIds = new ConcurrentHashMap<>();
        deviceIds.put("test", DeviceConnStatusV1_0.ConnectionStatus.ACTIVE);
        when(deviceConnStatusDao.get(any()))
                .thenReturn(new VehicleIdDeviceIdStatus(Version.V1_0, deviceIds));
        deviceStatusAPIInMemoryService.put(key,
                new VehicleIdDeviceIdStatus(Version.V1_0, deviceIds), Optional.empty(),
                Optional.empty());
        verify(deviceConnStatusDao, times(1)).putIfAbsent(any(), any(), any(), any());
    }

    @Test
    public void testDeleteFromInMemory() {
        Map<String, DeviceConnStatusV1_0.ConnectionStatus> deviceIds = new ConcurrentHashMap<>();

        deviceIds.put("test", DeviceConnStatusV1_0.ConnectionStatus.ACTIVE);
        when(deviceConnStatusDao.get(any()))
                .thenReturn(new VehicleIdDeviceIdStatus(Version.V1_0, deviceIds));
        deviceStatusAPIInMemoryService.delete(key, "test", Optional.empty(), Optional.empty());
        verify(deviceConnStatusDao, times(0)).putIfAbsent(any(), any(), any(), any());
    }

    @Test
    public void testDeleteKeyInMemory() {
        Map<String, DeviceConnStatusV1_0.ConnectionStatus> deviceIds = new ConcurrentHashMap<>();

        deviceIds.put("test", DeviceConnStatusV1_0.ConnectionStatus.ACTIVE);
        when(deviceConnStatusDao.get(any()))
                .thenReturn(new VehicleIdDeviceIdStatus(Version.V1_0, deviceIds));
        deviceStatusAPIInMemoryService.deleteKey(key, Optional.empty());
        verify(deviceConnStatusDao, times(1)).deleteFromMap(any(), any(), any(), any());
    }

    @Test
    public void testUpdateDeviceIdStatusWhenStatusIsPresentInMemory() {
        VehicleIdDeviceIdStatus vehicleIdDeviceIdStatus = new VehicleIdDeviceIdStatus();
        when(deviceConnStatusDao.get(any())).thenReturn(vehicleIdDeviceIdStatus);
        deviceStatusAPIInMemoryService.update(key, "d1", ACTIVE);
        verify(deviceConnStatusDao, times(0)).putIfAbsent(any(), any(), any(), any());
    }

    @Test
    public void testReadFromRedisIfNotFoundInCache() {
        VehicleIdDeviceIdMapping vehicleIdDeviceIdMapping = new VehicleIdDeviceIdMapping();
        DeviceConnStatusV1_0.ConnectionStatus connectionStatus =
                DeviceConnStatusV1_0.ConnectionStatus.ACTIVE;
        ConcurrentHashSet deviceStatusSet = new ConcurrentHashSet<>();
        deviceStatusSet.add("v1");
        vehicleIdDeviceIdMapping.setDeviceIds(deviceStatusSet);
        when(deviceStatusUtil.getMapParentKey(Mockito.anyString(), any())).thenReturn("v1");
        when(deviceStatusDao.forceGet(Mockito.anyString(), any()))
                .thenReturn(vehicleIdDeviceIdMapping);
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withVehicleId("v1");
        deviceStatusAPIInMemoryService.forceGet(Optional.of("v1"), header.getVehicleId());
        verify(deviceConnStatusDao, times(0)).get(any());
    }

    @Test
    public void testReadFromRedisIfSubServiceNotPresentInCache() {
        VehicleIdDeviceIdMapping vehicleIdDeviceIdMapping = new VehicleIdDeviceIdMapping();
        DeviceConnStatusV1_0.ConnectionStatus connectionStatus =
                DeviceConnStatusV1_0.ConnectionStatus.ACTIVE;

        HashMap<String, DeviceConnStatusV1_0.ConnectionStatus> deviceStatusMap = new HashMap<>();
        deviceStatusMap.put("v1", connectionStatus);
        vehicleIdDeviceIdMapping.addDeviceId("v1");

        ReflectionTestUtils.setField(deviceStatusAPIInMemoryService, "mapParentKey",
                "VEHICLE_DEVICE_MAPPING:test");

        when(deviceStatusDao.forceGet(Mockito.anyString(), any()))
                .thenReturn(vehicleIdDeviceIdMapping);
        Assert.assertEquals(connectionStatus, deviceStatusAPIInMemoryService
                .forceGet(Optional.empty(), "v1").getDeviceIds().get("v1"));
    }

    @Test
    public void testReadFromRedisIfsubservicePresentInCache() {
        DeviceConnStatusV1_0.ConnectionStatus connectionStatus =
                DeviceConnStatusV1_0.ConnectionStatus.ACTIVE;

        HashMap<String, DeviceConnStatusV1_0.ConnectionStatus> deviceStatusMap = new HashMap<>();
        deviceStatusMap.put("v1", connectionStatus);
        HashMap<String, String> subServiceToParentKeyMapping = new HashMap<>();
        subServiceToParentKeyMapping.put("fleet", "VEHICLE_DEVICE_MAPPING:test/fleet");
        subServiceToParentKeyMapping.put("ubi", "VEHICLE_DEVICE_MAPPING:test/ubi");
        VehicleIdDeviceIdMapping vehicleIdDeviceIdMapping = new VehicleIdDeviceIdMapping();
        vehicleIdDeviceIdMapping.addDeviceId("v1");

        ReflectionTestUtils.setField(deviceStatusAPIInMemoryService, "subServiceToParentKeyMapping",
                subServiceToParentKeyMapping);

        when(deviceStatusUtil.getSubServiceToParentKeyMapping())
                .thenReturn(subServiceToParentKeyMapping);
        when(deviceStatusDao.forceGet(Mockito.anyString(), any()))
                .thenReturn(vehicleIdDeviceIdMapping);
        Assert.assertEquals(connectionStatus, deviceStatusAPIInMemoryService
                .forceGet(Optional.of("fleet"), "v1").getDeviceIds().get("v1"));
    }
}
