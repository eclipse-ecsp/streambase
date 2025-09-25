package org.eclipse.ecsp.stream.dma;

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutMapOfEntitiesRequest;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.DeviceConnStatusDao;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Integration tests for DeviceStatusApiService.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {Launcher.class})
@EnableRuleMigrationSupport
@TestPropertySource("/dma-handler-test.properties")
public class DeviceStatusApiServiceIntegrationTest extends KafkaStreamsApplicationTestBase {

    @Qualifier("deviceStatusApiServiceImpl")
    @Autowired
    private DeviceStatusService<VehicleIdDeviceIdStatus> deviceStatusServiceImpl;

    @Qualifier("deviceConnStatusApiDaoImpl")
    @Autowired
    private DeviceConnStatusDao<VehicleIdDeviceIdStatus> deviceStatusDAO;

    String key = "vehicleId12345";
    String key2 = "vehicleId123456";
    String deviceId1 = "deviceId12345";
    String deviceId2 = "deviceId12346";
    String deviceId3 = "deviceId12347";
    String subService1 = "ecall/test_service/subService1";
    String subService2 = "ecall/test_service/subService2";

    Map<String, DeviceConnStatusV1_0.ConnectionStatus> deviceStatusMap = new ConcurrentHashMap<>();

    VehicleIdDeviceIdStatus mapping;
    @Autowired
    private IgniteCache cache;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setUp() {
        deviceStatusMap.put(deviceId1, DeviceConnStatusV1_0.ConnectionStatus.ACTIVE);
        mapping = new VehicleIdDeviceIdStatus(Version.V1_0, deviceStatusMap);
        deviceStatusServiceImpl.update(key, deviceId1,
                String.valueOf(DeviceConnStatusV1_0.ConnectionStatus.ACTIVE));
    }

    /**
     * Clears the test environment.
     */
    @After
    public void clear() {
        deviceStatusServiceImpl.deleteKey(key, null);
    }

    /**
     * Tests the getDeviceStatusService method.
     */
    @Test
    public void testGetDeviceStatusService() {
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withVehicleId(key).withTargetDeviceId(deviceId1);
        Assert.assertEquals(mapping.getDeviceIds(),
                deviceStatusServiceImpl.get(key, Optional.empty()).getDeviceIds());
    }

    /**
     * Tests forceGetAndUpdateInMemoryWhenStatusNotPresentInCache method.
     */
    @Test
    public void testForceGetAndUpdateInMemoryWhenStatusNotPresentInCache() {

        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withVehicleId(key2).withTargetDeviceId(deviceId2);
        Assert.assertNull(deviceStatusServiceImpl.get(key2, Optional.empty()));
        DeviceStatusKey mapEntryKey = new DeviceStatusKey(key2);
        String mapKey = "VEHICLE_DEVICE_MAPPING:Ecall";
        PutMapOfEntitiesRequest<VehicleIdDeviceIdMapping> putRequest =
                new PutMapOfEntitiesRequest<>();
        putRequest.withKey(mapKey);
        Map<String, VehicleIdDeviceIdMapping> map = new HashMap<>();
        VehicleIdDeviceIdMapping mapEntryValue = new VehicleIdDeviceIdMapping();
        mapEntryValue.addDeviceId(deviceId2);
        map.put(mapEntryKey.convertToString(), mapEntryValue);
        putRequest.withValue(map);
        putRequest.withNamespaceEnabled(false);
        cache.putMapOfEntities(putRequest);
        VehicleIdDeviceIdStatus status =
                deviceStatusServiceImpl.forceGet(Optional.empty(), header.getVehicleId());
        Assert.assertEquals("ACTIVE", status.getDeviceIds().get(deviceId2).getConnectionStatus());
        deviceStatusServiceImpl.update(key2, deviceId2,
                status.getDeviceIds().get(deviceId2).getConnectionStatus());
        Assert.assertNotNull(deviceStatusServiceImpl.get(key2, Optional.empty()));
        Assert.assertEquals("ACTIVE", deviceStatusServiceImpl.get(key2, Optional.empty())
                .getDeviceIds().get(deviceId2).getConnectionStatus());
    }
}
