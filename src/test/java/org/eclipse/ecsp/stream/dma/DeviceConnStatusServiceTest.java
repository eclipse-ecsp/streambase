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

package org.eclipse.ecsp.stream.dma;

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutMapOfEntitiesRequest;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.stream.dma.dao.DeviceConnStatusDAO;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusServiceImpl;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;



/**
 * class DeviceConnStatusServiceTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-handler-test.properties")
public class DeviceConnStatusServiceTest extends KafkaStreamsApplicationTestBase {

    /** The key. */
    String key = "vehicleId12345";
    
    /** The device id 1. */
    String deviceId1 = "deviceId12345";
    
    /** The device id 2. */
    String deviceId2 = "deviceId12346";
    
    /** The device id 3. */
    String deviceId3 = "deviceId12347";
    
    /** The sub service 1. */
    String subService1 = "ecall/test_service/ubi";
    
    /** The sub service 2. */
    String subService2 = "ecall/test_service/ftd";
    
    /** The device status service impl. */
    @Autowired
    private DeviceStatusServiceImpl deviceStatusServiceImpl;
    
    /** The cache. */
    @Autowired
    private IgniteCache cache;
    
    /** The device status DAO. */
    @Autowired
    private DeviceConnStatusDAO deviceStatusDAO;
    
    /** The value. */
    private ConcurrentHashSet<String> value;

    /**
     * setUp().
     */
    @Before
    public void setUp() {
        value = new ConcurrentHashSet<String>();
        value.add(deviceId1);
        value.add(deviceId2);
        value.add(deviceId3);
        deviceStatusServiceImpl.put(key, value, null, Optional.empty());
    }

    /**
     * Clear.
     */
    @After
    public void clear() {
        deviceStatusServiceImpl.deleteKey(key, null);
    }

    /**
     * Test get device status service.
     */
    @Test
    public void testGetDeviceStatusService() {
        Assert.assertEquals(value, deviceStatusServiceImpl.get(key, Optional.empty()));
    }

    /**
     * Test get device status service for sub service.
     */
    @Test
    public void testGetDeviceStatusServiceForSubService() {
        clear();
        deviceStatusServiceImpl.put(key, value, null, Optional.of(subService1));
        Assert.assertEquals(value, deviceStatusServiceImpl.get(key, Optional.of(subService1)));
    }

    /**
     * Test delete single device id.
     */
    @Test
    public void testDeleteSingleDeviceId() {
        String deviceIdToDelete = deviceId1;
        deviceStatusServiceImpl.delete(key, deviceIdToDelete, null, Optional.empty());

        ConcurrentHashSet<String> valueExpected = new ConcurrentHashSet<String>();
        valueExpected.add(deviceId2);
        valueExpected.add(deviceId3);

        Assert.assertEquals(valueExpected, deviceStatusServiceImpl.get(key, Optional.empty()));

    }

    /**
     * Test delete key.
     */
    @Test
    public void testDeleteKey() {
        deviceStatusServiceImpl.deleteKey(key, null);
        Assert.assertNull(deviceStatusServiceImpl.get(key, Optional.empty()));
    }

    /**
     * Test delete key for sub service.
     */
    @Test
    public void testDeleteKeyForSubService() {
        ConcurrentHashSet<String> set = new ConcurrentHashSet<String>();
        set.add(deviceId2);
        deviceStatusServiceImpl.put(key, set, null, Optional.of("subService1"));

        String deviceIdToDelete = deviceId2;
        deviceStatusServiceImpl.delete(key, deviceIdToDelete, null, Optional.of("subService1"));

        Assert.assertNull(deviceStatusServiceImpl.get(key, Optional.of("subService1")));
    }

    /**
     * Test force get.
     */
    @Test
    public void testForceGet() {
        String key2 = "vehicleId12346";
        Assert.assertNull(deviceStatusServiceImpl.get(key2, Optional.empty()));

        DeviceStatusKey mapEntryKey = new DeviceStatusKey(key2);
        String mapKey = "VEHICLE_DEVICE_MAPPING:Ecall";
        PutMapOfEntitiesRequest<VehicleIdDeviceIdMapping> putRequest =
                new PutMapOfEntitiesRequest<VehicleIdDeviceIdMapping>();
        putRequest.withKey(mapKey);
        Map<String, VehicleIdDeviceIdMapping> map = new HashMap<String, VehicleIdDeviceIdMapping>();
        VehicleIdDeviceIdMapping mapEntryValue = new VehicleIdDeviceIdMapping();
        mapEntryValue.addDeviceId(deviceId1);
        map.put(mapEntryKey.convertToString(), mapEntryValue);
        putRequest.withValue(map);
        putRequest.withNamespaceEnabled(false);
        cache.putMapOfEntities(putRequest);

        ConcurrentHashSet<String> actualDeviceIds = new ConcurrentHashSet<String>();
        actualDeviceIds.add(deviceId1);
        ConcurrentHashSet<String> deviceIds = deviceStatusServiceImpl
                .forceGet(key2, Optional.empty());
        Assert.assertEquals(actualDeviceIds, deviceIds);
    }

    /**
     * Test get device status when mapping is present in memory with null devices.
     */
    @Test
    public void testGetDeviceStatusWhenMappingIsPresentInMemoryWithNullDevices() {
        clear();
        deviceStatusServiceImpl.put(key, null, null, Optional.empty());
        Assert.assertNull(deviceStatusServiceImpl.get(key, Optional.empty()));
        Assert.assertNull(deviceStatusDAO.get(new DeviceStatusKey(key)).getDeviceIds());
    }

    /**
     * Test get device status when mapping is present in memory with zero devices.
     */
    @Test
    public void testGetDeviceStatusWhenMappingIsPresentInMemoryWithZeroDevices() {
        clear();
        deviceStatusServiceImpl.put(key, new ConcurrentHashSet<>(), null, Optional.empty());
        Assert.assertNull(deviceStatusServiceImpl.get(key, Optional.empty()));
        Assert.assertEquals(0, deviceStatusDAO.get(new DeviceStatusKey(key)).getDeviceIds().size());
    }

    /**
     * Test get device status when mapping is not present in memory.
     */
    @Test
    public void testGetDeviceStatusWhenMappingIsNotPresentInMemory() {
        clear();
        Assert.assertNull(deviceStatusServiceImpl.get(key, Optional.empty()));
        Assert.assertNull(deviceStatusDAO.get(new DeviceStatusKey(key)));
    }

}
