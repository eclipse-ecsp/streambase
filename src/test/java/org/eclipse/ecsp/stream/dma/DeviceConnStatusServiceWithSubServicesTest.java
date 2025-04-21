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
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusServiceImpl;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.junit.Assert;
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
 * class DeviceConnStatusServiceWithSubServicesTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-handler-sub-services-test.properties")
public class DeviceConnStatusServiceWithSubServicesTest extends KafkaStreamsApplicationTestBase {
    
    /** The device status service impl. */
    @Autowired
    private DeviceStatusServiceImpl deviceStatusServiceImpl;
    
    /** The cache. */
    @Autowired
    private IgniteCache cache;

    /** The Constant SUB_SERVICE_ECALL. */
    private static final String SUB_SERVICE_ECALL = "ecall/test/ftd";
    
    /** The key. */
    String key = "vehicleId12345";
    
    /** The device id 1. */
    String deviceId1 = "deviceId12345";
    
    /**
     * Test force get if sub services exist.
     */
    @Test
    public void testForceGetIfSubServicesExist() {
        String key2 = "vehicleId12346";
        Assert.assertNull(deviceStatusServiceImpl.get(key2, Optional.of(SUB_SERVICE_ECALL)));

        DeviceStatusKey mapEntryKey = new DeviceStatusKey(key2);
        String mapKey = "VEHICLE_DEVICE_MAPPING" + ":" + SUB_SERVICE_ECALL;
        PutMapOfEntitiesRequest<VehicleIdDeviceIdMapping> putRequest 
            = new PutMapOfEntitiesRequest<VehicleIdDeviceIdMapping>();
        putRequest.withKey(mapKey);
        Map<String, VehicleIdDeviceIdMapping> map = new HashMap<>();
        VehicleIdDeviceIdMapping mapEntryValue = new VehicleIdDeviceIdMapping();
        mapEntryValue.addDeviceId(deviceId1);
        map.put(mapEntryKey.convertToString(), mapEntryValue);
        putRequest.withValue(map);
        putRequest.withNamespaceEnabled(false);
        cache.putMapOfEntities(putRequest);

        ConcurrentHashSet<String> actualDeviceIds = new ConcurrentHashSet<>();
        actualDeviceIds.add(deviceId1);
        ConcurrentHashSet<String> deviceIds = deviceStatusServiceImpl.forceGet(key2, Optional.of(SUB_SERVICE_ECALL));
        Assert.assertEquals(actualDeviceIds, deviceIds);
    }
}
