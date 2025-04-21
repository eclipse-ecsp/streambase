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
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutMapOfEntitiesRequest;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;


/**
 * This test class is to verify whether the in-memory state store can sync-up with redis.
 *
 * @author hbadshah
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-handler-sub-services-test.properties")
public class SynchronizationIntegrationWithSubServicesTest extends KafkaStreamsApplicationTestBase {

    /** The device status cache backed in memory DAO. */
    @Autowired
    private DeviceStatusDaoCacheBackedInMemoryImpl deviceStatusCacheBackedInMemoryDAO;

    /** The cache. */
    @Autowired
    private IgniteCache cache;

    /** The transformer. */
    @Autowired
    private DeviceMessageIgniteEventTransformer transformer;

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /**
     * Test device conn sync with cache integration if sub services configured.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testDeviceConnSyncWithCacheIntegrationIfSubServicesConfigured() throws InterruptedException {
        ConcurrentHashSet<String> value = new ConcurrentHashSet<String>();
        value.add("deviceId12345");
        VehicleIdDeviceIdMapping mapping = new VehicleIdDeviceIdMapping(Version.V1_0, value);

        ConcurrentHashSet<String> value2 = new ConcurrentHashSet<String>();
        value2.add("deviceId22345");
        VehicleIdDeviceIdMapping mapping2 = new VehicleIdDeviceIdMapping(Version.V1_0, value2);

        ConcurrentHashSet<String> value3 = new ConcurrentHashSet<String>();
        value3.add("deviceId32345");
        VehicleIdDeviceIdMapping mapping3 = new VehicleIdDeviceIdMapping(Version.V1_0, value3);

        DeviceStatusKey abc = new DeviceStatusKey("abc");
        DeviceStatusKey efg = new DeviceStatusKey("efg");
        DeviceStatusKey hij = new DeviceStatusKey("hij");

        // Device Connection status from now on will be put in to cache from
        // HiveMq and not by DMA

        putToCacheForSubServices(abc, mapping);
        putToCacheForSubServices(efg, mapping2);
        putToCacheForSubServices(hij, mapping3);
        deviceStatusCacheBackedInMemoryDAO.initialize();

        //After initialization, below as argument to DeviceStatusKey's constructor, is how the keys
        //will be stored in DMA's in-memory map. Combination of VIN+subService.
        DeviceStatusKey abcWithSubService = new DeviceStatusKey("abc;ecall/test/ubi");
        DeviceStatusKey efgWithSubService = new DeviceStatusKey("efg;ecall/test/ubi");
        DeviceStatusKey hijWithSubService = new DeviceStatusKey("hij;ecall/test/ftd");
        Assert.assertNull(deviceStatusCacheBackedInMemoryDAO.get(abcWithSubService));
        Assert.assertNull(deviceStatusCacheBackedInMemoryDAO.get(efgWithSubService));
        Assert.assertNull(deviceStatusCacheBackedInMemoryDAO.get(hijWithSubService));
    }

    /**
     * Put to cache for sub services.
     *
     * @param key the key
     * @param value the value
     */
    private void putToCacheForSubServices(DeviceStatusKey key, VehicleIdDeviceIdMapping value) {
        PutMapOfEntitiesRequest<VehicleIdDeviceIdMapping> putRequest = new PutMapOfEntitiesRequest<>();
        if (key.convertToString().equals("hij")) {
            putRequest.withKey("VEHICLE_DEVICE_MAPPING:ecall/test/ftd");
        } else {
            putRequest.withKey("VEHICLE_DEVICE_MAPPING:ecall/test/ubi");
        }
        Map<String, VehicleIdDeviceIdMapping> pair = new HashMap<String, VehicleIdDeviceIdMapping>();
        pair.put(key.convertToString(), value);
        putRequest.withValue(pair);
        putRequest.withNamespaceEnabled(false);
        cache.putMapOfEntities(putRequest);
    }
}