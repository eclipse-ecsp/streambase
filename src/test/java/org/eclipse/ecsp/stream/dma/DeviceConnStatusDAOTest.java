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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.stores.CacheBypass;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.cache.GetMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.eclipse.ecsp.utils.metrics.InternalCacheGuage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;



/**
 * class DeviceConnStatusDAOTest UT {@link org.eclipse.ecsp.stream.dma.dao.DeviceConnStatusDAO}.
 */
public class DeviceConnStatusDAOTest {
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /** The device status cache backed in memory DAO. */
    @InjectMocks
    private DeviceStatusDaoCacheBackedInMemoryImpl deviceStatusCacheBackedInMemoryDAO;

    /** The cache. */
    @Mock
    private IgniteCache cache;

    /** The bypass. */
    @Mock
    private CacheBypass bypass;

    /** The cache guage. */
    @Mock
    private InternalCacheGuage cacheGuage;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        deviceStatusCacheBackedInMemoryDAO.initialize();
    }

    /**
     * Close.
     */
    @After
    public void close() {
        deviceStatusCacheBackedInMemoryDAO.close();
    }

    /**
     * Test device status DAO.
     */
    @Test
    public void testDeviceStatusDAO() {
        String key = "vehicleId12345";
        DeviceStatusKey deviceStatusKey = new DeviceStatusKey(key);
        ConcurrentHashSet<String> value = new ConcurrentHashSet<String>();
        value.add("deviceId12345");
        VehicleIdDeviceIdMapping mapping = new VehicleIdDeviceIdMapping(Version.V1_0, value);
        deviceStatusCacheBackedInMemoryDAO.putToMap(DeviceStatusKey
                        .getMapKey(serviceName), deviceStatusKey, mapping, null,
                InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);

        Assert.assertEquals(mapping, deviceStatusCacheBackedInMemoryDAO.get(deviceStatusKey));

        deviceStatusCacheBackedInMemoryDAO.deleteFromMap(DeviceStatusKey.getMapKey(serviceName), deviceStatusKey, null,
                InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);

        Assert.assertNull(deviceStatusCacheBackedInMemoryDAO.get(deviceStatusKey));

    }

    /**
     * Test sync with cache.
     */
    @Test
    public void testSyncWithCache() {
        ConcurrentHashSet<String> value = new ConcurrentHashSet<String>();
        value.add("deviceId12345");
        VehicleIdDeviceIdMapping mapping = new VehicleIdDeviceIdMapping(Version.V1_0, value);

        ConcurrentHashSet<String> value2 = new ConcurrentHashSet<String>();
        value.add("deviceId22345");
        VehicleIdDeviceIdMapping mapping2 = new VehicleIdDeviceIdMapping(Version.V1_0, value2);

        Map<String, IgniteEntity> map = new HashMap<String, IgniteEntity>();
        map.put("123", mapping);
        map.put("223", mapping2);
        deviceStatusCacheBackedInMemoryDAO.setServiceName(serviceName);
        deviceStatusCacheBackedInMemoryDAO.setSubServices("ecall");
        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(map);
        deviceStatusCacheBackedInMemoryDAO.initialize();
        Assert.assertNull(deviceStatusCacheBackedInMemoryDAO.get(new DeviceStatusKey("123")));
        Assert.assertNull(deviceStatusCacheBackedInMemoryDAO.get(new DeviceStatusKey("223")));

    }
}