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

import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.stream.dma.dao.DeviceConnStatusDAO;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusServiceImpl;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;



/**
 * {@link DeviceStatusServiceImplTest} test class for {@link DeviceStatusServiceImpl}.
 */
public class DeviceStatusServiceImplTest {

    /** The device status service impl. */
    @InjectMocks
    private DeviceStatusServiceImpl deviceStatusServiceImpl;
    
    /** The device status DAO. */
    @Mock
    private DeviceConnStatusDAO deviceStatusDAO;
    
    /** The key. */
    private String key = "Vehicle12345";
    
    /** The device ids. */
    private ConcurrentHashSet<String> deviceIds;

    /**
     * setup.
     */
    @Before
    public void setup() {
        deviceIds = new ConcurrentHashSet<>();
        deviceIds.add("deviceId1");
        deviceIds.add("deviceId2");
        MockitoAnnotations.openMocks(this);
    }

    /**
     * Test get device id if mapping found in memory.
     */
    @Test
    public void testGetDeviceIdIfMappingFoundInMemory() {
        when(deviceStatusDAO.get(any())).thenReturn(new VehicleIdDeviceIdMapping(Version.V1_0, deviceIds));
        deviceStatusServiceImpl.get(key, Optional.empty());
        verify(deviceStatusDAO, Mockito.times(0)).put(any(), any(), any(), any());
    }

    /**
     * Test get device id if mapping present in memory with null device ids.
     */
    @Test
    public void testGetDeviceIdIfMappingPresentInMemoryWithNullDeviceIds() {
        when(deviceStatusDAO.get(any())).thenReturn(new VehicleIdDeviceIdMapping(Version.V1_0, null));
        when(deviceStatusDAO.forceGet(any(), any())).thenReturn(new VehicleIdDeviceIdMapping(Version.V1_0, deviceIds));
        deviceStatusServiceImpl.get(key, Optional.empty());
        verify(deviceStatusDAO, Mockito.times(1)).put(any(), any(), any(), any());
    }

    /**
     * Test get device id if mapping present in memory with zero device ids.
     */
    @Test
    public void testGetDeviceIdIfMappingPresentInMemoryWithZeroDeviceIds() {
        when(deviceStatusDAO.get(any())).thenReturn(new VehicleIdDeviceIdMapping(Version.V1_0,
                new ConcurrentHashSet<>()));
        when(deviceStatusDAO.forceGet(any(), any())).thenReturn(new VehicleIdDeviceIdMapping(Version.V1_0, deviceIds));
        deviceStatusServiceImpl.get(key, Optional.empty());
        verify(deviceStatusDAO, Mockito.times(1)).put(any(), any(), any(), any());
    }

    /**
     * Test get device id if mapping is not present in memory.
     */
    @Test
    public void testGetDeviceIdIfMappingIsNotPresentInMemory() {
        when(deviceStatusDAO.get(any())).thenReturn(null);
        when(deviceStatusDAO.forceGet(any(), any())).thenReturn(new VehicleIdDeviceIdMapping(Version.V1_0, deviceIds));
        deviceStatusServiceImpl.get(key, Optional.of("subService1"));
        verify(deviceStatusDAO, Mockito.times(1)).put(any(), any(), any(), any());
    }

    /**
     * Test get device id if mapping is not present in memory and not present in redis.
     */
    @Test
    public void testGetDeviceIdIfMappingIsNotPresentInMemoryAndNotPresentInRedis() {
        when(deviceStatusDAO.get(any())).thenReturn(null);
        when(deviceStatusDAO.forceGet(any(), any())).thenReturn(new VehicleIdDeviceIdMapping(Version.V1_0, null));
        deviceStatusServiceImpl.get(key, Optional.of("subService1"));
        verify(deviceStatusDAO, Mockito.times(0)).put(any(), any(), any(), any());
    }
}
