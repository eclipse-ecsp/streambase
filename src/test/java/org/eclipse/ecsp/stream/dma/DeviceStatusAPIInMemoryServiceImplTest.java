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

import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusAPIInMemoryServiceImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoInMemoryCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.eclipse.ecsp.stream.dma.dao.DMAConstants.ACTIVE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * {@link DeviceStatusAPIInMemoryServiceImplTest} UT class for {@link DeviceStatusAPIInMemoryServiceImpl}.
 */
public class DeviceStatusAPIInMemoryServiceImplTest {

    /** The device status API in memory service. */
    @InjectMocks
    private DeviceStatusAPIInMemoryServiceImpl deviceStatusAPIInMemoryService;
    
    /** The key. */
    private String key = "Vehicle12345";
    
    /** The device status dao. */
    @Mock
    private DeviceStatusDaoInMemoryCache deviceStatusDao;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    /**
     * Test get device id status from in memory.
     */
    @Test
    public void testGetDeviceIdStatusFromInMemory() {
        VehicleIdDeviceIdStatus vehicleIdDeviceIdStatus = new VehicleIdDeviceIdStatus();
        when(deviceStatusDao.get(any())).thenReturn(vehicleIdDeviceIdStatus);
        Assert.assertEquals(vehicleIdDeviceIdStatus, deviceStatusAPIInMemoryService.get(key));
    }

    /**
     * Test get device id status when status not present in memory.
     */
    @Test
    public void testGetDeviceIdStatusWhenStatusNotPresentInMemory() {
        VehicleIdDeviceIdStatus vehicleIdDeviceIdStatus = new VehicleIdDeviceIdStatus();
        Assert.assertNull(deviceStatusAPIInMemoryService.get(key));
    }

    /**
     * Test update device id status when status not present in memory.
     */
    @Test
    public void testUpdateDeviceIdStatusWhenStatusNotPresentInMemory() {
        deviceStatusAPIInMemoryService.update(key, "d1", ACTIVE);
        verify(deviceStatusDao, times(1)).putIfAbsent(any(), any(), any(), any());
    }

    /**
     * Test update device id status when status is present in memory.
     */
    @Test
    public void testUpdateDeviceIdStatusWhenStatusIsPresentInMemory() {
        VehicleIdDeviceIdStatus vehicleIdDeviceIdStatus = new VehicleIdDeviceIdStatus();
        when(deviceStatusDao.get(any())).thenReturn(vehicleIdDeviceIdStatus);
        deviceStatusAPIInMemoryService.update(key, "d1", ACTIVE);
        verify(deviceStatusDao, times(0)).putIfAbsent(any(), any(), any(), any());
        verify(deviceStatusDao, times(1)).put(any(), any(), any());
    }
}
