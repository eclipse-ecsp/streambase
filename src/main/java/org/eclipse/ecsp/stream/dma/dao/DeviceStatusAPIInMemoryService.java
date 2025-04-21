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

package org.eclipse.ecsp.stream.dma.dao;

import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;


/**
 * A DAO layer for in-memory connection status cache.
 * Check {@link DeviceStatusDaoInMemoryCache} for details on the in-memory HashMap data structure.
 *
 * @author hbadshah
 *

 */
public interface DeviceStatusAPIInMemoryService {
    
    /**
     * Get from in-memory cache.
     *
     * @param vehicleId the vehicle id
     * @return the vehicle id device id status
     */
    public VehicleIdDeviceIdStatus get(String vehicleId);

    /**
     * Puts data in in-memory cache if the data does not exist. Else, updates the data in in-memory cache.
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     * @param connectionStatus the connection status
     */
    public void update(String vehicleId, String deviceId, String connectionStatus);

    /**
     * Force get.
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     */
    public void forceGet(String vehicleId, String deviceId);
}
