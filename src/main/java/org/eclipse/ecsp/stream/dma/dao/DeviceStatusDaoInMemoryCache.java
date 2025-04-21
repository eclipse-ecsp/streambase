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

import jakarta.annotation.PostConstruct;
import org.eclipse.ecsp.analytics.stream.base.stores.CachedMapStateStore;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.springframework.stereotype.Repository;


/**
 *         In addition to DeviceStatusDaoCacheBackedInMemoryImpl
 *         data structure, DMA maintains below data structure too.
 *         Where key of the in-memory map is vehicleId wrapped in
 *         DeviceStatusKey and value against each key is an instance of
 *         VehicleIdDeviceIdToStatusMapping which in turn contains
 *         a mapping of deviceId to its connection status.
 *         It differs from the existing in-memory DS in the sense that,
 *         existing stores mapping of vehicleId-VehicleIdDeviceIdMapping and
 *         this one stores mapping of vehicleId-VehicleIdDeviceIdStatus
 *         RDNG: 170506, 170507
 *
 *
 */
@Repository
public class DeviceStatusDaoInMemoryCache extends CachedMapStateStore<DeviceStatusKey, VehicleIdDeviceIdStatus> {

    /**
     * Inits the.
     */
    @PostConstruct
    public void init() {
        setPersistInIgniteCache(false);
    }
}
