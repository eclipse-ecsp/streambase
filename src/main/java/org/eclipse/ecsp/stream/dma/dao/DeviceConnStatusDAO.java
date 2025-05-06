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

import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.analytics.stream.base.stores.MutableKeyValueStore;
import org.eclipse.ecsp.analytics.stream.base.stores.SortedKeyValueStore;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;

import java.util.Optional;


/**
 * interface DeviceConnStatusDAO extends MutableKeyValueStore.
 */
public interface DeviceConnStatusDAO extends MutableKeyValueStore<DeviceStatusKey, VehicleIdDeviceIdMapping>,
        SortedKeyValueStore<DeviceStatusKey, VehicleIdDeviceIdMapping> {

    /**
     * Gets the offset metadata.
     *
     * @param serviceName the service name
     * @return the offset metadata
     */
    public Optional<OffsetMetadata> getOffsetMetadata(String serviceName);

    /**
     * Initialize.
     */
    public void initialize();

}