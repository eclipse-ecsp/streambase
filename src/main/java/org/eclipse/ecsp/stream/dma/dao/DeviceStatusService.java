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

import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import java.util.Optional;

/**
 * Service interface for managing device connection statuses.
 *
 * @param <T> The type of the device connection status object.
 */
public interface DeviceStatusService<T> {

    /**
     * Retrieves the connection status data for a given vehicle ID.
     *
     * @param vehicleId  The vehicle ID.
     * @param subService Optional sub-service identifier.
     * @return The connection status data for the vehicle.
     */
    public T get(String vehicleId, Optional<String> subService);

    /**
     * Stores or updates connection status data in the in-memory cache.
     *
     * @param vehicleId  The vehicle ID.
     * @param deviceIds  The deviceIds associated with this vehicleId.
     * @param mutationId Optional mutation identifier.
     * @param subService Optional sub-service identifier.
     */
    public void put(String vehicleId, T deviceIds, Optional<MutationId> mutationId, Optional<String> subService);

    /**
     * Deletes connection status data for a specific device from the in-memory
     * cache.
     *
     * @param vehicleId  The vehicle ID.
     * @param deviceId   The device ID to delete.
     * @param mutationId Optional mutation identifier.
     * @param subService Optional sub-service identifier.
     */
    public void delete(String vehicleId, String deviceId, Optional<MutationId> mutationId, Optional<String> subService);

    /**
     * Deletes all connection status data for a given vehicle ID from the in-memory
     * cache.
     *
     * @param vehicleId  The vehicle ID.
     * @param mutationId Optional mutation identifier.
     */
    public void deleteKey(String vehicleId, Optional<MutationId> mutationId);

    /**
     * Retrieves the latest offset metadata for a given service name.
     *
     * @param serviceName The name of the service.
     * @return An {@link Optional} containing the latest offset metadata, if
     *         available.
     */
    public Optional<OffsetMetadata> getOffsetMetadata(String serviceName);

    /**
     * Retrieves connection status data directly from the cache, bypassing the
     * in-memory store.
     *
     * @param subService Optional sub-service identifier.
     * @param vehicleId  The vehicle ID.
     * @return The connection status data for the vehicle.
     */
    public T forceGet(String vehicleId, Optional<String> subService);

    /**
     * Updates the connection status of a specific device for a given vehicle ID.
     *
     * @param vehicleId        The vehicle ID.
     * @param targetDeviceId   The target device ID.
     * @param connectionStatus The new connection status of the device.
     */
    public void update(String vehicleId, String targetDeviceId, String connectionStatus, Optional<String> subService);

}
