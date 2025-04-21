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
import org.eclipse.ecsp.utils.ConcurrentHashSet;

import java.util.Optional;


/**
 * {@link DeviceStatusService}.
 */
public interface DeviceStatusService {

    /**
     * Gets the.
     *
     * @param vehicleId the vehicle id
     * @param subService the sub service
     * @return the concurrent hash set
     */
    public ConcurrentHashSet<String> get(String vehicleId, Optional<String> subService);

    /**
     * Put.
     *
     * @param vehicleId the vehicle id
     * @param deviceIds the device ids
     * @param mutationId the mutation id
     * @param subService the sub service
     */
    public void put(String vehicleId, ConcurrentHashSet<String> deviceIds,
            Optional<MutationId> mutationId, Optional<String> subService);

    /**
     * Delete.
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     * @param mutationId the mutation id
     * @param subService the sub service
     */
    public void delete(String vehicleId, String deviceId, Optional<MutationId> mutationId, Optional<String> subService);

    /**
     * Delete key.
     *
     * @param vehicleId the vehicle id
     * @param mutationId the mutation id
     */
    public void deleteKey(String vehicleId, Optional<MutationId> mutationId);

    /**
     * Gets the offset metadata.
     *
     * @param serviceName the service name
     * @return the offset metadata
     */
    public Optional<OffsetMetadata> getOffsetMetadata(String serviceName);

    /**
     * Bypass in-memory key store and read from cache.
     *
     * @param vehicleId vehicleId
     * @param subService the sub service
     * @return ConcurrentHashSet
     */
    public ConcurrentHashSet<String> forceGet(String vehicleId, Optional<String> subService);

}
