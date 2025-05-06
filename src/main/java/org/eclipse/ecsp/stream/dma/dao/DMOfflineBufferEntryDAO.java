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

import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.nosqldao.IgniteBaseDAO;

import java.util.List;
import java.util.Optional;


/**
 * DMOfflineBufferEntryDAO extends {@link IgniteBaseDAO}.
 */
public interface DMOfflineBufferEntryDAO extends IgniteBaseDAO<String, DMOfflineBufferEntry> {
    
    /**
     * Add pending ignite event for a service.
     *
     * @param deviceId the device id
     * @param igniteKey the ignite key
     * @param igniteEvent the ignite event
     * @param subService the sub service
     */
    public void addOfflineBufferEntry(String deviceId, IgniteKey<String> igniteKey,
            DeviceMessage igniteEvent, String subService);

    /**
     * Get cached events for a particular device and service sorted based on priority.
     *
     * @param vehicleId vehicleId
     * @param descSortOrder descSortOrder
     * @param deviceId deviceId
     * @param subService subService
     * @return the sorted list of cached events or an empty list if there are no events
     */
    public List<DMOfflineBufferEntry> getOfflineBufferEntriesSortedByPriority(String vehicleId, boolean descSortOrder,
            Optional<String> deviceId, Optional<String> subService);

    /**
     * Remove the given entry from offline buffer.
     *
     * @param offlineEntryId offlineEntryId
     */
    public void removeOfflineBufferEntry(String offlineEntryId);

    /**
     * get entry with earliest TTL from offline buffer.
     *
     * @return the offline buffer entry with earliest ttl
     */
    public DMOfflineBufferEntry getOfflineBufferEntryWithEarliestTtl();

    /**
     * get offline buffer entry records with expired TTL.
     *
     * @return the offline buffer entries with expired ttl
     */
    public List<DMOfflineBufferEntry> getOfflineBufferEntriesWithExpiredTtl();
}
