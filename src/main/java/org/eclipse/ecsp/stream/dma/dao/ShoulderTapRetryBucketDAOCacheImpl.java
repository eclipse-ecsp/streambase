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
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.stores.CachedSortedMapStateStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.RetryRecordIds;
import org.eclipse.ecsp.stream.dma.dao.key.ShoulderTapRetryBucketKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import java.util.Comparator;
import java.util.Optional;


/**
 * class {@link ShoulderTapRetryBucketDAOCacheImpl}
 * extends {@link CachedSortedMapStateStore}
 * implements {@link ShoulderTapRetryBucketDAO}.
 */
@Repository
@Scope("prototype")
public class ShoulderTapRetryBucketDAOCacheImpl extends 
    CachedSortedMapStateStore<ShoulderTapRetryBucketKey, RetryRecordIds> implements ShoulderTapRetryBucketDAO {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(ShoulderTapRetryBucketDAOCacheImpl.class);
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /**
     * Sets the service name.
     *
     * @param serviceName the new service name
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Update vehicleId in the set of strings that needs to be retried corresponding to timestamp.
     *
     * @param mapKey the map key
     * @param key the key
     * @param vehicleId the vehicle id
     */
    @Override
    public void update(String mapKey, ShoulderTapRetryBucketKey key, String vehicleId) {
        putToMapIfAbsent(mapKey, key, new RetryRecordIds(Version.V1_0), Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        RetryRecordIds vehicleIds = get(key);
        vehicleIds.addRecordId(vehicleId);
        putToMap(mapKey, key, vehicleIds, Optional.empty(), 
                InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        logger.debug("Updated vehicleId {} in ShoulderTapRetryBucketDAO for key {}", vehicleId, key.getTimestamp());

    }

    /**
     * Delete vehicleId from the set of messageIds to be retried for a timestamp.
     *
     * @param mapKey the map key
     * @param key the key
     * @param vehicleId the vehicle id
     */
    @Override
    public void deleteVehicleId(String mapKey, ShoulderTapRetryBucketKey key, String vehicleId) {
        RetryRecordIds vehicleIds = get(key);
        if (vehicleIds.deleteRecordId(vehicleId)) {
            logger.debug("Deleted vehicleId {} in ShoulderTapRetryBucketDAO for key {}", vehicleId, key);
            putToMap(mapKey, key, vehicleIds, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        } else {
            logger.error("vehicleId {} is not present in ShoulderTapRetryBucketDAO for key {}. "
                    + "Delete op cannot be performed.", vehicleId, key.getTimestamp());
        }

    }

    /**
     * Initialize.
     *
     * @param taskId the task id
     */
    @Override
    public void initialize(String taskId) {
        final long currentTime = System.currentTimeMillis();
        StringBuilder regexBuilder = new StringBuilder();
        regexBuilder.append(DMAConstants.SHOULDER_TAP_RETRY_BUCKET).append(DMAConstants.COLON).append(serviceName)
                .append(DMAConstants.COLON).append(taskId);
        ShoulderTapRetryBucketKey converter = new ShoulderTapRetryBucketKey();
        //passing the taskId to CacheSortedMapStore, which in turn will pass it to setup of CacheBypass.
        setTaskId(taskId);
        syncWithMapCache(regexBuilder.toString(), converter,
                InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        long endTime = System.currentTimeMillis();
        logger.info("Time taken to Initialize ShoulderTapRetryBucketDAO for taskId {} is {} seconds", taskId,
                (endTime - currentTime) / Constants.THOUSAND);
    }

    /**
     * setup().
     */
    @PostConstruct
    public void setup() {
        Comparator<ShoulderTapRetryBucketKey> comparator =
                Comparator.comparingLong(ShoulderTapRetryBucketKey::getTimestamp);
        createStoreWithComparator(comparator);
    }

}
