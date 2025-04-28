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
import org.eclipse.ecsp.stream.dma.dao.key.RetryBucketKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;
import java.util.Comparator;
import java.util.Optional;


/**
 * This class is responsible for string the messageIds to be retired at a
 * particult timestamp. Here key timestamp is of type long and value
 * is RetryMessageIds is wrapper around a set of strings (messageIds).
 */
@Repository
@Scope("prototype")
public class DMARetryBucketDAOCacheBackedInMemoryImpl extends CachedSortedMapStateStore<RetryBucketKey, RetryRecordIds>
        implements DmaRetryBucketDao {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DMARetryBucketDAOCacheBackedInMemoryImpl.class);

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
     * Update messageId in the set of strings that needs to be retried corresponding to timestamp.
     *
     * @param mapKey the map key
     * @param key the key
     * @param messageId the message id
     */
    @Override
    public void update(String mapKey, RetryBucketKey key, String messageId) {
        putToMapIfAbsent(mapKey, key, new RetryRecordIds(Version.V1_0), Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        RetryRecordIds retryMessageIds = get(key);
        if (retryMessageIds == null) {
            retryMessageIds = new RetryRecordIds(Version.V1_0);
        }
        logger.info("Got retryRecordMessageIds: {} for key: {}", retryMessageIds, key.convertToString());
        retryMessageIds.addRecordId(messageId);
        putToMap(mapKey, key, retryMessageIds, Optional.empty(), InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        logger.debug("Updated messageId {} in RetryBucketDAO for key {}", messageId, key.getTimestamp());
    }

    /**
     * Delete messageId from the set of messageIds to be retried for a timestamp.
     *
     * @param mapKey the map key
     * @param key the key
     * @param messageId the message id
     */
    @Override
    public void deleteMessageId(String mapKey, RetryBucketKey key, String messageId) {
        RetryRecordIds retryMessageIds = get(key);
        if (retryMessageIds.deleteRecordId(messageId)) {
            logger.debug("Deleted messageId {} in RetryBucketDAO for key {}", messageId, key);
            putToMap(mapKey, key, retryMessageIds, Optional.empty(), InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        } else {
            logger.error("messageId {} is not present in RetryBucketDAO for key {}. Delete op cannot be performed.",
                    messageId, key.getTimestamp());
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
        regexBuilder.append(DMAConstants.RETRY_BUCKET).append(DMAConstants.COLON)
                .append(serviceName).append(DMAConstants.COLON).append(taskId);
        RetryBucketKey converter = new RetryBucketKey();
        //passing the taskId to CacheSortedMapStore, which in turn will pass it to setup of CacheBypass.
        setTaskId(taskId);
        syncWithMapCache(regexBuilder.toString(), converter, InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        long endTime = System.currentTimeMillis();
        logger.info("Time taken to Initialize RetryBucketDAO for taskId {} is {} seconds", taskId,
                (endTime - currentTime) / Constants.THOUSAND);
    }

    /**
     * Setup.
     */
    @PostConstruct
    public void setup() {
        Comparator<RetryBucketKey> comparator = Comparator.comparingLong(RetryBucketKey::getTimestamp);
        createStoreWithComparator(comparator);
    }

}