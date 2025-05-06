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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.stores.CachedMapStateStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.entities.dma.RetryRecord;
import org.eclipse.ecsp.stream.dma.dao.key.RetryRecordKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Repository;


/**
 * DMARetryRecordDAOCacheBackedInMemoryImpl is extends CachedMapStateStore which is a
 * generic concurrent hash map that periodically backs up
 * data to cache (Redis).
 *
 * @author avadakkootko
 */
@Lazy
@Repository
public class DMARetryRecordDAOCacheBackedInMemoryImpl extends CachedMapStateStore<RetryRecordKey, RetryRecord>
        implements DMARetryRecordDAO {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DMARetryRecordDAOCacheBackedInMemoryImpl.class);
    
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
     * Initialize.
     *
     * @param taskId the task id
     */
    @Override
    public void initialize(String taskId) {
        StringBuilder regexBuilder = new StringBuilder();
        regexBuilder.append(DMAConstants.RETRY_MESSAGEID).append(DMAConstants.COLON)
                .append(serviceName).append(DMAConstants.COLON).append(taskId);
        //passing the taskId to CachedMapStore, which in turn will pass it to setup of CacheBypass.
        setTaskId(taskId);
        long currentTime = System.currentTimeMillis();
        syncWithMapCache(regexBuilder.toString(), new RetryRecordKey(), InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        long endTime = System.currentTimeMillis();
        logger.info("Time taken to Initialize RetryRecordDAO for taskId {} is {} seconds", taskId,
                (endTime - currentTime) / Constants.THOUSAND);
    }

}