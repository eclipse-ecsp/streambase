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

import org.eclipse.ecsp.analytics.stream.base.stores.MutableKeyValueStore;
import org.eclipse.ecsp.analytics.stream.base.stores.SortedKeyValueStore;
import org.eclipse.ecsp.entities.dma.RetryRecordIds;
import org.eclipse.ecsp.stream.dma.dao.key.RetryBucketKey;


/**
 * interface DMARetryBucketDAO extends MutableKeyValueStore.
 */
public interface DmaRetryBucketDao  extends MutableKeyValueStore<RetryBucketKey, RetryRecordIds>,
        SortedKeyValueStore<RetryBucketKey, RetryRecordIds> {

    /**
     * Update.
     *
     * @param mapKey the map key
     * @param key the key
     * @param messageId the message id
     */
    public void update(String mapKey, RetryBucketKey key, String messageId);

    /**
     * Delete message id.
     *
     * @param mapKey the map key
     * @param key the key
     * @param messageId the message id
     */
    public void deleteMessageId(String mapKey, RetryBucketKey key, String messageId);

    /**
     * Initialize.
     *
     * @param taskId the task id
     */
    public void initialize(String taskId);

}