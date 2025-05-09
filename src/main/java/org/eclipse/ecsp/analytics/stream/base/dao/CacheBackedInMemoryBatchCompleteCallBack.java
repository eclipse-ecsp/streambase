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

package org.eclipse.ecsp.analytics.stream.base.dao;

import java.util.List;

/**
 * Interface to implement a callback after completion of processing of a batch of records in in-memory
 * cache.  
 **/
public interface CacheBackedInMemoryBatchCompleteCallBack {

    /**
     * Callback method invoked after the completion of processing a batch of records in the in-memory cache.
     *
     * @param processedRecords A list of objects representing the records that were processed in the batch.
     */
    public void batchCompleted(List<Object> processedRecords);

}
