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

package org.eclipse.ecsp.analytics.stream.base;

import org.eclipse.ecsp.analytics.stream.base.utils.Pair;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;

import java.util.List;
import java.util.SortedMap;

/**
 * This interface defines the contract for data structure that implements message ordering.
 */
public interface SequenceBuffer {

    /**
     * Initialize sequence buffer by supplied data object.
     *
     * @param data data
     */
    void init(SequenceBuffer data);

    /**
     * remove list sequence keys from in-memory sequence buffer.
     *
     * @param sequenceKey sequenceKey
     */
    void removeSequence(Long sequenceKey);

    /**
     * remove the data from sequence buffer.
     *
     * @param sequenceKey
     *         sequence buffer key
     * @param dataKey
     *         data key which will be remove from the sequence buffer
     */
    void remove(Long sequenceKey, String dataKey);

    /**
     * returns the all eligible sequence which are ready to flush.
     *
     * @param flushTime flushTime
     * @return SortedMap
     */
    SortedMap<Long, List<String>> head(long flushTime);


    /**
     * It returns the head KeyValuePair's key id.
     *
     * @return String
     */

    String head();
    
    /**
     * add Pair (IgniteKey and IgniteValue) in state store.
     *
     * @param pairId pairId
     * @param pair pair
     */
    void add(String pairId, Pair<IgniteKey<?>, IgniteEvent> pair);

    /**
     * It returns the complete buffered sequence which will be used for storing it to state store.
     *
     * @return SortedMap
     */
    SortedMap<Long, List<String>> getAll();

    /**
     * Get last flush timestamp.
     *
     * @return long
     */
    long getLastFlushTimestamp();

    /**
     * Set last flush timestamp. It is required when.
     *
     * @param lastFlushTimestamp lastFlushTimestamp
     */
    void setLastFlushTimestamp(long lastFlushTimestamp);

}
