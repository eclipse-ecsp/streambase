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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * It is TreeMap based message sequencing.
 */
public class SequenceBufferTreeMapImpl implements SequenceBuffer {
    
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(SequenceBufferTreeMapImpl.class);
    
    /** The data. */
    private TreeMap<Long, List<String>> data = new TreeMap<>();
    
    /** The last flush timestamp. */
    private long lastFlushTimestamp;

    /**
     * Initializes the SequenceBuffer.
     *
     * @param sequenceBuffer the sequence buffer
     */
    @Override
    public void init(SequenceBuffer sequenceBuffer) {
        if (sequenceBuffer != null) {
            // At the time of initialization, It picks the data
            // from State-Store and stores into In-memory Cached.
            this.data.putAll(sequenceBuffer.getAll());
            this.lastFlushTimestamp = sequenceBuffer.getLastFlushTimestamp();
        }
    }  
    
    
    /**
     * Returns {@link TreeMap#headMap(Object)}.
     *
     * @param flushTime the flush time
     * @return the sorted map
     */
    @Override
    public SortedMap<Long, List<String>> head(long flushTime) {
        SortedMap<Long, List<String>> headmap = new TreeMap<>();
        SortedMap<Long, List<String>> existingData = data.headMap(flushTime, true);
        for (Entry<Long, List<String>> entry : existingData.entrySet()) {
            List<String> dataKeys = new ArrayList<>();
            for (String dataKey : entry.getValue()) {
                dataKeys.add(dataKey);
            }
            headmap.put(entry.getKey(), dataKeys);
        }
        return headmap;
    }

    /**
     * Head.
     *
     * @return the string
     */
    @Override
    public String head() {
        String headEventKeyInStore = null;
        Entry<Long, List<String>> lastEntry = data.ceilingEntry(System.currentTimeMillis());
        if (lastEntry != null && !lastEntry.getValue().isEmpty()) {
            // Get the IgniteEvent from the StateStore
            headEventKeyInStore = lastEntry.getValue().get(0);
        }
        return headEventKeyInStore;
    }

    /**
     * Gets the all.
     *
     * @return the all
     */
    @Override
    public SortedMap getAll() {
        return data;
    }

    /**
     * Gets the last flush timestamp.
     *
     * @return the last flush timestamp
     */
    @Override
    public long getLastFlushTimestamp() {
        return lastFlushTimestamp;
    }

    /**
     * Sets the last flush timestamp.
     *
     * @param lastFlushTimestamp the new last flush timestamp
     */
    @Override
    public void setLastFlushTimestamp(long lastFlushTimestamp) {
        this.lastFlushTimestamp = lastFlushTimestamp;
    }

    /**
     * Adds the.
     *
     * @param pairId the pair id
     * @param pair the pair
     */
    @Override
    public void add(String pairId, Pair<IgniteKey<?>, IgniteEvent> pair) {
        logger.trace("Store entry with key {} in state store", pairId);

        // Add to state store
        long bucketKey = pair.getB().getTimestamp();
        data.computeIfAbsent(bucketKey, k -> new ArrayList<>());
        List<String> eventsOccurAtTime = data.get(bucketKey);
        eventsOccurAtTime.add(pairId);
        logger.trace("Added key {} to sequence buffer entry {} with size {}", pairId, bucketKey,
                eventsOccurAtTime.size());
    }

    /**
     * Removes the sequence.
     *
     * @param sequenceKey the sequence key
     */
    @Override
    public void removeSequence(Long sequenceKey) {
        logger.trace("Removing key from sequence buffer: {}", sequenceKey);
        // Remove the all time based keys from Cache
        data.remove(sequenceKey);
    }

    /**
     * Removes the.
     *
     * @param sequenceKey the sequence key
     * @param dataKey the data key
     */
    @Override
    public void remove(Long sequenceKey, String dataKey) {
        List<String> allDatakeys = data.get(sequenceKey);
        if (allDatakeys != null) {
            logger.trace("Removing data key {} from sequence key: {}", dataKey, sequenceKey);
            allDatakeys.remove(dataKey);
        }
    }
}
