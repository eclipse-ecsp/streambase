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

package org.eclipse.ecsp.stream.dma.handler;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * class {@link TestKVIterator}implements {@link KeyValueIterator}.
 *
 * @param <K> k
 * @param <V> v
 */
public class TestKVIterator<K, V> implements KeyValueIterator<K, V> {

    /** The sorted map. */
    private ConcurrentSkipListMap<K, V> sortedMap;
    
    /** The key iter. */
    private Iterator<K> keyIter;

    /**
     * Since we have to iterate over the original map, make a deep copy of
     * this map.
     *
     * @param map the map
     */
    public TestKVIterator(Map<K, V> map) {

        this.sortedMap = new ConcurrentSkipListMap<K, V>();
        this.sortedMap.putAll(map);
        keyIter = map.keySet().iterator();

    }

    /**
     * Checks for next.
     *
     * @return true, if successful
     */
    @Override
    public boolean hasNext() {
        return keyIter.hasNext();
    }

    /**
     * Next.
     *
     * @return the key value
     */
    @Override
    public KeyValue<K, V> next() {
        if (hasNext()) {
            K key = this.keyIter.next();
            V val = this.sortedMap.get(key);
            return new KeyValue<K, V>(key, val);
        }
        return null;
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        if (sortedMap != null) {
            sortedMap.clear();
        }

    }

    /**
     * Peek next key.
     *
     * @return the k
     */
    @Override
    public K peekNextKey() {
        throw new UnsupportedOperationException("Method peekNextKey not supported in KeyValueMapIterator");
    }

}