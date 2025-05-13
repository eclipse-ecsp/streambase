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

package org.eclipse.ecsp.analytics.stream.base.stores;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.eclipse.ecsp.analytics.stream.base.utils.ObjectUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * This state store stores keys in sorted order and provides range operations for keys in an efficient manner.
 *
 * @author avadakkootko
 * @param <K> the key type
 * @param <V> the value type
 */
@Repository
@Scope("prototype")
public class GenericSortedMapStateStore<K, V> extends GenericMapStateStoreBase<K, V, ConcurrentSkipListMap<K, V>>
        implements SortedKeyValueStore<K, V> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(GenericSortedMapStateStore.class);

    /** The Constant STORE_NAME. */
    private static final String STORE_NAME = "GenericSortedMapStateStoreName";

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return STORE_NAME;
    }

    /**
     * Creates the store with comparator.
     *
     * @param comparator the comparator
     */
    public void createStoreWithComparator(Comparator<K> comparator) {
        mapStore = new ConcurrentSkipListMap<K, V>(comparator);
    }

    /**
     * Creates the empty store.
     *
     * @return the concurrent skip list map
     */
    @Override
    protected ConcurrentSkipListMap<K, V> createEmptyStore() {
        return new ConcurrentSkipListMap<>();
    }

    /**
     * Creates the iterator.
     *
     * @param mapStore2 the map store 2
     * @return the key value iterator
     */
    @Override
    protected KeyValueIterator<K, V> createIterator(ConcurrentSkipListMap<K, V> mapStore2) {
        return new ConcurrentSkipListMapIterator(mapStore2);
    }

    /**
     * Range.
     *
     * @param fromKey the from key
     * @param toKey the to key
     * @return the key value iterator
     */
    @Override
    public KeyValueIterator<K, V> range(K fromKey, K toKey) {
        ObjectUtils.requireNonNull(fromKey, "Received null fromKey.");
        ObjectUtils.requireNonNull(toKey, "Received null toKey.");
        return new ConcurrentSkipListMapIterator(
                 mapStore.subMap(fromKey, true, toKey,
                        true));
    }

    /**
     * All.
     *
     * @return the key value iterator
     */
    @Override
    public KeyValueIterator<K, V> all() {
        return new ConcurrentSkipListMapIterator(mapStore);
    }

    /**
     * Returns a view of the portion of this map whose keys are less than (or equal to) toKey.
     *
     * @param toKey toKey
     * @return KeyValueIterator
     */
    @Override
    public KeyValueIterator<K, V> getHead(K toKey) {
        ObjectUtils.requireNonNull(toKey, "Received null key.");
        return new ConcurrentSkipListMapIterator(mapStore.headMap(toKey, true));
    }

    /**
     * Returns a view of the portion of this map whose keys are greater than (or equal to) fromKey.
     *
     * @param fromKey fromKey
     * @return KeyValueIterator
     */
    @Override
    public KeyValueIterator<K, V> getTail(K fromKey) {
        ObjectUtils.requireNonNull(fromKey, "Received null key.");
        return new ConcurrentSkipListMapIterator(mapStore.tailMap(fromKey, true));
    }

    /**
     * The Class ConcurrentSkipListMapIterator.
     */
    private class ConcurrentSkipListMapIterator implements KeyValueIterator<K, V> {

        /** The sorted map. */
        private ConcurrentSkipListMap<K, V> sortedMap;
        
        /** The key iter. */
        private Iterator<K> keyIter;

        /**
         * Since we have to iterate over the original map, make a deep copy
         * of this map.
         *
         * @param map the map
         */
        public ConcurrentSkipListMapIterator(Map<K, V> map) {
            /*
             * Since we have to iterate over the original map, make a deep copy
             * of this map.
             */
            logger.trace("Initializing iterator for map state store.");
            this.sortedMap = new ConcurrentSkipListMap<>();
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
                return new KeyValue<>(key, val);
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
}