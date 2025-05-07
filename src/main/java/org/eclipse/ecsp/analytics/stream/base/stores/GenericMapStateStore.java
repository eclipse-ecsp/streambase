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
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GenericMapStateStore uses ConcurrentHashMap for a thread-safe storage of key value pair in memory.
 *
 * @author avadakkootko
 * @param <K> the key type
 * @param <V> the value type
 */
@Repository
@Scope("prototype")
public class GenericMapStateStore<K, V> extends GenericMapStateStoreBase<K, V, ConcurrentHashMap<K, V>> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(GenericMapStateStore.class);
    
    /** The Constant STORE_NAME. */
    private static final String STORE_NAME = "GenericMapStateStoreName";

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
     * Creates the empty store.
     *
     * @return the concurrent hash map
     */
    @Override
    protected ConcurrentHashMap<K, V> createEmptyStore() {
        return new ConcurrentHashMap<>();
    }

    /**
     * Creates the iterator.
     *
     * @param mapStore2 the map store 2
     * @return the key value iterator
     */
    @Override
    protected KeyValueIterator<K, V> createIterator(ConcurrentHashMap<K, V> mapStore2) {
        return new ConcurrentHashMapIterator<>(mapStore2);
    }

    /**
     * The Class ConcurrentHashMapIterator.
     *
     * @param <S> the generic type
     * @param <U> the generic type
     */
    private class ConcurrentHashMapIterator<S, U> implements KeyValueIterator<S, U> {

        /** The concurrent map. */
        private ConcurrentHashMap<S, U> concurrentMap;
        
        /** The key iter. */
        private Iterator<S> keyIter;

        /**
         * Instantiates a new concurrent hash map iterator.
         *
         * @param map the map
         */
        public ConcurrentHashMapIterator(Map<S, U> map) {
            /*
             * Since we have to iterate over the original map, make a deep copy
             * of this map.
             */
            logger.trace("Initializing iterator for map state store.");
            this.concurrentMap = new ConcurrentHashMap<>();
            this.concurrentMap.putAll(map);
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
        public KeyValue<S, U> next() {
            if (hasNext()) {
                S key = this.keyIter.next();
                U val = this.concurrentMap.get(key);
                return new KeyValue<>(key, val);
            }
            return null;
        }

        /**
         * Close.
         */
        @Override
        public void close() {
            if (concurrentMap != null) {
                concurrentMap.clear();
            }

        }

        /**
         * Peek next key.
         *
         * @return the s
         */
        @Override
        public S peekNextKey() {
            throw new UnsupportedOperationException("Method peekNextKey not supported in KeyValueMapIterator");
        }

    }

}
