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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.ecsp.analytics.stream.base.utils.ObjectUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.List;
import java.util.Map;

import static org.eclipse.ecsp.analytics.stream.base.utils.Constants.RECEIVED_NULL_KEY;

/**
 * GenericMapStateStore uses ConcurrentHashMap for a thread-safe storage of key value pair in memory.
 *
 * @author avadakkootko
 * @param <K> the key type
 * @param <V> the value type
 * @param <M> the generic type
 */
public abstract class GenericMapStateStoreBase<K, V, M extends Map<K, V>> implements KeyValueStore<K, V> {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(GenericMapStateStoreBase.class);
    
    /** The map store. */
    protected M mapStore = createEmptyStore();

    /**
     * Creates the empty store.
     *
     * @return the m
     */
    protected abstract M createEmptyStore();

    /**
     * Inits the.
     *
     * @param context the context
     * @param root the root
     */
    @Override
    public void init(ProcessorContext context, StateStore root) {
        logger.info("GenericMapStateStore already initialized.");
    }

    /**
     * Flush.
     */
    @Override
    public void flush() {
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        mapStore.clear();
        logger.info("GenericMapStateStore Closed.");
    }

    /**
     * Persistent.
     *
     * @return true, if successful
     */
    @Override
    public boolean persistent() {
        return false;
    }

    /**
     * Checks if is open.
     *
     * @return true, if is open
     */
    @Override
    public boolean isOpen() {
        return true;
    }

    /**
     * Gets the.
     *
     * @param key the key
     * @return the v
     */
    @Override
    public V get(K key) {
        ObjectUtils.requireNonNull(key, RECEIVED_NULL_KEY);
        logger.trace("Retrieving the value for the key:{} from generic map state store", key);
        return mapStore.get(key);
    }

    /**
     * Range.
     *
     * @param from the from
     * @param to the to
     * @return the key value iterator
     */
    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        logger.error("Method range is not implemented by GenericMapStateStore class.");
        throw new UnsupportedOperationException("Method range is not implemented by GenericMapStateStore class.");
    }

    /**
     * All.
     *
     * @return the key value iterator
     */
    @Override
    public KeyValueIterator<K, V> all() {
        return createIterator(mapStore);
    }

    /**
     * Creates the iterator.
     *
     * @param mapStore2 the map store 2
     * @return the key value iterator
     */
    protected abstract KeyValueIterator<K, V> createIterator(M mapStore2);

    /**
     * Approximate num entries.
     *
     * @return the long
     */
    @Override
    public long approximateNumEntries() {
        return mapStore.size();
    }

    /**
     * Put.
     *
     * @param key the key
     * @param value the value
     */
    @Override
    public void put(K key, V value) {
        ObjectUtils.requireNonNull(key, RECEIVED_NULL_KEY);
        ObjectUtils.requireNonNull(value, "Received null value.");
        logger.debug("Putting into generic map state store. key:{} and value:{}", key, value);
        mapStore.put(key, value);
    }

    /**
     * Put if absent.
     *
     * @param key the key
     * @param value the value
     * @return the v
     */
    @Override
    public V putIfAbsent(K key, V value) {
        ObjectUtils.requireNonNull(key, RECEIVED_NULL_KEY);
        ObjectUtils.requireNonNull(value, "Received null value.");
        // check if the key exist
        return mapStore.putIfAbsent(key, value);
    }

    /**
     * Put all.
     *
     * @param entries the entries
     */
    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        ObjectUtils.requireNonNull(entries, "Received null values.");
        for (KeyValue<K, V> keyValue : entries) {
            K key = keyValue.key;
            V val = keyValue.value;
            // Null check will be done inside put.
            put(key, val);
        }
    }

    /**
     * Delete.
     *
     * @param key the key
     * @return the v
     */
    @Override
    public V delete(K key) {
        V obj = this.mapStore.get(key);
        if (null != obj) {
            logger.debug("Deleting key:{} from map state store.", key);
            this.mapStore.remove(key);
        }
        return obj;
    }
}
