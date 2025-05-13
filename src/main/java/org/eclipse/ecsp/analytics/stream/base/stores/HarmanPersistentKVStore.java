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

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * class HarmanPersistentKVStore implements KeyValueStore.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class HarmanPersistentKVStore<K, V> implements KeyValueStore<K, V> {

    /** The log. */
    private static IgniteLogger log = IgniteLoggerFactory.getLogger(HarmanPersistentKVStore.class);
    
    /** The proxied. */
    private final KeyValueStore<Bytes, byte[]> proxied;
    
    /** The key serde. */
    private final Serde<K> keySerde;
    
    /** The value serde. */
    private final Serde<V> valueSerde;
    
    /** The serdes. */
    private StateSerdes<K, V> serdes;

    /**
     * HarmanPersistentKVStore.
     *
     * @param name name
     * @param changeLoggingEnabled changeLoggingEnabled
     * @param keySerde keySerde
     * @param valueSerde valueSerde
     * @param props props
     */
    protected HarmanPersistentKVStore(String name, boolean changeLoggingEnabled, final Serde<K> keySerde,
            final Serde<V> valueSerde, Properties props) {
        if (changeLoggingEnabled) {
            Map<String, String> changelogConfig = new HashMap<>();
            changelogConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");

            StoreBuilder<KeyValueStore<Bytes, byte[]>> changeLogBuilder = Stores.keyValueStoreBuilder(
                            new HarmanRocksDBStoreSupplier(name, props),
                            Serdes.Bytes(),
                            Serdes.ByteArray())
                    .withLoggingEnabled(changelogConfig);
            proxied = changeLogBuilder.build();
            log.info("Created instance of HarmanRocksDBStore with change log enabled");
        } else {
            proxied = new HarmanRocksDBStore<>(name,
                    Serdes.Bytes(),
                    Serdes.ByteArray(), props);
            log.warn("Created instance of HarmanRocksDBStore with "
                    + "change log disabled. State store is not fault-tolerant!");
        }
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Put.
     *
     * @param key the key
     * @param value the value
     */
    @Override
    public void put(final K key, final V value) {
        final Bytes bytesKey = Bytes.wrap(serdes.rawKey(key));
        final byte[] bytesValue = serdes.rawValue(value);
        proxied.put(bytesKey, bytesValue);
    }

    /**
     * Put if absent.
     *
     * @param key the key
     * @param value the value
     * @return the v
     */
    @Override
    public V putIfAbsent(final K key, final V value) {
        final V v = get(key);
        if (v == null) {
            put(key, value);
        }
        return v;
    }

    /**
     * Put all.
     *
     * @param entries the entries
     */
    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        final List<KeyValue<Bytes, byte[]>> keyValues = new ArrayList<>();
        for (final KeyValue<K, V> entry : entries) {
            keyValues.add(KeyValue.pair(Bytes.wrap(serdes.rawKey(entry.key)), serdes.rawValue(entry.value)));
        }
        proxied.putAll(keyValues);
    }

    /**
     * Delete.
     *
     * @param key the key
     * @return the v
     */
    @Override
    public V delete(final K key) {
        final byte[] oldValue = proxied.delete(Bytes.wrap(serdes.rawKey(key)));
        if (oldValue == null) {
            return null;
        }
        return serdes.valueFrom(oldValue);
    }

    /**
     * Gets the.
     *
     * @param key the key
     * @return the v
     */
    @Override
    public V get(final K key) {
        final byte[] rawValue = proxied.get(Bytes.wrap(serdes.rawKey(key)));
        if (rawValue == null) {
            return null;
        }
        return serdes.valueFrom(rawValue);
    }

    /**
     * Range.
     *
     * @param from the from
     * @param to the to
     * @return the key value iterator
     */
    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return new SerializedKVIterator<>(proxied.range(Bytes.wrap(serdes.rawKey(from)),
                Bytes.wrap(serdes.rawKey(to))),
                serdes);
    }

    /**
     * All.
     *
     * @return the key value iterator
     */
    @Override
    public KeyValueIterator<K, V> all() {
        return new SerializedKVIterator<>(proxied.all(), serdes);
    }

    /**
     * Approximate num entries.
     *
     * @return the long
     */
    @Override
    public long approximateNumEntries() {
        return proxied.approximateNumEntries();
    }
    
    /**
     * Initialize the HarmanPersistentKVStore.

     * @param context ProcessorContext instance
     * @param root StateStore instance
     * 
     * @deprecated since 2.41.0-1
     */
    @Deprecated(since = "2.41.0-1")
    @Override
    public void init(ProcessorContext context, StateStore root) {
        if (context instanceof StateStoreContext stateStoreContext) {
            init(stateStoreContext, root);
        } else {
            throw new UnsupportedOperationException(
                "Use RocksDBStore#init(StateStoreContext, StateStore) instead."
            );
        }
    }
    

    /**
     * Inits the.
     *
     * @param context the context
     * @param root the root
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(final StateStoreContext context, final StateStore root) {

        proxied.init(context, root);
        this.serdes = new StateSerdes<>(proxied.name(),
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return proxied.name();
    }

    /**
     * Flush.
     */
    @Override
    public void flush() {
        proxied.flush();
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        proxied.close();
    }

    /**
     * Persistent.
     *
     * @return true, if successful
     */
    @Override
    public boolean persistent() {
        return proxied.persistent();
    }

    /**
     * Checks if is open.
     *
     * @return true, if is open
     */
    @Override
    public boolean isOpen() {
        return proxied.isOpen();
    }
}
