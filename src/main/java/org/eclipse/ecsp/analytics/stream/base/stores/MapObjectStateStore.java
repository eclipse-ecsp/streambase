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
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.eclipse.ecsp.analytics.stream.base.utils.Constants.RECEIVED_NULL_KEY;

/**
 * InMemory hash map state store. This is for supporting CFMS code base as part of Ignite2.o
 */
@Component
@Scope("prototype")
public class MapObjectStateStore implements KeyValueStore<String, Object> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(MapObjectStateStore.class);
    
    /** The Constant STORE_NAME. */
    private static final String STORE_NAME = "MapStateStoreName";
    
    /** The map store. */
    private Map<String, Object> mapStore = new ConcurrentHashMap<>();

    /**
     * A constant dummy name, as this map store will never be called by
     * its name, as it is maintained by the application not kafka.
     *
     * @return name
     */
    @Override
    public String name() {
        return STORE_NAME;
    }

    /**
     * Inits the.
     *
     * @param context the context
     * @param root the root
     */
    @Override
    public void init(ProcessorContext context, StateStore root) {
        // This method will not be called
    }

    /**
     * Flush.
     */
    @Override
    public void flush() {
        logger.info("Clearing the hash map state store");
        mapStore.clear();
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        logger.error("Method close is not implemented by MapObjectStateStore class.");
    }

    /**
     * Persistent.
     *
     * @return true, if successful
     */
    @Override
    public boolean persistent() {
        return true;
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
     * @return the object
     */
    @Override
    public Object get(String key) {
        ObjectUtils.requireNonNull(key, RECEIVED_NULL_KEY);
        logger.debug("Retrieving the value for the key:{}", key);
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
    public KeyValueIterator<String, Object> range(String from, String to) {
        return null;
    }

    /**
     * All.
     *
     * @return the key value iterator
     */
    @Override
    public KeyValueIterator<String, Object> all() {
        return new MapIterator(this.mapStore);
    }

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
    public void put(String key, Object value) {
        ObjectUtils.requireNonNull(key, RECEIVED_NULL_KEY);
        ObjectUtils.requireNonNull(value, "Received null value.");
        logger.debug("Putting into map state store. key:{} and value:{}", key, value);
        mapStore.put(key, value);
    }

    /**
     * Put if absent.
     *
     * @param key the key
     * @param value the value
     * @return the object
     */
    @Override
    public Object putIfAbsent(String key, Object value) {
        ObjectUtils.requireNonNull(key, RECEIVED_NULL_KEY);
        ObjectUtils.requireNonNull(value, "Received null value.");
        // check if the key exist
        if (!this.mapStore.containsKey(key)) {
            logger.debug("Adding key:{} to the map state store as it is not present.", key);
            put(key, value);
            // return the incoming value as return value.
            return value;
        } else {
            logger.debug("key:{} will not be added to the map state store as it is already present.", key);
            return null;
        }
    }

    /**
     * Put all.
     *
     * @param entries the entries
     */
    @Override
    public void putAll(List<KeyValue<String, Object>> entries) {
        ObjectUtils.requireNonNull(entries, "Received null values.");
        for (KeyValue<String, Object> keyValue : entries) {
            String key = keyValue.key;
            Object val = keyValue.value;
            logger.debug("Putting key:{} and value:{} in the map state store.");
            put(key, val);
        }

    }

    /**
     * Delete.
     *
     * @param key the key
     * @return the object
     */
    @Override
    public Object delete(String key) {
        ObjectUtils.requireNonNull(mapStore, "Uninitialized state store.");
        Object obj = this.mapStore.get(key);
        if (null != obj) {
            logger.debug("Deleting key:{} from map state store.", key);
            this.mapStore.remove(key);
        }
        return obj;
    }

    /**
     * Iterator is not required to iterate over the map elements, but
     * in order to be compatible with other state stores, we are implementing
     * this as an iterator.
     */
    private class MapIterator implements KeyValueIterator<String, Object> {

        /** The key iter. */
        // map keys iterator
        Iterator<String> keyIter;
        
        /** The map. */
        private Map<String, Object> map;

        /**
         * Since we have to iterate over the original map, make a deep copy
         * of this map.
         *
         * @param map the map
         */
        public MapIterator(Map<String, Object> map) {
            /*
             * Since we have to iterate over the original map, make a deep copy
             * of this map.
             */
            logger.trace("Initializing iterator for map state store.");
            this.map = new HashMap<>();
            this.map.putAll(map);
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
        public KeyValue<String, Object> next() {

            if (hasNext()) {
                // Retrieve the key from the list
                String key = this.keyIter.next();
                // now retrieve the key from the map
                Object val = this.map.get(key);
                logger.debug("Sending key:{} and value:{}", key, val);
                // return the key and value
                return new KeyValue<>(key, val);
            }
            return null;
        }

        /**
         * Close.
         */
        @Override
        public void close() {
            logger.error("Close operation not supported for map iterator.");
        }

        /**
         * Peek next key.
         *
         * @return the string
         */
        @Override
        public String peekNextKey() {
            throw new UnsupportedOperationException("close operation not supported for map iterator.");
        }

    }

}
