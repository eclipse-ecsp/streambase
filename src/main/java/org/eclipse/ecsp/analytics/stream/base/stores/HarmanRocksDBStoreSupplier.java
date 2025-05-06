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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

/**
 * class HarmanRocksDBStoreSupplier implements KeyValueBytesStoreSupplier.
 */
public class HarmanRocksDBStoreSupplier implements KeyValueBytesStoreSupplier {

    /** The name. */
    private final String name;
    
    /** The state store config. */
    private final Properties stateStoreConfig;

    /**
     * Instantiates a new harman rocks DB store supplier.
     *
     * @param name the name
     */
    public HarmanRocksDBStoreSupplier(final String name) {
        this(name, new Properties());
    }

    /**
     * Instantiates a new harman rocks DB store supplier.
     *
     * @param name the name
     * @param stateStoreConfig the state store config
     */
    public HarmanRocksDBStoreSupplier(final String name, final Properties stateStoreConfig) {
        this.name = name;
        this.stateStoreConfig = stateStoreConfig;
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Gets the.
     *
     * @return the key value store
     */
    @Override
    public KeyValueStore<Bytes, byte[]> get() {

        return new HarmanRocksDBStore<>(name,
                Serdes.Bytes(),
                Serdes.ByteArray(), stateStoreConfig);
    }

    /**
     * Metrics scope.
     *
     * @return the string
     */
    @Override
    public String metricsScope() {
        return "rocksdb-state";
    }
}
