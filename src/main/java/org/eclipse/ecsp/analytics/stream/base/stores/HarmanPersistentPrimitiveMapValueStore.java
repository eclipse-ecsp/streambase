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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * class {@link HarmanPersistentPrimitiveMapValueStore}
 * extends {@link HarmanPersistentKVStore}.
 */
public class HarmanPersistentPrimitiveMapValueStore extends HarmanPersistentKVStore<String, Map<?, ?>> {
    
    /** The Constant OBJECT_MAPPER. */
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    /**
     * {@link HarmanPersistentPrimitiveMapValueStore}.
     *
     * @param name name
     * @param changeLoggingEnabled changeLoggingEnabled
     * @param properties properties
     */

    public HarmanPersistentPrimitiveMapValueStore(String name, boolean changeLoggingEnabled, Properties properties) {
        
        super(name, changeLoggingEnabled, Serdes.serdeFrom(String.class), Serdes.serdeFrom(new Serializer<Map<?, ?>>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                //method
            }

            @Override
            public byte[] serialize(String topic, Map data) {
                if (data == null) {
                    return new byte[0];
                }
                try {
                    return OBJECT_MAPPER.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new SerializationException(e);
                }
            }

            @Override
            public void close() {
                //method
            }

        }, new Deserializer<Map<?, ?>>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                //method
            }

            @Override
            public Map deserialize(String topic, byte[] data) {
                if (data == null) {
                    return Collections.emptyMap();
                }
                try {
                    return OBJECT_MAPPER.readValue(data, Map.class);
                } catch (IOException e) {
                    throw new SerializationException(e);
                }
            }

            @Override
            public void close() {
                //method
            }

        }), properties);
    }

    /**
     * HarmanPersistentPrimitiveMapValueStore.
     *
     * @param name name
     * @param changeLoggingEnabled changeLoggingEnabled
     */
    public HarmanPersistentPrimitiveMapValueStore(String name, boolean changeLoggingEnabled) {
        super(name, changeLoggingEnabled, Serdes.serdeFrom(String.class), Serdes.serdeFrom(new Serializer<Map<?, ?>>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                //method
            }

            @Override
            public byte[] serialize(String topic, Map data) {
                if (data == null) {
                    return new byte[0];
                }
                try {
                    return OBJECT_MAPPER.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new SerializationException(e);
                }
            }

            @Override
            public void close() {
                //method
            }

        }, new Deserializer<Map<?, ?>>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                //method
            }

            @Override
            public Map deserialize(String topic, byte[] data) {
                if (data == null) {
                    return Collections.emptyMap();
                }
                try {
                    return OBJECT_MAPPER.readValue(data, Map.class);
                } catch (IOException e) {
                    throw new SerializationException(e);
                }
            }

            @Override
            public void close() {
                // no additional operation to be performed at close
            }

        }), new Properties());
    }
}
