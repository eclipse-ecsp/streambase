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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * class JsonStateStore extends HarmanPersistentKVStore.
 */
public class JsonStateStore extends HarmanPersistentKVStore<String, JsonNode> {

    /** The Constant OBJECT_MAPPER. */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * JsonStateStore Constructor.
     *
     * @param name name
     * @param changeLoggingEnabled changeLoggingEnabled
     * @param properties properties
     */
    public JsonStateStore(String name, boolean changeLoggingEnabled, Properties properties) {
        super(name, changeLoggingEnabled, Serdes.serdeFrom(String.class), Serdes.serdeFrom(new Serializer<JsonNode>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                //Overridden method
            }

            @Override
            public byte[] serialize(String topic, JsonNode data) {
                if (data == null) {
                    return new byte[0];
                }
                try {
                    return OBJECT_MAPPER.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new SerializationException(e);
                }
            }

        }, new Deserializer<JsonNode>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                //Overridden method
            }

            @Override
            public JsonNode deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                try {
                    return OBJECT_MAPPER.readTree(data);
                } catch (IOException e) {
                    throw new SerializationException(e);
                }
            }
        }), properties);
    }

    /**
     * JsonStateStore.
     *
     * @param name name
     * @param changeLoggingEnabled changeLoggingEnabled
     */
    public JsonStateStore(String name, boolean changeLoggingEnabled) {
        super(name, changeLoggingEnabled, Serdes.serdeFrom(String.class), Serdes.serdeFrom(new Serializer<JsonNode>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                //Overridden method
            }

            @Override
            public byte[] serialize(String topic, JsonNode data) {
                if (data == null) {
                    return new byte[0];
                }
                try {
                    return OBJECT_MAPPER.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new SerializationException(e);
                }
            }

        }, new Deserializer<JsonNode>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                //Overridden method
            }

            @Override
            public JsonNode deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                try {
                    return OBJECT_MAPPER.readTree(data);
                } catch (IOException e) {
                    throw new SerializationException(e);
                }
            }
        }), new Properties());
    }
}
