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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Properties;

/**
 * A generic object state store that uses Kryo to serialize and
 * deserialize arbitrary entities. The default serializer is
 * CompatibleFieldSerializer thereby allowing changes to java classes.
 *
 * @author ssasidharan
 */
public class ObjectStateStore extends HarmanPersistentKVStore<String, Object> {
    
    /** The Constant KRYO_REPO. */
    private static final ThreadLocal<Kryo> KRYO_REPO = ThreadLocal.withInitial(() -> {
        Kryo k = new Kryo();
        k.setDefaultSerializer(CompatibleFieldSerializer.class);
        k.register(DateTime.class, new JodaDateTimeSerializer());
        k.setCopyReferences(false);
        return k;
    });

    /**
     * ObjectStateStore.
     *
     * @param name name
     * @param changeLoggingEnabled changeLoggingEnabled
     * @param props props
     */
    public ObjectStateStore(String name, boolean changeLoggingEnabled, Properties props) {
        super(name, changeLoggingEnabled, Serdes.String(), Serdes.serdeFrom(new Serializer<Object>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // overridden method
            }

            @Override
            public byte[] serialize(String topic, Object data) {
                Kryo kryo = KRYO_REPO.get();
                ByteArrayOutputStream baos = new ByteArrayOutputStream(1 * Constants.BYTE_1024 * Constants.BYTE_1024);
                Output output = new Output(baos);
                kryo.writeClassAndObject(output, data);
                output.close();
                byte[] bytes = baos.toByteArray();
                return bytes.length == 1 ? null : bytes;
            }

            @Override
            public void close() {
                // overridden method
            }

        }, new Deserializer<Object>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // overridden method
            }

            @Override
            public Object deserialize(String topic, byte[] data) {
                if (data != null) {
                    Input input = new Input(new ByteArrayInputStream(data));
                    Object o = KRYO_REPO.get().readClassAndObject(input);
                    input.close();
                    return o;
                }
                return null;
            }

            @Override
            public void close() {
                // overridden method
            }
        }), props);
    }

    /**
     * ObjectStateStore.
     *
     * @param name name
     * @param changeLoggingEnabled changeLoggingEnabled
     */
    public ObjectStateStore(String name, boolean changeLoggingEnabled) {
        super(name, changeLoggingEnabled, Serdes.String(), Serdes.serdeFrom(new Serializer<Object>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // overridden method
            }

            @Override
            public byte[] serialize(String topic, Object data) {
                Kryo kryo = KRYO_REPO.get();
                ByteArrayOutputStream baos = new ByteArrayOutputStream(1 * Constants.BYTE_1024 * Constants.BYTE_1024);
                Output output = new Output(baos);
                kryo.writeClassAndObject(output, data);
                output.close();
                byte[] bytes = baos.toByteArray();
                return bytes.length == 1 ? null : bytes;
            }

            @Override
            public void close() {
                // overridden method
            }

        }, new Deserializer<Object>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // overridden method
            }

            @Override
            public Object deserialize(String topic, byte[] data) {
                if (data != null) {
                    Input input = new Input(new ByteArrayInputStream(data));
                    Object o = KRYO_REPO.get().readClassAndObject(input);
                    input.close();
                    return o;
                }
                return null;
            }

            @Override
            public void close() {
                // overridden method
            }
        }), new Properties());
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        KRYO_REPO.remove();
        super.close();
    }
}
