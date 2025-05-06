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

package org.eclipse.ecsp.analytics.stream.base;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.ecsp.analytics.stream.base.stores.MapObjectStateStore;
import org.eclipse.ecsp.analytics.stream.threadlocal.ContextKey;
import org.eclipse.ecsp.analytics.stream.threadlocal.TaskContextHandler;
import org.eclipse.ecsp.domain.IgniteEventSource;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.transform.IgniteKeyTransformer;
import org.eclipse.ecsp.transform.Transformer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Abstraction for the Kafka Streams based processing context.
 *
 * @author ssasidharan
 * @param <K> the key type
 * @param <V> the value type
 */
public class KafkaStreamsProcessorContext<K, V> implements StreamProcessingContext<K, V> {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaStreamsProcessorContext.class);
    
    /** The context. */
    private ProcessorContext<K, V> context;
    
    /** The kafka producer. */
    private KafkaProducer<byte[], byte[]> kafkaProducer = null;
    
    /** The config. */
    private Properties config = null;
    
    /** The {@link MetricRegistry}. */
    private MetricRegistry metricRegistry = null;

    /** The task id. Storing the taskID from the processor context-->topicGroupID_partitionID */
    private String taskId;
    
    /** The {@link TaskContextHandler}. This is the first processor in chain of processors. */
    private TaskContextHandler handler;
    
    /** The last processor in chain. */
    private boolean lastProcessorInChain;
    
    /** The {@link Transformer} implementation to transform the Kafka Record. */
    private Transformer igniteTransformer;
    
    /** The key transformer implementation to transform the key part. */
    private IgniteKeyTransformer<String> keyTransformer;

    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /**
     * Enum for the state-stores that are supported by this library.
     */
    public enum StoreType {
        
        /** The rocksdb. */
        ROCKSDB("rocksdb") {
            @Override
            public KeyValueStore getStore(ProcessorContext context, String name) {
                return (KeyValueStore) context.getStateStore(name);
            }
        },
        
        /** The map. */
        MAP("map") {
            @Override
            public KeyValueStore getStore(ProcessorContext context, String name) {
                return new MapObjectStateStore();
            }
        };

        /** The store type. */
        private String storeType;

        /**
         * Instantiates a new store type.
         *
         * @param type the type
         */
        StoreType(String type) {
            this.storeType = type;
        }

        /**
         * Gets the store type.
         *
         * @return the store type
         */
        public String getStoreType() {
            return this.storeType;
        }

        /**
         * Whether the current type is supported or not.
         *
         * @param type the type
         * @return Whether the given type is supported or not.
         */
        public static boolean isSupported(String type) {
            for (StoreType storeType : StoreType.values()) {
                if (storeType.getStoreType().equalsIgnoreCase(type)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Gets the store.
         *
         * @param <K> the key type
         * @param <V> the value type
         * @param context the context
         * @param name the name
         * @return the store
         */
        public abstract <K, V> KeyValueStore<K, V> getStore(ProcessorContext<K, V> context, String name);
        
        /**
         * Returns the instance of KeyValueStore based on the store type.

         * @param <K> Key type in {@link ProcessorContext}
         * @param <V> Value type in {@link ProcessorContext}
         * @param context {@link ProcessorContext} instance
         * @param name The name of the state-store.
         * @param storeType The type of the store
         * @return the instance of KeyValueStore based on the store type.
         */
        public static <K, V> KeyValueStore<K, V> getStateStore(ProcessorContext<K, V> context, 
                String name, String storeType) {
            if (isSupported(storeType)) {
                if (storeType.equals(ROCKSDB.getStoreType())) {
                    return ROCKSDB.getStore(context, name);
                } else if (storeType.equals(MAP.getStoreType())) {
                    return MAP.getStore(context, name);
                }
            }
            return null;
        }
    }
    
    /**
     * Initializes the context for KafkaStreams. This also initializes the list of transformers provided 
     * to transform the kafka records. For different customers / platforms, different transformers can be 
     * provided and based on the source of kafka record, specific transformer will be used to transform the record.
     *
     * @param pc {@link ProcessorContext} instance.
     * @param config Configurations needed to initialize the {@link KafkaProducer}
     * @param metricRegistry {@link MetricRegistry}
     * @param lastProcessorInChain Whether this is the last processor in chain.
     * @param ctx2 {@link ApplicationContext}
     */
    public KafkaStreamsProcessorContext(ProcessorContext<K, V> pc, Properties config, MetricRegistry metricRegistry,
            boolean lastProcessorInChain, ApplicationContext ctx2) {
        this.context = pc;
        this.config = config;
        this.metricRegistry = metricRegistry;
        this.taskId = pc.taskId().toString();
        this.lastProcessorInChain = lastProcessorInChain;
        this.ctx = ctx2;
        kafkaProducer = KafkaProducerInstance.getProducerInstance(config);
        handler = TaskContextHandler.getTaskContextHandler();
        String transformerList = config.getProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES);
        // Flag which will ensure that the instances of transformers
        // created via runtime class loader( Non Spring Based Bean
        // creation) will have the properties available to it via the
        // parameterized constructor.
        boolean transformerInjectPropertyFlg = Boolean.parseBoolean(config
                .getProperty(PropertyNames.TRANSFORMER_INJECT_PROPERTY_ENABLE));

        if (StringUtils.isBlank(transformerList)) {
            logger.error("Event transformer list cannot be blank");
            throw new IllegalArgumentException("Event transformer list cannot be blank");
        }
        String[] transformerArr = transformerList.split(",");
        for (String transformer : transformerArr) {
            Transformer t = null;
            if (transformerInjectPropertyFlg) {
                try {
                    logger.info("Loading parameterized constructor for transformer :{}", transformer);
                    t = (Transformer) ctx.getAutowireCapableBeanFactory().getBean(transformer, config);
                } catch (Exception ex) {
                    throw new IllegalArgumentException(
                            transformer + " parameterized constructor is not available to accept Properties.");
                }
            } else {
                t = (Transformer) ctx.getAutowireCapableBeanFactory().getBean(transformer);
            }
            if (IgniteEventSource.IGNITE.equals(t.getSource())) {
                igniteTransformer = t;
                break;
            }

        }
        Objects.requireNonNull(igniteTransformer);
        String transformer = config.getProperty(PropertyNames.IGNITE_KEY_TRANSFORMER);
        try {
            keyTransformer = (IgniteKeyTransformer<String>) getClass().getClassLoader().loadClass(transformer)
                    .getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException 
                | ClassNotFoundException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    String.format("%s refers to a class that is not available on the classpath", keyTransformer));
        }
        Objects.requireNonNull(keyTransformer);
    }

    /**
     * The name of the stream / Kafka topic .
     *
     * @return the string
     */
    @Override
    public String streamName() {
        Optional<RecordMetadata> recordMetadata = context.recordMetadata();
        if (recordMetadata.isPresent()) {
            return recordMetadata.get().topic();
        }
        return null;
    }

    /**
     * The partition ID.
     *
     * @return the partition ID.
     */
    @Override
    public int partition() {
        Optional<RecordMetadata> recordMetadata = context.recordMetadata();
        if (recordMetadata.isPresent()) {
            return recordMetadata.get().partition();
        }
        return 0;
    }

    /**
     * Offset of the Kafka Record.
     *
     * @return the Offset for the Kafka Record.
     */
    @Override
    public long offset() {
        Optional<RecordMetadata> recordMetadata = context.recordMetadata();
        if (recordMetadata.isPresent()) {
            return recordMetadata.get().offset();
        }
        return 0;
    }

    /**
     * Checkpoint.
     */
    @Override
    public void checkpoint() {
        context.commit();
    }

    /**
     * Forwards a record to all child processors. 
     *
     * @param kafkaRecord the kafka record
     * 
     * @see ProcessorContext#forward(Record)
     */
    @Override
    public void forward(Record<K, V> kafkaRecord) {
        kafkaRecord.withTimestamp(System.currentTimeMillis());
        context.forward(kafkaRecord);
    }

    /**
     * Forwards a record to the specified child processor.
     *
     * @param kafkaRecord the kafka record
     * @param name name of the child processor.
     */
    @Override
    public void forward(Record<K, V> kafkaRecord, String name) {
        if (lastProcessorInChain) {
            kafkaRecord.withTimestamp(System.currentTimeMillis());
            context.forward(kafkaRecord, name);
        } else {
            handler.setValue(taskId, ContextKey.KAFKA_SINK_TOPIC, name);
            kafkaRecord.withTimestamp(System.currentTimeMillis());
            context.forward(kafkaRecord);
        }
    }

    /**
     * Forward directly to a Kafka topic.
     *
     * @param key the key
     * @param value the value
     * @param topic the topic
     */
    @Override
    public void forwardDirectly(String key, String value, String topic) {
        forwardDirectly(key.getBytes(), value.getBytes(), topic);
    }

    /**
     * Forwards directly to a kafka topic.
     *
     * @param key the key
     * @param value the value
     * @param topic the topic
     */
    @Override
    public void forwardDirectly(@SuppressWarnings("rawtypes") IgniteKey key, IgniteEvent value, String topic) {
        if (!(key instanceof IgniteStringKey)) {
            throw new IllegalArgumentException(String
                    .format("Key %s is not currently supported", key.getClass().getName()));
        }
        @SuppressWarnings("unchecked")
        byte[] keyBytes = keyTransformer.toBlob(key);
        byte[] valueBytes = igniteTransformer.toBlob(value);
        forwardDirectly(keyBytes, valueBytes, topic);
    }

    /**
     * Forward directly to a Kafka topic.
     *
     * @param key the key
     * @param value the value
     * @param topic the topic
     */
    private void forwardDirectly(byte[] key, byte[] value, String topic) {
        kafkaProducer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                logger.error("Exception when pushing message directly to stream: ", exception);
            }
        });
    }

    /**
     * Check what is the value of the property "state.store.type" if the
     * state store type is map, then return the HashMapStateStore else go as
     * usual through context.
     * Pls note that hash map state store will nnot be initialized during
     * the topology builder. It will be initialized first time when you call
     * this method
     *
     * @param name the name
     * @return the state store
     */
    @Override
    public KeyValueStore getStateStore(String name) {
        String storeType = this.config.getProperty(PropertyNames.STATE_STORE_TYPE, StoreType.ROCKSDB.getStoreType());
        return StoreType.getStateStore(context, name, storeType);
    }

    /**
     * Gets the metric registry.
     *
     * @return the metric registry
     */
    @Override
    public MetricRegistry getMetricRegistry() {
        return this.metricRegistry;
    }

    /**
     * Gets the task ID.
     *
     * @return the task ID
     */
    @Override
    public String getTaskID() {
        return this.taskId;
    }

    /**
     * Schedule.
     *
     * @param interval the interval
     * @param punctuationType the punctuation type
     * @param punctuator the punctuator
     */
    @Override
    public void schedule(long interval, PunctuationType punctuationType, Punctuator punctuator) {
        // RTC-141484 - Kafka version upgrade from 1.0.0. to 2.2.0 changes
        context.schedule(Duration.ofMillis(interval), punctuationType, punctuator);
    }
}
