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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Properties;

/**
 * Singleton Kafka producer. Factory class that creates the {@link KafkaProducer} instance
 * with the supplied configurations.
 *
 * @author ssasidharan
 */
public class KafkaProducerInstance {
    
    /** The config holder. */
    private static Properties configHolder = null;
    
    /** The {@link KafkaProducer} instance. */
    private final KafkaProducer<byte[], byte[]> producer;

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaProducerInstance.class);

    /**
     * Instantiates a new kafka producer instance with the supplied config.
     *
     * @param config The Properties instance.
     */
    private KafkaProducerInstance(Properties config) {
        logger.debug("Inside KafkaProducerInstance constructor config {}", config);
        Properties props = new Properties();
        config.forEach(props::put);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getProperty(PropertyNames.KAFKA_MAX_REQUEST_SIZE));
        props.put(ProducerConfig.ACKS_CONFIG, config.getProperty(PropertyNames.KAFKA_ACKS_CONFIG));
        props.put(ProducerConfig.RETRIES_CONFIG, config.getProperty(PropertyNames.KAFKA_RETRIES_CONFIG));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getProperty(PropertyNames.KAFKA_BATCH_SIZE_CONFIG));
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.getProperty(PropertyNames.KAFKA_LINGER_MS_CONFIG));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                config.getProperty(PropertyNames.KAFKA_BUFFER_MEMORY_CONFIG));
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                config.getProperty(PropertyNames.KAFKA_REQUEST_TIMEOUT_MS_CONFIG));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                config.getProperty(PropertyNames.KAFKA_DELIVERY_TIMEOUT_MS_CONFIG));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                config.getProperty(PropertyNames.KAFKA_COMPRESSION_TYPE_CONFIG));
        producer = new KafkaProducer<>(props);
        logger.info("KafkaProducer instance created with config {}", props);
    }

    /**
     * Gets the producer instance.
     *
     * @param config the config
     * @return the {@link KafkaProducer} instance.
     */
    public static KafkaProducer<byte[], byte[]> getProducerInstance(Properties config) {
        configHolder = config;
        return KafkaProducerInstanceHolder.getInstance().producer;
    }

    /**
     * Holds the instance of KafkaProducer.
     */
    @ThreadSafe
    private static final class KafkaProducerInstanceHolder {
        
        /** The {@link KafkaProducerInstance} instance. */
        private static KafkaProducerInstance instance;
        
        /**
         * Instantiates a new kafka producer instance holder.
         */
        private KafkaProducerInstanceHolder() {}

        /**
         * Gets the single instance of KafkaProducerInstanceHolder.
         *
         * @return single instance of KafkaProducerInstanceHolder
         */
        public static synchronized KafkaProducerInstance getInstance() {
            if (instance != null) {
                return instance;
            }
            logger.info("Creating new Instance of KafkaProducerInstance");
            instance = new KafkaProducerInstance(configHolder);
            logger.info("New Instance of KafkaProducerInstance created successfully");
            return instance;
        }
    }

}
