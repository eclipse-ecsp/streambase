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

package org.eclipse.ecsp.analytics.stream.base.dao.impl;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.dao.SinkNode;
import org.eclipse.ecsp.analytics.stream.base.exception.ClassNotFoundException;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaSslUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Implementation for Kafka topic as sink node {@link SinkNode}. 
 */
public class KafkaSinkNode implements SinkNode<byte[], byte[]> {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaSinkNode.class);
    
    /** The replace classloader. */
    private boolean replaceClassloader = false;
    
    /** The is sync put. */
    private boolean isSyncPut;
    
    /** The kafka partitioner class name. */
    private String kafkaPartitionerClassName = null;
    
    /** The {@link KafkaProducer} instance.*/
    private KafkaProducer<byte[], byte[]> producer = null;

    /**
     * Initializes the KafkaProducer with certain configuration like Kafka Partitioner, Kafka Replace Classloader
     * and SSL (if enabled).
     *
     * @param props the supplied configuration. 
     */
    @Override
    public void init(Properties props) {
        kafkaPartitionerClassName = props.getProperty(PropertyNames.KAFKA_PARTITIONER);
        replaceClassloader = Boolean.parseBoolean(props.getProperty(PropertyNames.KAFKA_REPLACE_CLASSLOADER));
        isSyncPut = Boolean.parseBoolean(props.getProperty(PropertyNames.KAFKA_DEVICE_EVENTS_ASYNC_PUTS));
        KafkaSslUtils.checkAndApplySslProperties(props);
        initKafkaProducer(props);
    }

    /**
     * Put into the specified Kafka topic.
     *
     * @param key the key
     * @param messageInBytes the message in bytes
     * @param kafkaTopic the kafka topic name
     * @param primaryKeyMapping the primary key mapping
     */
    @Override
    public void put(byte[] key, byte[] messageInBytes, String kafkaTopic, String primaryKeyMapping) {
        String keyString = Arrays.toString(key);
        try {
            logger.debug("Sending message to kafka. Topic: {}, Message: {}, key: {}", kafkaTopic, keyString);
            java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> f = producer
                    .send(new ProducerRecord<byte[], byte[]>(kafkaTopic, key, messageInBytes),                        
                        (metadata, exception) -> {
                            if (exception != null) {
                                logger.error("Exception occurred in KafkaProducerByPartition callback "
                                    + "for key : {}", keyString, exception);
                            }
                        });
            testIsSync(f, keyString);
        } catch (Exception e) {
            logger.error("Unable to send messages on Kafka for key : {} ", keyString, e);
        }
        logger.debug("Successfully sent message to kafka. Topic: {}, key: {}", kafkaTopic, keyString);
    }

    /**
     * Test is sync.
     *
     * @param f the f
     * @param keyString the key string
     */
    private void testIsSync(Future<RecordMetadata> f, String keyString) {
        if (isSyncPut) {
            try {
                f.get();
            } catch (InterruptedException exception) {
                logger.error("Interrupted exception occured when when putting message to kafka for PDID : {} "
                        + keyString, exception);
                Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
                logger.error("Failed when putting message to kafka for PDID : {} " + keyString, ee);
            }
        }
    }

    /**
     * Initializes the {@link KafkaProducer}.
     *
     * @param props the Properties instance with supplied configuration.
     */
    private void initKafkaProducer(Properties props) {
        logger.info("Initializing Kafka Producer");
        try {
            Class.forName(kafkaPartitionerClassName);
        } catch (Exception e) {
            logger.error("Failed when loading partitioner", e);
            throw new ClassNotFoundException("Failed when loading partitioner", e);
        }
        ClassLoader ccl = Thread.currentThread().getContextClassLoader();
        if (replaceClassloader) {
            Thread.currentThread().setContextClassLoader(null);
        }
        producer = new KafkaProducer<>(props);
        if (replaceClassloader) {
            Thread.currentThread().setContextClassLoader(ccl);
        }
    }

    /**
     * Flush all the records immediately.
     *
     * @see KafkaProducer#flush()
     */
    @Override
    public void flush() {
        logger.info("Flushing Kafka Producer");
        producer.flush();
    }

    /**
     * method to close the opened resources.
     */
    @Override
    public void close() {
        if (producer != null) {
            logger.info("Closing Kafka Producer :");
            producer.close();
            logger.info("Closed Kafka Producer :");
        }
    }

}
