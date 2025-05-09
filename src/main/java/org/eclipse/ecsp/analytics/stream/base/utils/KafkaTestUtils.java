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

package org.eclipse.ecsp.analytics.stream.base.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * Utility functions to make integration testing more convenient.
 */
public class KafkaTestUtils {
    
    /**
     * Instantiates a new kafka test utils.
     */
    private KafkaTestUtils() {

    }
    
    /** The Constant UNLIMITED_MESSAGES. */
    private static final int UNLIMITED_MESSAGES = -1;

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic         Kafka topic to read messages from
     * @param consumerConfig         Kafka consumer configuration
     * @param maxMessages         Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    public static <K, V> List<V> readValues(String topic, Properties consumerConfig, int maxMessages) {
        List<V> returnList = new ArrayList<>();
        List<KeyValue<K, V>> kvs = readKeyValues(topic, consumerConfig, maxMessages);
        for (KeyValue<K, V> kv : kvs) {
            returnList.add(kv.value);
        }
        return returnList;
    }

    /**
     * Returns as many messages as possible from the topic until a (currently hardcoded) timeout is reached.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic         Kafka topic to read messages from
     * @param consumerConfig         Kafka consumer configuration
     * @return The KeyValue elements retrieved via the consumer.
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(String topic, Properties consumerConfig) {
        return readKeyValues(topic, consumerConfig, UNLIMITED_MESSAGES);
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer
     * (the topic(s) to read from are already configured in the consumer).
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic         Kafka topic to read messages from
     * @param consumerConfig         Kafka consumer configuration
     * @param maxMessages         Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(String topic, Properties consumerConfig, int maxMessages) {
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig);
        List<KeyValue<K, V>> consumedValues = new ArrayList<>();
        try {
            consumer.subscribe(Collections.singletonList(topic));
            int pollIntervalMs = Constants.THREAD_SLEEP_TIME_100;
            int maxTotalPollTimeMs = Constants.THREAD_SLEEP_TIME_4000;
            int totalPollTimeMs = 0;
            while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumedValues.size(), maxMessages)) {
                totalPollTimeMs += pollIntervalMs;
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
                for (ConsumerRecord<K, V> consumerRecord : records) {
                    consumedValues.add(new KeyValue<>(consumerRecord.key(), consumerRecord.value()));
                }
            }
        } finally {
            consumer.close();
        }
        return consumedValues;
    }

    /**
     * readKeyValuesWithHeaders().
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic topic
     * @param consumerConfig consumerConfig
     * @param maxMessages maxMessages
     * @return the list
     */
    public static <K, V> List<ConsumerRecord<K, V>> readKeyValuesWithHeaders(
            String topic, Properties consumerConfig, int maxMessages) {
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig);
        List<ConsumerRecord<K, V>> consumerRecords = new ArrayList<>();
        try {
            consumer.subscribe(Collections.singletonList(topic));
            int pollIntervalMs = Constants.THREAD_SLEEP_TIME_100;
            int maxTotalPollTimeMs = Constants.THREAD_SLEEP_TIME_4000;
            int totalPollTimeMs = 0;
            while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumerRecords.size(), maxMessages)) {
                totalPollTimeMs += pollIntervalMs;
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
                for (ConsumerRecord<K, V> kvConsumerRecord : records) {
                    consumerRecords.add(kvConsumerRecord);
                }
            }
        } finally {
            consumer.close();
        }
        return consumerRecords;
    }

    /**
     * Continue consuming.
     *
     * @param messagesConsumed the messages consumed
     * @param maxMessages the max messages
     * @return true, if successful
     */
    private static boolean continueConsuming(int messagesConsumed, int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }

    /**
     * Removes local state stores. Useful to reset state in-between integration test runs.
     *
     * @param streamsConfiguration         Streams configuration settings
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static void purgeLocalStreamsState(Properties streamsConfiguration) throws IOException {
        String path = streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        if (path != null) {
            File node = Paths.get(path).normalize().toFile();
            // Only purge state when it's under /tmp. This is a safety net to
            // prevent accidentally
            // deleting important local directory trees.
            if (node.getAbsolutePath().startsWith("/tmp")) {
                Utils.delete(new File(node.getAbsolutePath()));
            }
        }
    }

    /**
     * produceKeyValuesSynchronously().
     *
     * @param <K>         Key type of the data records
     * @param <V>         Value type of the data records
     * @param topic         Kafka topic to write the data records to
     * @param records         Data records to write to Kafka
     * @param producerConfig         Kafka producer configuration
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    public static <K, V> void produceKeyValuesSynchronously(
            String topic, Collection<KeyValue<K, V>> records, Properties producerConfig)
            throws ExecutionException, InterruptedException {
        try (Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            for (KeyValue<K, V> keyValue : records) {
                Future<RecordMetadata> f = producer.send(
                        new ProducerRecord<>(topic, keyValue.key, keyValue.value));
                f.get();
            }
            producer.flush();
        }
    }

    /**
     * produceKeyValuesSynchronouslyWithHeaders().
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic topic
     * @param records records
     * @param producerConfig producerConfig
     * @param kafkaHeader kafkaHeader
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static <K, V> void produceKeyValuesSynchronouslyWithHeaders(
            String topic, Collection<KeyValue<K, V>> records, Properties producerConfig, List<Header> kafkaHeader)
            throws ExecutionException, InterruptedException {

        try (Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            for (KeyValue<K, V> keyValue : records) {
                Future<RecordMetadata> f = producer.send(
                        new ProducerRecord<>(topic, null, keyValue.key, keyValue.value, kafkaHeader));
                f.get();
            }
            producer.flush();
        }
    }

    /**
     * produceValuesSynchronously().
     *
     * @param <V> the value type
     * @param topic topic
     * @param records records
     * @param producerConfig producerConfig
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static <V> void produceValuesSynchronously(
            String topic, Collection<V> records, Properties producerConfig)
            throws ExecutionException, InterruptedException {
        Collection<KeyValue<Object, V>> keyedRecords = new ArrayList<>();
        for (V value : records) {
            KeyValue<Object, V> kv = new KeyValue<>(null, value);
            keyedRecords.add(kv);
        }
        produceKeyValuesSynchronously(topic, keyedRecords, producerConfig);
    }

    /**
     * readMessages().
     *
     * @param topic topic
     * @param consumerProps consumerProps
     * @param i i
     * @return List
     */
    public static List<String[]> readMessages(String topic, Properties consumerProps, int i) {
        return KafkaTestUtils.readKeyValues(topic, consumerProps, i).stream()
                .map(t -> new String[] { (String) t.key, (String) t.value }).toList();
    }

    /**
     * Read messages with headers.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic the topic
     * @param consumerProps the consumer props
     * @param i the i
     * @return the list
     */
    public static <K, V> List<ConsumerRecord<K, V>> readMessagesWithHeaders(
            String topic, Properties consumerProps, int i) {
        return KafkaTestUtils.readKeyValuesWithHeaders(topic, consumerProps, i);
    }

    /**.
     * sendMessages().
     *
     * @param topic topic
     * @param producerProps producerProps
     * @param strings strings
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static void sendMessages(String topic, Properties producerProps, String... strings)
            throws ExecutionException, InterruptedException {
        Collection<KeyValue<Object, Object>> kvs = new ArrayList<>();
        for (int i = 1; i <= strings.length; i++) {
            if (i % Constants.TWO == 0) {
                kvs.add(new KeyValue<Object, Object>(strings[i - Constants.TWO], strings[i - 1]));
            }
        }
        KafkaTestUtils.produceKeyValuesSynchronously(topic, kvs, producerProps);
    }

    /**
     * sendMessages().
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic topic
     * @param producerProps producerProps
     * @param key key
     * @param value value
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static <K, V> void sendMessages(String topic, Properties producerProps, K key, V value)
            throws ExecutionException, InterruptedException {
        Collection<KeyValue<K, V>> kvs = new ArrayList<>();
        kvs.add(new KeyValue<>(key, value));
        KafkaTestUtils.produceKeyValuesSynchronously(topic, kvs, producerProps);
    }

    /**
     * sendMessages().
     *
     * @param topic topic
     * @param producerProps producerProps
     * @param bytes bytes
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static void sendMessages(String topic, Properties producerProps, List<byte[]> bytes)
            throws ExecutionException, InterruptedException {
        Collection<KeyValue<Object, Object>> kvs = new ArrayList<>();
        for (int i = 1; i <= bytes.size(); i++) {
            if (i % Constants.TWO == 0) {
                kvs.add(new KeyValue<>(bytes.get(i - Constants.TWO), bytes.get(i - 1)));
            }
        }
        KafkaTestUtils.produceKeyValuesSynchronously(topic, kvs, producerProps);
    }

    /**
     * getMessages().
     *
     * @param topic topic
     * @param consumerProps consumerProps
     * @param n n
     * @param waitTime waitTime
     * @return List
     * @throws InterruptedException InterruptedException
     */
    public static List<String[]> getMessages(String topic, Properties consumerProps, int n, int waitTime)
            throws InterruptedException {
        int timeWaited = 0;
        int increment = Constants.THREAD_SLEEP_TIME_500;
        List<String[]> messages = new ArrayList<>();
        while ((messages.size() < n) && (timeWaited <= waitTime)) {
            messages.addAll(KafkaTestUtils.readMessages(topic, consumerProps, n));
            Thread.sleep(increment);
            timeWaited = timeWaited + increment;
        }
        return messages;
    }
}
