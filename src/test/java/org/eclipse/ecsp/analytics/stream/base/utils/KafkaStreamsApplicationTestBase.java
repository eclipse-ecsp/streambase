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

import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.kafka.SingleNodeKafkaCluster;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.ClassRule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;




/**
 * A convenient base class for integration testing kafka streams service applications.
 * The key things to do when subclassing this are a) invoke super.setup()
 * in subclass setup() b) invoke super.launchApplication() in the
 * test case methods to start the stream processing application.
 * Check the KafkaStreamsLauncherTest class for concrete examples
 *
 * @author ssasidharan
 */
public class KafkaStreamsApplicationTestBase {
    
    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The Constant MQTT_SERVER. */
    @ClassRule
    public static final EmbeddedMQTTServer MQTT_SERVER = new EmbeddedMQTTServer();
    
    /** The Constant REDIS_SERVER. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS_SERVER = new EmbeddedRedisServer();
    // RTC-155383 - Running Kafka and Zookeeper on dynamic ports to resolve bind
    /** The Constant KAFKA_CLUSTER. */
    // address issue in streambase project
    @ClassRule
    public static final SingleNodeKafkaCluster KAFKA_CLUSTER = new SingleNodeKafkaCluster();
    
    /** The Constant UNLIMITED_MESSAGES. */
    private static final int UNLIMITED_MESSAGES = -1;
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaStreamsApplicationTestBase.class);
    
    /** The ctx. */
    @Autowired
    protected ApplicationContext ctx;
    
    /** The ks props. */
    protected Properties ksProps;
    
    /** The consumer props. */
    protected Properties consumerProps;
    
    /** The producer props. */
    protected Properties producerProps;
    
    /** The mqtt messages. */
    private Map<String, List<byte[]>> mqttMessages = new HashMap<>();
    
    /** The prometheus export server. */
    private HTTPServer prometheusExportServer;
    
    /** The launcher. */
    private Launcher launcher;
    
    /** The enable prometheus. */
    private boolean enablePrometheus;

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
            int maxTotalPollTimeMs = Constants.THREAD_SLEEP_TIME_2000;
            int totalPollTimeMs = 0;
            while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumedValues.size(), maxMessages)) {
                totalPollTimeMs += pollIntervalMs;
                ConsumerRecords<K, V> records = consumer.poll(pollIntervalMs);
                for (ConsumerRecord<K, V> record : records) {
                    consumedValues.add(new KeyValue<>(record.key(), record.value()));
                }
            }
        } finally {
            consumer.close();
        }
        return consumedValues;
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
     * This launches the kafka streams application in an existing Spring context.
     * Spring context has to be loaded by the subclass using.
     *
     * @throws Exception the exception
     * @RunWith or @ExtendWith.
     */
    protected void launchApplication() throws Exception {
        launcher = ctx.getBean(Launcher.class);
        launcher.setExecuteShutdownHook(false);
        /*
         * if (enablePrometheus) { prometheusExportServer = new HTTPServer(1234, true);
         * }
         */
        launcher.launch();
    }

    /**
     * This shuts down the kafka streams application in an existing Spring context.
     */
    protected void shutDownApplication() {
        logger.info("Shutting down kafka streams with timeout of 30 seconds");
        launcher.closeStreamWithTimeout();
        if (null != prometheusExportServer) {
            prometheusExportServer.stop();
        }
    }

    /**
     * Shut down.
     */
    protected void shutDown() {
        logger.info("Shutting down kafka streams with timeout of 30 seconds");
        launcher.closeStreamWithTimeout();
    }

    /**
     * Creates topics and ensures they are created by deleting if need be before creating.
     *
     * @param topics topics
     * @throws InterruptedException the interrupted exception
     */
    protected void createTopics(String... topics) throws InterruptedException {
        for (String topic : topics) {
            while (true) {
                logger.info("Deleting topic {}", topic);
                try {
                    KAFKA_CLUSTER.deleteTopic(topic);
                } catch (Exception e) {
                    logger.info("Exception: {}", e);
                }
                Thread.sleep(Constants.THREAD_SLEEP_TIME_100);
                logger.info("Creating topic {}", topic);
                try {
                    KAFKA_CLUSTER.createTopic(topic);
                } catch (TopicExistsException tee) {
                    logger.error("Creating topic {} failed. Will delete and try again", topic);
                    continue;
                } catch (Exception e) {
                    logger.error("Exception occurred - ", e);
                }
                break;
            }
        }
    }

    /**
     * Subclasses should invoke this method in their @Before. And use the
     * ksProps, consumerProps and producerProps inherited from this
     * base.
     *
     * @throws Exception Exception
     */
    public void setup() throws Exception {
        Thread.sleep(Constants.THREAD_SLEEP_TIME_2000);
        ksProps = new Properties();
        ksProps.put(PropertyNames.LAUNCHER_IMPL, "org.eclipse.ecsp.analytics.stream.base.KafkaStreamsLauncher");
        ksProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ksProps.put(PropertyNames.BOOTSTRAP_SERVERS, KAFKA_CLUSTER.bootstrapServers());
        ksProps.put(PropertyNames.ZOOKEEPER_CONNECT, KAFKA_CLUSTER.zkconnectstring());
        ksProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.ByteArray().getClass().getName());
        ksProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.ByteArray().getClass().getName());
        ksProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().deserializer().getClass().getName());
        ksProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().deserializer().getClass().getName());
        ksProps.put(PropertyNames.NUM_STREAM_THREADS, "1");
        ksProps.put(PropertyNames.REPLICATION_FACTOR, "1");
        ksProps.put(PropertyNames.SHARED_TOPICS, "Master-Data-Topic, Config-Data-Topic");
        ksProps.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        ksProps.put(PropertyNames.KAFKA_SSL_ENABLE, "false");
        Launcher.setDynamicProps(ksProps);
        consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        KafkaTestUtils.purgeLocalStreamsState(ksProps);
    }

    /**
     * Removes local state stores. Useful to reset state in-between integration test runs.
     *
     * @param streamsConfiguration         Streams configuration settings
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected void purgeLocalStreamsState(Properties streamsConfiguration) throws IOException {
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
    protected <K, V> void produceKeyValuesSynchronously(
            String topic, Collection<KeyValue<K, V>> records, Properties producerConfig)
            throws ExecutionException, InterruptedException {
        Producer<K, V> producer = new KafkaProducer<>(producerConfig);
        try {
            for (KeyValue<K, V> record : records) {
                Future<RecordMetadata> f = producer.send(
                        new ProducerRecord<>(topic, record.key, record.value));
                f.get();
            }
            producer.flush();
        } finally {
            producer.close();
        }
    }

    /**
     * Produce values synchronously.
     *
     * @param <V> the value type
     * @param topic the topic
     * @param records the records
     * @param producerConfig the producer config
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    protected <V> void produceValuesSynchronously(
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
     * Read messages.
     *
     * @param topic the topic
     * @param consumerProps the consumer props
     * @param i the i
     * @return the list
     * @throws TimeoutException the timeout exception
     */
    protected List<String[]> readMessages(String topic, Properties consumerProps, int i) throws TimeoutException {
        return KafkaTestUtils.readKeyValues(topic, consumerProps, i).stream()
                .map(t -> new String[] { (String) t.key, (String) t.value })
                .toList();
    }

    /**
     * Send messages.
     *
     * @param topic the topic
     * @param producerProps the producer props
     * @param strings the strings
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    protected void sendMessages(String topic, Properties producerProps, String... strings)
            throws ExecutionException, InterruptedException {
        Collection<KeyValue<Object, Object>> kvs = new ArrayList<>();
        for (int i = 1; i <= strings.length; i++) {
            if (i % Constants.TWO == 0) {
                kvs.add(new KeyValue(strings[i - Constants.TWO], strings[i - 1]));
            }
        }
        KafkaTestUtils.produceKeyValuesSynchronously(topic, kvs, producerProps);
    }

    /**
     * Send messages.
     *
     * @param topic the topic
     * @param producerProps the producer props
     * @param bytes the bytes
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    protected void sendMessages(String topic, Properties producerProps, List<byte[]> bytes)
            throws ExecutionException, InterruptedException {
        Collection<KeyValue<Object, Object>> kvs = new ArrayList<>();
        for (int i = 1; i <= bytes.size(); i++) {
            if (i % Constants.TWO == 0) {
                kvs.add(new KeyValue(bytes.get(i - Constants.TWO), bytes.get(i - 1)));
            }
        }
        KafkaTestUtils.produceKeyValuesSynchronously(topic, kvs, producerProps);
    }

    /**
     * Gets the messages.
     *
     * @param topic the topic
     * @param consumerProps the consumer props
     * @param n the n
     * @param waitTime the wait time
     * @return the messages
     * @throws TimeoutException the timeout exception
     * @throws InterruptedException the interrupted exception
     */
    protected List<String[]> getMessages(String topic, Properties consumerProps, int n, int waitTime)
            throws TimeoutException, InterruptedException {
        int timeWaited = 0;
        int increment = Constants.THREAD_SLEEP_TIME_2000;
        List<String[]> messages = new ArrayList<>();
        while ((messages.size() < n) && (timeWaited <= waitTime)) {
            messages.addAll(KafkaTestUtils.readMessages(topic, consumerProps, n));
            Thread.sleep(increment);
            timeWaited = timeWaited + increment;
        }
        return messages;
    }

    /**
     * Subscibe to mqtt topic.
     *
     * @param topic the topic
     * @throws MqttException the mqtt exception
     */
    protected void subscibeToMqttTopic(String topic) throws MqttException {
        MqttCallback callback = new MqttCallback() {

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                logger.info("Received message on topic {}", topic);
                if (mqttMessages.containsKey(topic)) {
                    mqttMessages.get(topic).add(message.getPayload());
                } else {
                    List<byte[]> messages = new ArrayList<>();
                    messages.add(message.getPayload());
                    mqttMessages.put(topic, messages);
                }

            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Nothing to do

            }

            @Override
            public void connectionLost(Throwable cause) {
                // Nothing to do

            }
        };
        MQTT_SERVER.subscribeToTopic(topic, callback);
    }

    /**
     * Gets the messages from mqtt topic.
     *
     * @param topic the topic
     * @param n the n
     * @param waitTime the wait time
     * @return the messages from mqtt topic
     * @throws TimeoutException the timeout exception
     * @throws InterruptedException the interrupted exception
     */
    protected List<byte[]> getMessagesFromMqttTopic(String topic, int n, int waitTime)
            throws TimeoutException, InterruptedException {
        int timeWaited = 0;
        int increment = Constants.THREAD_SLEEP_TIME_2000;
        List<byte[]> messages = new ArrayList<>();
        while ((messages.size() < n) && (timeWaited <= waitTime)) {
            List<byte[]> payloads = mqttMessages.get(topic);
            if (null != payloads) {
                messages.addAll(payloads);
            }
            Thread.sleep(increment);
            timeWaited = timeWaited + increment;
        }
        return messages;
    }

    /**
     * Publish message to mqtt topic.
     *
     * @param topic the topic
     * @param payload the payload
     * @throws MqttException the mqtt exception
     */
    protected void publishMessageToMqttTopic(String topic, byte[] payload) throws MqttException {
        MQTT_SERVER.publishToTopic(topic, payload);
    }

    /**
     * Retries a function call and returns result or throws exception if
     * function didn't return a non-null response for all attempts. Retry
     * interval is 250ms.
     *
     * @param <R> the generic type
     * @param n         - number of retries to attempt
     * @param f         - function that should return a result if it is successful
     * @return result from function
     */
    protected <R> R retryWithException(int n, Function<Void, R> f) {
        return RetryUtils.retryWithException(n, f);
    }

    /**
     * Gets the key value records.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic the topic
     * @param consumerProps the consumer props
     * @param n the n
     * @param waitTime the wait time
     * @return the key value records
     * @throws InterruptedException the interrupted exception
     */
    protected <K, V> List<KeyValue<K, V>> getKeyValueRecords(String topic,
            Properties consumerProps, int n, int waitTime)
            throws InterruptedException {
        int timeWaited = 0;
        int increment = Constants.THREAD_SLEEP_TIME_2000;
        List<KeyValue<K, V>> messages = new ArrayList<>();
        while ((messages.size() < n) && (timeWaited <= waitTime)) {
            List<KeyValue<K, V>> currentList = KafkaTestUtils.readKeyValues(topic, consumerProps, n);
            messages.addAll(currentList);
            Thread.sleep(increment);
            timeWaited = timeWaited + increment;
        }
        return messages;
    }
}
