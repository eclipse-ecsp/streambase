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

package org.eclipse.ecsp.stream.dma;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;



/**
 * class KafkaDispatcherIntegrationTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@TestPropertySource("/dma-test-kafka-dispatch.properties")
public class KafkaDispatcherIntegrationTest extends KafkaStreamsApplicationTestBase {

    /** The Constant KAFKA_HEADER_KEY_1. */
    private static final String KAFKA_HEADER_KEY_1 = "header_key_1";
    
    /** The Constant KAFKA_HEADER_KEY_2. */
    private static final String KAFKA_HEADER_KEY_2 = "header_key_2";
    
    /** The Constant KAFKA_HEADER_KEY_3. */
    private static final String KAFKA_HEADER_KEY_3 = "header_key_3";
    
    /** The Constant KAFKA_HEADER_VALUE_1. */
    private static final String KAFKA_HEADER_VALUE_1 = "header_value_1";
    
    /** The Constant KAFKA_HEADER_VALUE_1_MODIFIED. */
    private static final String KAFKA_HEADER_VALUE_1_MODIFIED = "header_value_1_modified";
    
    /** The Constant KAFKA_HEADER_VALUE_2. */
    private static final String KAFKA_HEADER_VALUE_2 = "header_value_2";
    
    /** The Constant KAFKA_HEADER_VALUE_3. */
    private static final String KAFKA_HEADER_VALUE_3 = "header_value_3";
    
    /** The Constant HEADER_VALUE_ASSERTION_FAILURE. */
    private static final String HEADER_VALUE_ASSERTION_FAILURE = "Header value does not match";
    
    /** The vehicle id 1. */
    private static String vehicleId1 = "Vehicle12345";
    
    /** The kafka headers 1. */
    List<Header> kafkaHeaders1;
    
    /** The kafka headers 2. */
    List<Header> kafkaHeaders2;
    
    /** The service name. */
    @Value("${service.name}")
    private String serviceName;
    
    /** The source topic. */
    @Value("${source.topic.name}")
    private String sourceTopic;
    
    /** The mqtt prefix. */
    @Value("${mqtt.service.topic.name.prefix}")
    private String mqttPrefix;
    
    /** The to device. */
    @Value("${" + PropertyNames.MQTT_TOPIC_TO_DEVICE_INFIX + ":" + Constants.TO_DEVICE + "}")
    private String toDevice;
    
    /** The mqtt topic. */
    @Value("${mqtt.service.topic.name}")
    private String mqttTopic;
    
    /** The kafka dispatch topic name. */
    private String kafkaDispatchTopicName;
    
    /** The vehicle id 2. */
    private String vehicleId2 = "Vehicle6789";

    /**
     * setUp().
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        super.setup();
        kafkaDispatchTopicName = "kafka-dispatch-topic";
        createTopics(sourceTopic, kafkaDispatchTopicName);
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "demo_consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
    }

    /**
     * Test with kafka headers.
     *
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     * @throws TimeoutException the timeout exception
     */
    @Test
    public void testWithKafkaHeaders() throws ExecutionException, InterruptedException, TimeoutException {

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());

        kafkaHeaders1 = new ArrayList<>();
        kafkaHeaders1.add(new RecordHeader(KAFKA_HEADER_KEY_1, KAFKA_HEADER_VALUE_1.getBytes(StandardCharsets.UTF_8)));
        kafkaHeaders1.add(new RecordHeader(KAFKA_HEADER_KEY_2, KAFKA_HEADER_VALUE_2.getBytes(StandardCharsets.UTF_8)));
        kafkaHeaders2 = new ArrayList<>();
        String speedEvent1 = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\":"
                + " {\"value\":20.0},\"MessageId\": \"1234\",\"CorrelationId\": \"1234\","
                + "\"BizTransactionId\": \"Biz1234\",\"ecuType\":\"testEcu\",\"VehicleId\": "
                + "\"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";

        kafkaHeaders2.add(new RecordHeader(KAFKA_HEADER_KEY_3, KAFKA_HEADER_VALUE_3.getBytes(StandardCharsets.UTF_8)));
        sendMessages(sourceTopic, producerProps, Arrays.asList(vehicleId1.getBytes(), 
                speedEvent1.getBytes()), kafkaHeaders1);
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        Map<String, String> headersMap = getHeadersFromMessageWithKey(vehicleId1);
        String headerValue1 = headersMap.get(KAFKA_HEADER_KEY_1);
        assertTrue(headerValue1.equalsIgnoreCase(KAFKA_HEADER_VALUE_1_MODIFIED), HEADER_VALUE_ASSERTION_FAILURE);
        String headerValue2 = headersMap.get(KAFKA_HEADER_KEY_2);
        assertTrue(headerValue2.equalsIgnoreCase(KAFKA_HEADER_VALUE_2), HEADER_VALUE_ASSERTION_FAILURE);
        String speedEvent2 = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\":"
                + " {\"value\":20.0},\"MessageId\": \"567\",\"CorrelationId\": \"567\","
                + "\"BizTransactionId\": \"Biz567\",\"ecuType\":\"testEcu\",\"VehicleId\": "
                + "\"Vehicle6789\",\"SourceDeviceId\": \"6789\"}";
        assertEquals(Constants.TWO, headersMap.entrySet().size());

        sendMessages(sourceTopic, producerProps, Arrays.asList(vehicleId2.getBytes(), 
                speedEvent2.getBytes()), kafkaHeaders2);
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        Map<String, String> headersMap2 = getHeadersFromMessageWithKey(vehicleId2);
        String headerValue3 = headersMap2.get(KAFKA_HEADER_KEY_3);
        assertTrue(headerValue3.equalsIgnoreCase(KAFKA_HEADER_VALUE_3), HEADER_VALUE_ASSERTION_FAILURE);
        assertEquals(1, headersMap2.entrySet().size());
    }

    /**
     * Gets the headers from message with key.
     *
     * @param key the key
     * @return the headers from message with key
     * @throws InterruptedException the interrupted exception
     * @throws TimeoutException the timeout exception
     */
    private Map<String, String> getHeadersFromMessageWithKey(String key)
            throws InterruptedException, TimeoutException {
        List<ConsumerRecord<Object, Object>> messages = getMessagesWithHeaders(
                kafkaDispatchTopicName, consumerProps, Constants.TEN, Constants.INT_20000);
        Map<String, String> headersMap = new HashMap<>();

        for (ConsumerRecord<Object, Object> message : messages) {
            if (message.key().equals(key)) {
                Headers headers = message.headers();
                for (Header header : headers) {
                    headersMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                }
            }
        }
        return headersMap;
    }

    /**
     * Send messages.
     *
     * @param topic the topic
     * @param producerProps the producer props
     * @param bytes the bytes
     * @param kafkaHeader the kafka header
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    private void sendMessages(String topic, Properties producerProps, List<byte[]> bytes, List<Header> kafkaHeader)
            throws ExecutionException, InterruptedException {
        Collection<KeyValue<Object, Object>> kvs = new ArrayList<>();
        for (int i = 1; i <= bytes.size(); i++) {
            if (i % Constants.TWO == 0) {
                kvs.add(new KeyValue(bytes.get(i - Constants.TWO), bytes.get(i - 1)));
            }
        }
        KafkaTestUtils.produceKeyValuesSynchronouslyWithHeaders(topic, kvs, producerProps, kafkaHeader);
    }

    /**
     * Gets the messages with headers.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param topic the topic
     * @param consumerProps the consumer props
     * @param n the n
     * @param waitTime the wait time
     * @return the messages with headers
     * @throws TimeoutException the timeout exception
     * @throws InterruptedException the interrupted exception
     */
    private <K, V> List<ConsumerRecord<K, V>> getMessagesWithHeaders(String topic,
            Properties consumerProps, int n, int waitTime)
            throws TimeoutException, InterruptedException {
        int timeWaited = 0;
        int increment = Constants.THREAD_SLEEP_TIME_2000;
        List<ConsumerRecord<K, V>> messages = new ArrayList<>();
        while ((messages.size() < n) && (timeWaited <= waitTime)) {
            messages.addAll(KafkaTestUtils.readMessagesWithHeaders(topic, consumerProps, n));
            Thread.sleep(increment);
            timeWaited = timeWaited + increment;
        }
        return messages;
    }

    /**
     * inner class KafkaDispatcherTestStreamProcessor implements IgniteEventStreamProcessor.
     */
    public static class KafkaDispatcherTestStreamProcessor implements IgniteEventStreamProcessor {
        
        /** The spc. */
        private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
            this.spc = spc;

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return "kafka-dis-sp";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            IgniteEvent value = kafkaRecord.value();
            if (!value.getEventId().equals(EventID.DEVICEMESSAGEFAILURE)) {
                ((AbstractIgniteEvent) value).setDeviceRoutable(true);
                System.out.println("Process for service processor called");

                // modify headers for event with vehicleId1
                if (value.getVehicleId().equalsIgnoreCase(vehicleId1)) {
                    Map<String, String> kafkaHeaders = value.getKafkaHeaders();
                    kafkaHeaders.put(KAFKA_HEADER_KEY_1, KAFKA_HEADER_VALUE_1_MODIFIED);
                    ((AbstractIgniteEvent) value).setKafkaHeaders(kafkaHeaders);
                }
                spc.forward(kafkaRecord);
            }
        }

        /**
         * Punctuate.
         *
         * @param timestamp the timestamp
         */
        @Override
        public void punctuate(long timestamp) {


        }

        /**
         * Close.
         */
        @Override
        public void close() {


        }

        /**
         * Config changed.
         *
         * @param props the props
         */
        @Override
        public void configChanged(Properties props) {


        }

        /**
         * Creates the state store.
         *
         * @return the harman persistent KV store
         */
        @Override
        public HarmanPersistentKVStore createStateStore() {
            return null;
        }
    }
}