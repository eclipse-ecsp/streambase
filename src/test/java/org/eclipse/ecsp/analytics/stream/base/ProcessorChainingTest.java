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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.discovery.PropBasedDiscoveryServiceImpl;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * {@link ProcessorChainingTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/stream-base-test.properties")
public class ProcessorChainingTest extends KafkaStreamsApplicationTestBase {

    /** The in topic name. */
    private static String inTopicName;
    
    /** The out topic name. */
    private static String outTopicName;
    
    /** The i. */
    private static int i = 0;

    /**
     * Setup.
     *
     * @throws Exception the exception
     */
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        i++;
        inTopicName = "sourceTopic" + i;
        outTopicName = "sinkTopic" + i;
        createTopics(inTopicName, outTopicName);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.String().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.String().serializer().getClass().getName());
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, PropBasedDiscoveryServiceImpl.class.getName());
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, inTopicName);
        ksProps.put(PropertyNames.APPLICATION_ID, "pt");

    }

    /**
     * Testing Processor chaining with PRE_PROCESSORS, SERVICE_STREAM_PROCESSORS and POST_PROCESSORS.
     *
     * @throws Exception the exception
     */
    @Test
    public void testProcessorChaining() throws Exception {
        ksProps.put(PropertyNames.PRE_PROCESSORS, StreamPreProcessor.class.getName());
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, StreamServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS, StreamPostProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        launchApplication();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
        List<String[]> messages = KafkaTestUtils.getMessages(outTopicName, consumerProps,
                1, Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals("key1", messages.get(0)[0]);
        Assert.assertEquals("value1_StreamPreProcessor_StreamServiceProcessor_StreamPostProcessor",
                messages.get(0)[1]);

    }

    /**
     * Testing Processor chaining with only SERVICE_STREAM_PROCESSORS and POST_PROCESSORS.
     *
     * @throws Exception the exception
     */
    // @Test
    public void testProcessorChainingWithoutPreProcessor() throws Exception {
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, StreamServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS, StreamPostProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        launchApplication();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
        List<String[]> messages = KafkaTestUtils.getMessages(outTopicName, consumerProps,
                1, Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals("key1", messages.get(0)[0]);
        Assert.assertEquals("value1_StreamServiceProcessor_StreamPostProcessor",
                messages.get(0)[1]);

    }

    /**
     * Testing Processor chaining with only multiple PRE_PROCESSORS, SERVICE_STREAM_PROCESSORS and POST_PROCESSORS.
     *
     * @throws Exception the exception
     */
    @Test
    public void testProcessorChainingWithMultiplePreProcessor() throws Exception {
        ksProps.put(PropertyNames.PRE_PROCESSORS, StreamPreProcessor.class.getName() + ","
                + StreamPreProcessor2.class.getName());
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, StreamServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS, StreamPostProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        launchApplication();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
        List<String[]> messages = KafkaTestUtils.getMessages(outTopicName, consumerProps,
                1, Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals("key1", messages.get(0)[0]);
        Assert.assertEquals("value1_StreamPreProcessor_StreamPreProcessor2_StreamServiceProcessor_StreamPostProcessor",
                messages.get(0)[1]);

    }

    /**
     * inner class StreamPostProcessor implements StreamProcessor.
     */
    public static final class StreamPostProcessor implements StreamProcessor<byte[], byte[], String, String> {
        
        /** The spc. */
        private StreamProcessingContext<String, String> spc;

        /**
         * Instantiates a new stream post processor.
         */
        public StreamPostProcessor() {

        }

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<String, String> spc) {
            this.spc = spc;

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {

            return "StreamPostProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String fwdValue = new String(kafkaRecord.value()) + "_StreamPostProcessor";
            spc.forward(new Record<>(new String(kafkaRecord.key()), fwdValue, kafkaRecord.timestamp()));

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

        /**
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] { outTopicName };
        }

    }

    /**
     * inner class StreamPreProcessor implements StreamProcessor.
     */
    public static final class StreamPreProcessor implements StreamProcessor<byte[], byte[], byte[], byte[]> {
        
        /** The spc. */
        private StreamProcessingContext<byte[], byte[]> spc;

        /**
         * Instantiates a new stream pre processor.
         */
        public StreamPreProcessor() {

        }

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<byte[], byte[]> spc) {
            this.spc = spc;

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {

            return "StreamPreProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String fwdValue = new String(kafkaRecord.value()) + "_StreamPreProcessor";
            spc.forward(new Record<>(kafkaRecord.key(), fwdValue.getBytes(), kafkaRecord.timestamp()));
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

        /**
         * Sources.
         *
         * @return the string[]
         */
        @Override
        public String[] sources() {
            return new String[] { inTopicName };
        }

        /**
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] {};
        }
    }

    /**
     * inner class final class StreamServiceProcessor implements StreamProcessor.
     */
    public static final class StreamPreProcessor2 implements StreamProcessor<byte[], byte[], byte[], byte[]> {
        
        /** The spc. */
        private StreamProcessingContext<byte[], byte[]> spc;

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<byte[], byte[]> spc) {
            this.spc = spc;

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {

            return "StreamPreProcessor_2";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String fwdValue = new String(kafkaRecord.value()) + "_StreamPreProcessor2";
            spc.forward(new Record<>(kafkaRecord.key(), fwdValue.getBytes(), kafkaRecord.timestamp()));
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

        /**
         * Sources.
         *
         * @return the string[]
         */
        @Override
        public String[] sources() {
            return new String[] { inTopicName };
        }

        /**
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] {};
        }
    }

    /**
     * inner final class StreamServiceProcessor implements StreamProcessor.
     */
    public static final class StreamServiceProcessor implements StreamProcessor<byte[], byte[], byte[], byte[]> {
        
        /** The spc. */
        private StreamProcessingContext<byte[], byte[]> spc;

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<byte[], byte[]> spc) {
            this.spc = spc;

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {

            return "StreamServiceProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String fwdValue = new String(kafkaRecord.value()) + "_StreamServiceProcessor";
            spc.forward(new Record<>(kafkaRecord.key(), fwdValue.getBytes(), kafkaRecord.timestamp()));
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

        /**
         * Sources.
         *
         * @return the string[]
         */
        @Override
        public String[] sources() {
            return new String[] { inTopicName };
        }

        /**
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] {};
        }
    }

}
