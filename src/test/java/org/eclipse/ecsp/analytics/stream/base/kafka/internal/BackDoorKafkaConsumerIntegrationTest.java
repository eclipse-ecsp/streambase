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

package org.eclipse.ecsp.analytics.stream.base.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Optional;
import java.util.Properties;



/**
 * BackDoorKafkaConsumerIntegrationTest implements {@link KafkaStreamsApplicationTestBase}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-connectionstatus-handler-test.properties")
public class BackDoorKafkaConsumerIntegrationTest extends KafkaStreamsApplicationTestBase {

    /** The conn status topic. */
    private static String connStatusTopic;
    
    /** The source topic name. */
    private static String sourceTopicName;
    
    /** The i. */
    private static int i = 0;
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The back doorconsumer. */
    @Autowired
    private DeviceStatusBackDoorKafkaConsumer backDoorconsumer;

    /**
     * Subclasses should invoke this method in their @Before.
     *
     * @throws Exception when topic creation fails
     */
    @Before
    public void setup() throws Exception {
        super.setup();
        i++;
        sourceTopicName = "sourceTopic";
        connStatusTopic = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName.toLowerCase();
        createTopics(connStatusTopic, sourceTopicName, "dff-dfn-updates");
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopicName);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception {
        shutDownApplication();
    }

    /**
     * Start back door kafka consumer test.
     *
     * @throws Exception the exception
     */
    @Test
    public void startBackDoorKafkaConsumerTest() throws Exception {

        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, BackDoorTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        backDoorconsumer.shutdown();
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        backDoorconsumer.setIgniteKeyTransformerImpl("org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        backDoorconsumer.setKafkaBootstrapServers(consumerProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        backDoorconsumer.initializeProperties();
        TestCallBack callBack = new TestCallBack();

        backDoorconsumer.setDeviceStatusTopicName(connStatusTopic);
        backDoorconsumer.setDmaConsumerGroupId((i + "testBackDoorGroupId"));
        backDoorconsumer.setDmaConsumerPoll(TestConstants.THREAD_SLEEP_TIME_10);
        backDoorconsumer.setDmaAutoOffsetReset("latest");
        backDoorconsumer.setServiceName("TestService");
        backDoorconsumer.addCallback(callBack, 0);
        backDoorconsumer.startDMABackDoorConsumer();
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        String deviceId = "12345";
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": "
                + "{\"value\":20.0},\"MessageId\": "
                + "\"1234\",\"CorrelationId\": \"1234\",\"BizTransactionId\": \"Biz1234\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, deviceId.getBytes(), speedEvent.getBytes());
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        Assert.assertEquals(deviceId, callBack.getKey());
        Assert.assertEquals("Speed", callBack.getValue().getEventId());
        Assert.assertEquals("1.0", callBack.getValue().getVersion().getValue());
        Assert.assertEquals("1234", callBack.getValue().getMessageId());
        Assert.assertEquals("1234", callBack.getValue().getCorrelationId());
        Assert.assertEquals("Biz1234", callBack.getValue().getBizTransactionId());

        backDoorconsumer.shutdown();
        shutDownApplication();
    }

    /**
     * Test get instance.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetInstance() throws Exception {

        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, BackDoorTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        backDoorconsumer.setIgniteKeyTransformerImpl("org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        backDoorconsumer.setKafkaBootstrapServers(consumerProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        backDoorconsumer
                .setConnectionMsgValueTransformer("org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer");
        backDoorconsumer.initializeProperties();
        Assert.assertTrue(backDoorconsumer.getPayloadValueTransformer() instanceof DeviceMessageIgniteEventTransformer);
    }

    /**
     * Test get instance with exception.
     *
     * @throws Exception the exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetInstanceWithException() throws Exception {

        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, BackDoorTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        backDoorconsumer.setIgniteKeyTransformerImpl("org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        backDoorconsumer.setKafkaBootstrapServers(consumerProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        backDoorconsumer
                .setConnectionMsgValueTransformer("org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer");
        Mockito.when(ctx.getBean((Class<Object>) Mockito.any())).thenThrow(IllegalArgumentException.class);
        backDoorconsumer.initializeProperties();
    }

    /**
     * Stop and start back door kafka consumer.
     *
     * @throws Exception the exception
     */
    @Test
    public void stopAndStartBackDoorKafkaConsumer() throws Exception {

        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, BackDoorTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(TestConstants.HUNDRED_THOUSAND);
        backDoorconsumer.shutdown();
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        backDoorconsumer.setIgniteKeyTransformerImpl("org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        backDoorconsumer.setKafkaBootstrapServers(consumerProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        backDoorconsumer.initializeProperties();
        TestCallBack callBack = new TestCallBack();
        backDoorconsumer.setDeviceStatusTopicName(connStatusTopic);
        backDoorconsumer.setDmaConsumerGroupId((i + "testBackDoorGroupId"));
        backDoorconsumer.setDmaConsumerPoll(TestConstants.THREAD_SLEEP_TIME_10);
        backDoorconsumer.setDmaAutoOffsetReset("latest");
        backDoorconsumer.setServiceName("TestService");
        backDoorconsumer.addCallback(callBack, 0);
        backDoorconsumer.startDMABackDoorConsumer();
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        String deviceId = "12345";
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": "
                + "{\"value\":20.0},\"MessageId\": \"1234\","
                + "\"CorrelationId\": \"1234\",\"BizTransactionId\": \"Biz1234\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, deviceId.getBytes(), speedEvent.getBytes());
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        Assert.assertEquals(deviceId, callBack.getKey());
        Assert.assertEquals("Speed", callBack.getValue().getEventId());
        Assert.assertEquals("1.0", callBack.getValue().getVersion().getValue());
        Assert.assertEquals("1234", callBack.getValue().getMessageId());
        Assert.assertEquals("1234", callBack.getValue().getCorrelationId());
        Assert.assertEquals("Biz1234", callBack.getValue().getBizTransactionId());

        backDoorconsumer.shutdown();
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        backDoorconsumer.addCallback(callBack, 0);
        backDoorconsumer.startDMABackDoorConsumer();
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":21.0},"
                + "\"MessageId\": \"1235\",\"CorrelationId\": \"1234\",\"BizTransactionId\": \"Biz1234\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, deviceId.getBytes(), speedEvent.getBytes());
        Thread.sleep(TestConstants.FIFTY_THOUSAND);
        Assert.assertEquals(deviceId, callBack.getKey());
        Assert.assertEquals("Speed", callBack.getValue().getEventId());
        Assert.assertEquals("1.0", callBack.getValue().getVersion().getValue());
        Assert.assertEquals("1235", callBack.getValue().getMessageId());
        Assert.assertEquals("1234", callBack.getValue().getCorrelationId());
        Assert.assertEquals("Biz1234", callBack.getValue().getBizTransactionId());

        backDoorconsumer.shutdown();
        shutDownApplication();
    }

    /**
     * This stream processor implements {@link IgniteEventStreamProcessor}.
     */
    public static final class BackDoorTestServiceProcessor implements IgniteEventStreamProcessor {
        
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
            return "BackDoorTestServiceProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            AbstractIgniteEvent event = (AbstractIgniteEvent) kafkaRecord.value();
            event.setDeviceRoutable(true);
            kafkaRecord.withValue(event);
            spc.forward(kafkaRecord);
        }

        /**
         * Punctuate.
         *
         * @param timestamp the timestamp
         */
        @Override
        public void punctuate(long timestamp) {
            //
        }

        /**
         * Close.
         */
        @Override
        public void close() {
            //
        }

        /**
         * Config changed.
         *
         * @param props the props
         */
        @Override
        public void configChanged(Properties props) {
            //
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
            return new String[] { sourceTopicName };
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
     * The Class TestCallBack.
     */
    class TestCallBack implements BackdoorKafkaConsumerCallback {

        /** The key. */
        private String key;
        
        /** The value. */
        private IgniteEvent value;

        /**
         * Gets the key.
         *
         * @return the key
         */
        public String getKey() {
            return key;
        }

        /**
         * Gets the value.
         *
         * @return the value
         */
        public IgniteEvent getValue() {
            return value;
        }

        /**
         * Process.
         *
         * @param key the key
         * @param value the value
         * @param meta the meta
         */
        @Override
        public void process(IgniteKey key, IgniteEvent value, OffsetMetadata meta) {
            this.key = key.getKey().toString();
            this.value = value;
        }

        /**
         * Gets the committable offset.
         *
         * @return the committable offset
         */
        @Override
        public Optional<OffsetMetadata> getCommittableOffset() {
            return Optional.empty();
        }

    }
}
