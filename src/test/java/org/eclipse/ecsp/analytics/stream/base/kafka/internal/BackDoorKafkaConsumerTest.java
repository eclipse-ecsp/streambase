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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;



/**
 * BackDoorKafkaConsumerTest implements {@link KafkaStreamsApplicationTestBase}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-backdoor-consumer-test.properties")
public class BackDoorKafkaConsumerTest extends KafkaStreamsApplicationTestBase {

    /** The device conn status topic. */
    private String deviceConnStatusTopic = "device-status-test";
    
    /** The i. */
    private static int i;
    
    /** The consumer. */
    @Autowired
    private DeviceStatusBackDoorKafkaConsumer consumer;

    /**
     * Subclasses should invoke this method in their @Before.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        i++;
        deviceConnStatusTopic = deviceConnStatusTopic + i;
        super.setup();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        createTopics((deviceConnStatusTopic));
    }

    /**
     * Test initialize properties.
     */
    @Test
    public void testInitializeProperties() {
        // below 2 lines have been added just to pass the test case.
        AdminClient client = KafkaAdminClient.create(consumerProps);
        consumer.setKafkaBootstrapServers("localhost:9092");
        consumer.setKafkaAdminClient(client);
        consumer.setIgniteKeyTransformerImpl("org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        
        consumer.initializeProperties();
        Properties props = consumer.getKafkaConsumerProps();

        Assert.assertEquals(Serdes.ByteArray().deserializer().getClass().getName(),
                props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        Assert.assertEquals(Serdes.ByteArray().deserializer().getClass().getName(),
                props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        Assert.assertNotNull(props.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        Assert.assertNotNull(props.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        Assert.assertNotNull(props.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        Assert.assertNotNull(props.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        Assert.assertNotNull(props.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        Assert.assertEquals("required", props.get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG));
        Assert.assertEquals("SSL", props.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    /**
     * Test ignite key transformer impl missing.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testIgniteKeyTransformerImplMissing() {
        consumer.setKafkaAdminClient(null);
        consumer.setIgniteKeyTransformerImpl(null);
        consumer.initializeProperties();
    }

    /**
     * Test group id missing.
     */
    @Test(expected = RuntimeException.class)
    public void testGroupIdMissing() {
        DeviceStatusBackDoorKafkaConsumer consumer = new DeviceStatusBackDoorKafkaConsumer();
        consumer.setIgniteKeyTransformerImpl("org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        consumer.setKafkaBootstrapServers("localhost:9092");

        consumer.initializeProperties();
        TestCallBack callBack = new TestCallBack();

        consumer.addCallback(callBack, 0);
        consumer.setDeviceStatusTopicName(deviceConnStatusTopic);
        consumer.setDmaConsumerPoll(TestConstants.THREAD_SLEEP_TIME_10);
        consumer.setDmaAutoOffsetReset("latest");
        consumer.setServiceName("TestService");
        consumer.startBackDoorKafkaConsumer();
    }

    /**
     * Test topic missing.
     */
    @Test(expected = RuntimeException.class)
    public void testTopicMissing() {
        DeviceStatusBackDoorKafkaConsumer consumer = new DeviceStatusBackDoorKafkaConsumer();
        consumer.setIgniteKeyTransformerImpl("org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        consumer.setKafkaBootstrapServers("localhost:9092");

        consumer.initializeProperties();
        TestCallBack callBack = new TestCallBack();

        consumer.addCallback(callBack, 0);
        consumer.setDmaConsumerGroupId((i + "testBackDoorGroupId"));
        consumer.setDmaConsumerPoll(TestConstants.THREAD_SLEEP_TIME_10);
        consumer.setDmaAutoOffsetReset("latest");
        consumer.setServiceName("TestService");
        consumer.startBackDoorKafkaConsumer();
    }

    /**
     * Test when no call backs registered.
     */
    @Test
    public void testWhenNoCallBacksRegistered() {
        consumer.setIgniteKeyTransformerImpl("org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        consumer.setKafkaBootstrapServers("localhost:9092");
        consumer.initializeProperties();
        consumer.setDeviceStatusTopicName(deviceConnStatusTopic);
        consumer.setDmaConsumerGroupId((i + "testBackDoorGroupId"));
        consumer.setDmaConsumerPoll(TestConstants.THREAD_SLEEP_TIME_10);
        consumer.setDmaAutoOffsetReset("latest");
        consumer.setServiceName("TestService");
        consumer.startBackDoorKafkaConsumer();
        ConcurrentHashMap<Integer, BackdoorKafkaConsumerCallback> callbacks
                = (ConcurrentHashMap<Integer, BackdoorKafkaConsumerCallback>) ReflectionTestUtils
                .getField(consumer, "callBackMap");
        Assert.assertEquals(0, callbacks.size());
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
