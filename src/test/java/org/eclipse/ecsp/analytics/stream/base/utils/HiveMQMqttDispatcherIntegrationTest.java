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

import com.hivemq.client.internal.mqtt.lifecycle.MqttClientAutoReconnectImpl;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.mqtt.MqttServer;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Optional;
import java.util.UUID;



/**
 * Test class to test the MqttDispatcher class functionality.
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/hivemq-test-mqtt-without-topic-prefix.properties")
public class HiveMQMqttDispatcherIntegrationTest {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(HiveMQMqttDispatcherIntegrationTest.class);
    
    /** The msg received. */
    boolean msgReceived = false;
    
    /** The mqtt topic. */
    private String mqttTopic = StringUtils.EMPTY;
    
    /** The mqtt dispatcher. */
    @Autowired
    private MqttDispatcher mqttDispatcher;
    
    /** The default mqtt topic name generator impl. */
    @Autowired
    private DefaultMqttTopicNameGeneratorImpl defaultMqttTopicNameGeneratorImpl;

    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();

    /** The Constant REDIS_SERVER. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS_SERVER = new EmbeddedRedisServer();

    /** The Constant MQTT_SERVER. */
    @ClassRule
    public static final MqttServer MQTT_SERVER = new MqttServer();

    /** The value. */
    private DeviceMessage value;

    /** The subscribe mqtt client. */
    private Mqtt3AsyncClient subscribeMqttClient;

    /** The transformer. */
    @Autowired
    private DeviceMessageIgniteEventTransformer transformer;

    /**
     * setup().
     */
    @Before
    public void setup() {
        TestEvent event = new TestEvent();
        value = new DeviceMessage();
        value.setMessage(transformer.toBlob(event));
        value.setEvent(event);
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withTargetDeviceId("test");
        value.setDeviceMessageHeader(header);
        subscribeMqttClient = MqttClient.builder().identifier(UUID.randomUUID().toString())
                .serverHost("localhost").serverPort(Constants.INT_1883)
                .useMqttVersion3().automaticReconnect(MqttClientAutoReconnectImpl.DEFAULT)
                .buildAsync();
    }

    /**
     * Test client connection without topic pefix.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testClientConnection_without_topic_pefix() throws InterruptedException {
        defaultMqttTopicNameGeneratorImpl.setTopicNamePrefix("");
        TestKey key = new TestKey();
        String mqttTopicToSubscribe = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(key, 
                value.getDeviceMessageHeader(), null).get();
        subscribeMqttClient.connectWith().cleanSession(false).send();
        subscribeMqttClient.subscribeWith()
                .topicFilter(mqttTopicToSubscribe)
                .callback((publish) -> {
                    logger.info("Msg received:{} on topic:{}", publish.getPayload().get(), publish.getTopic());

                    msgReceived = true;
                    mqttTopic = publish.getTopic().toString();
                }).send();
        mqttDispatcher.dispatch(key, value);
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        RetryUtils.retry(TestConstants.TWENTY, (v) -> {
            return mqttTopic.length() > 0 ? Boolean.TRUE : null;
        });

        Assert.assertEquals("test/2d/test", mqttTopic);
        Assert.assertEquals(true, msgReceived);
        subscribeMqttClient = null;
        mqttDispatcher.close();
    }

    /**
     * class TestKey implements IgniteKey.
     */
    public class TestKey implements IgniteKey<String> {
        
        /**
         * Gets the key.
         *
         * @return the key
         */
        @Override
        public String getKey() {
            return "test";
        }
    }

    /**
     * class TestEvent extends IgniteEventImpl.
     */
    public class TestEvent extends IgniteEventImpl {
        
        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /**
         * Instantiates a new test event.
         */
        public TestEvent() {
        }

        /**
         * Gets the event id.
         *
         * @return the event id
         */
        @Override
        public String getEventId() {
            return "Sample";
        }

        /**
         * Gets the target device id.
         *
         * @return the target device id
         */
        @Override
        public Optional<String> getTargetDeviceId() {
            return Optional.of("test");
        }
    }

}