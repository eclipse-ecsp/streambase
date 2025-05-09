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
import org.eclipse.ecsp.key.IgniteStringKey;
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
 * test class HiveMQMqttDispatcherHealthMontiorIntegrationTest.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/hivemq-mqtt-health-monitor.properties")
public class HiveMQMqttDispatcherHealthMontiorIntegrationTest {
    
    /** The Constant LOGGER. */
    private static final IgniteLogger LOGGER = IgniteLoggerFactory
            .getLogger(HiveMQMqttDispatcherHealthMontiorIntegrationTest.class);

    /** The Constant MQTT_SERVER. */
    @ClassRule
    public static final MqttServer MQTT_SERVER = new MqttServer();

    /** The mqtt dispatcher. */
    @Autowired
    MqttDispatcher mqttDispatcher;

    /** The default mqtt topic name generator impl. */
    @Autowired
    private DefaultMqttTopicNameGeneratorImpl defaultMqttTopicNameGeneratorImpl;
    
    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The Constant REDIS_SERVER. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS_SERVER = new EmbeddedRedisServer();
    
    /** The msg received. */
    boolean msgReceived = false;

    /** The mqtt topic. */
    private String mqttTopic = StringUtils.EMPTY;

    /** The forced check value. */
    private DeviceMessage forcedCheckValue;

    /** The forced check key. */
    private IgniteStringKey forcedCheckKey;

    /** The subscribe mqtt client. */
    private Mqtt3AsyncClient subscribeMqttClient;

    /**
     * setup():  to initialize the properties.
     */
    @Before
    public void setup() {

        forcedCheckKey = new IgniteStringKey();
        forcedCheckKey.setKey(Constants.FORCED_HEALTH_CHECK_DEVICE_ID);
        forcedCheckValue = new DeviceMessage();
        forcedCheckValue.setMessage("forcedHealthCheckDummyMsg".getBytes());
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withTargetDeviceId(Constants.FORCED_HEALTH_CHECK_DEVICE_ID);
        forcedCheckValue.setDeviceMessageHeader(header);
        defaultMqttTopicNameGeneratorImpl.setTopicNamePrefix("haa/custom/dev/");
        subscribeMqttClient = MqttClient.builder().identifier(UUID.randomUUID().toString())
                .serverHost("localhost").serverPort(Constants.INT_1883)
                .useMqttVersion3().automaticReconnect(MqttClientAutoReconnectImpl.DEFAULT)
                .buildAsync();
    }

    /**
     * Test mqtt health monitor integration.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testMqttHealthMonitorIntegration() throws InterruptedException {
        String mqttTopicToSubscribe = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(forcedCheckKey, 
                forcedCheckValue.getDeviceMessageHeader(), null).get();
        subscribeMqttClient.connectWith().cleanSession(false).send();
        subscribeMqttClient.subscribeWith()
                .topicFilter(mqttTopicToSubscribe)
                .callback(callback -> {
                    LOGGER.error("Msg received:{} on topic:{}", callback.getPayload(), callback.getTopic());
                    msgReceived = true;
                    mqttTopic = callback.getTopic().toString();
                }).send();

        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        Assert.assertEquals(false, mqttDispatcher.isHealthy(false));
        RetryUtils.retry(Constants.TWENTY, (v) -> {
            return mqttTopic.length() > 0 ? Boolean.TRUE : null;
        });
        Assert.assertEquals(false, msgReceived);
        Assert.assertEquals("", mqttTopic);
        Assert.assertEquals(true, mqttDispatcher.isHealthy(true));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        RetryUtils.retry(Constants.TWENTY, (v) -> {
            return mqttTopic.length() > 0 ? Boolean.TRUE : null;
        });
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        Assert.assertEquals(true, mqttDispatcher.isHealthy(false));
        Assert.assertEquals("haa/custom/dev/testDevice123/2d/test", mqttTopic);
        Assert.assertEquals(true, msgReceived);
        mqttDispatcher.close();
        subscribeMqttClient = null;
    }

    /**
     * inner class TestEvent extends IgniteEventImpl.
     */
    public class TestEvent extends IgniteEventImpl {

        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /**
         * Instantiates a new test event.
         */
        public TestEvent() {
            // Nothing to do.
        }

        /**
         * Gets the event id.
         *
         * @return the event id
         */
        @Override
        public String getEventId() {
            return "testHealthMonitorService";
        }

        /**
         * Gets the target device id.
         *
         * @return the target device id
         */
        @Override
        public Optional<String> getTargetDeviceId() {
            return Optional.of("testDevice123");
        }

    }

}