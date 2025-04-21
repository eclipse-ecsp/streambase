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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.test.TestUtils;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Optional;



/**
 * class {@link MqttDispatcherHealthMontiorIntegrationTest}:  UT class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/mqtt-health-monitor.properties")
public class MqttDispatcherHealthMontiorIntegrationTest {

    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The Constant REDIS_SERVER. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS_SERVER = new EmbeddedRedisServer();
    
    /** The Constant LOGGER. */
    private static final IgniteLogger LOGGER = IgniteLoggerFactory
            .getLogger(MqttDispatcherHealthMontiorIntegrationTest.class);

    /** The paho mqtt dispatcher. */
    @Autowired
    PahoMqttDispatcher pahoMqttDispatcher;

    /** The default mqtt topic name generator impl. */
    @Autowired
    private DefaultMqttTopicNameGeneratorImpl defaultMqttTopicNameGeneratorImpl;
    
    /** The mqtt dispatcher. */
    @Autowired
    MqttDispatcher mqttDispatcher;
    
    /** The client. */
    MqttClient client;
    
    /** The msg received. */
    boolean msgReceived = false;
    
    /** The mqtt topic. */
    private String mqttTopic = StringUtils.EMPTY;
    
    /** The forced check value. */
    private DeviceMessage forcedCheckValue;
    
    /** The forced check key. */
    private IgniteStringKey forcedCheckKey;

    /**
     * Setup class.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @BeforeClass
    public static void setupClass() throws IOException {
        TestUtils.startMqttServer();
    }

    /**
     * Teardown class.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @AfterClass
    public static void teardownClass() throws IOException {
        TestUtils.startMqttServer();
    }

    /**
     * setup().
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
        defaultMqttTopicNameGeneratorImpl.setTopicNamePrefix("haa/harman/dev/");

    }

    /**
     * Test mqtt health monitor integration.
     *
     * @throws MqttException the mqtt exception
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testMqttHealthMonitorIntegration() throws MqttException, InterruptedException {
        MqttClient client = pahoMqttDispatcher.getMqttClient(PropertyNames.DEFAULT_PLATFORMID).get();
        String mqttTopicToSubscribe = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(forcedCheckKey, 
                forcedCheckValue.getDeviceMessageHeader(), null).get();
        client.subscribe(mqttTopicToSubscribe);
        client.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                LOGGER.error("Msg received:{} on topic:{}", message, topic);
                msgReceived = true;
                mqttTopic = topic;
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }

            @Override
            public void connectionLost(Throwable cause) {

            }
        });

        Assert.assertEquals(true, mqttDispatcher.isHealthy(false));
        RetryUtils.retry(Constants.TWENTY, (v) -> {
            return mqttTopic.length() > 0 ? Boolean.TRUE : null;
        });
        Assert.assertEquals(false, msgReceived);
        Assert.assertEquals("", mqttTopic);
        Assert.assertEquals(true, mqttDispatcher.isHealthy(true));
        RetryUtils.retry(Constants.TWENTY, (v) -> {
            return mqttTopic.length() > 0 ? Boolean.TRUE : null;
        });

        Assert.assertEquals(true, mqttDispatcher.isHealthy(false));
        Assert.assertEquals("haa/harman/dev/testDevice123/2d/test", mqttTopic);
        Assert.assertEquals(true, msgReceived);

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