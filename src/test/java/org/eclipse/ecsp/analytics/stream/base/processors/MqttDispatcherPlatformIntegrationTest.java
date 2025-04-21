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

package org.eclipse.ecsp.analytics.stream.base.processors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.test.TestUtils;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.DefaultMqttTopicNameGeneratorImpl;
import org.eclipse.ecsp.analytics.stream.base.utils.MqttConfig;
import org.eclipse.ecsp.analytics.stream.base.utils.MqttDispatcher;
import org.eclipse.ecsp.analytics.stream.base.utils.PahoMqttDispatcher;
import org.eclipse.ecsp.analytics.stream.base.utils.RetryUtils;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;



/**
 * Test class to test the MqttDispatcher class functionality.
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/test-mqtt-platform.properties")
public class MqttDispatcherPlatformIntegrationTest {
    
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttDispatcherPlatformIntegrationTest.class);

    /** The msg received. */
    boolean msgReceived = false;

    /** The mqtt topic. */
    private String mqttTopic = StringUtils.EMPTY;

    /** The default mqtt topic name generator impl. */
    @Autowired
    private DefaultMqttTopicNameGeneratorImpl defaultMqttTopicNameGeneratorImpl;
    
    /** The mqtt dispatcher. */
    @Autowired
    private MqttDispatcher mqttDispatcher;

    /** The paho mqtt dispatcher. */
    @Autowired
    private PahoMqttDispatcher pahoMqttDispatcher;

    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();

    /** The Constant REDIS_SERVER. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS_SERVER = new EmbeddedRedisServer();

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
     */
    @AfterClass
    public static void teardownClass() {
        TestUtils.stopMqttServer();
    }

    /** The value. */
    private DeviceMessage value;

    /** The transformer. */
    @Autowired
    private DeviceMessageIgniteEventTransformer transformer;
    
    /** The platform ID. */
    private String platformID;
    
    /**
     * Create setup for this integration test case.
     */
    @Before
    public void setup() {
        platformID = "platform1";
        TestEvent event = new TestEvent();
        value = new DeviceMessage();
        value.setMessage(transformer.toBlob(event));
        value.setEvent(event);
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withTargetDeviceId("test");
        value.setDeviceMessageHeader(header);
    }

    /**
     * Test client connection with multiple platforms.
     *
     * @throws InterruptedException the interrupted exception
     * @throws MqttException the mqtt exception
     */
    @Test
    public void testClientConnectionWithMultiplePlatforms() throws InterruptedException, MqttException {
        defaultMqttTopicNameGeneratorImpl.setTopicNamePrefix("haa/harman/dev/");
        TestKey key = new TestKey();

        Optional<MqttConfig> configForPlatformOpt = pahoMqttDispatcher.getMqttConfig(platformID);
        Optional<MqttConfig> configForDefaultPlatformOpt = pahoMqttDispatcher
                .getMqttConfig(PropertyNames.DEFAULT_PLATFORMID);
        
        Assert.assertTrue("Config for platformID is not present", configForPlatformOpt.isPresent());
        Assert.assertTrue("Config for default platformID is not present", configForDefaultPlatformOpt.isPresent());
        Assert.assertNotNull("Broker URL for platform config is null", configForPlatformOpt.get().getBrokerUrl());
        Assert.assertNotNull("Broker URL for default platform config is null", 
                configForDefaultPlatformOpt.get().getBrokerUrl());
        /*
         * get a client to subscribe to the required topic topic
         */
        String mqttTopicToSubscribe = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(key, 
                value.getDeviceMessageHeader(), null).get();
        MqttClient client = pahoMqttDispatcher.getMqttClient(platformID).get();
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
        mqttDispatcher.dispatch(key, value);
        await().atMost(TestConstants.THREAD_SLEEP_TIME_1000, TimeUnit.MILLISECONDS);
        RetryUtils.retry(TestConstants.TWENTY, (v) -> {
            return mqttTopic.length() > 0 ? Boolean.TRUE : null;
        });
        MqttClient client2 = pahoMqttDispatcher.getMqttClient(PropertyNames.DEFAULT_PLATFORMID).get();
        Assert.assertNotNull("Client for platform is null", client);
        Assert.assertNotNull("Client for default platform is null", client2);
        Assert.assertEquals("haa/harman/dev/test/2d/test", mqttTopic);
        Assert.assertEquals(true, msgReceived);
    }
    
    /**
     * Test IgniteKey implementation.
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
     * Test IgniteEvent.
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