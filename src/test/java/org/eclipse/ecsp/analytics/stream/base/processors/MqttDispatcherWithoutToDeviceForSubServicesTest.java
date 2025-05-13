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

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.DefaultMqttTopicNameGeneratorImpl;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
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



/**
 * Test class to test the MqttDispatcher class functionality.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/test-mqtt-sub-services.properties")
public class MqttDispatcherWithoutToDeviceForSubServicesTest {
    
    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The Constant REDIS_SERVER. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS_SERVER = new EmbeddedRedisServer();
    
    /** The value. */
    private DeviceMessage value;
    
    /** The default mqtt topic name generator impl. */
    @Autowired
    private DefaultMqttTopicNameGeneratorImpl defaultMqttTopicNameGeneratorImpl;
    
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
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withTargetDeviceId(event.getTargetDeviceId().get());
        value.setDeviceMessageHeader(header);
    }

    /**
     * Test with custom mqtttopic for sub services.
     */
    @Test
    public void testWithCustomMqtttopicForSubServices() {
        TestEvent event = new TestEvent();
        event.setDevMsgTopicSuffix("CUSTOM/TOPIC");
        event.setDevMsgTopicPrefix("userId/");
        value = new DeviceMessage(transformer.toBlob(event), Version.V1_0, event,
                "feedBackTopic", Constants.THREAD_SLEEP_TIME_60000);

        defaultMqttTopicNameGeneratorImpl.setTopicNamePrefix("userId/");
        Optional<String> mqttTopic = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(new TestKey(), 
                value.getDeviceMessageHeader(), null);
        String mqttExpectedTopic = "userId/device123/CUSTOM/TOPIC";
        Assert.assertEquals(mqttExpectedTopic, mqttTopic.get());

        event = new TestEvent();
        event.setDevMsgTopicSuffix("/custom/topic");
        value = new DeviceMessage(transformer.toBlob(event), Version.V1_0, event,
                "feedBackTopic", Constants.THREAD_SLEEP_TIME_60000);

        // Test whether the first occuring "/" is being removed and not
        // consecutive ones
        mqttTopic = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(new TestKey(), value.getDeviceMessageHeader(),
                null);
        mqttExpectedTopic = "userId/device123/CUSTOM/TOPIC";
        Assert.assertEquals(mqttExpectedTopic, mqttTopic.get());
    }

    /**
     * inner class TestKey implements IgniteKey.
     */
    public class TestKey implements IgniteKey<String> {

        /**
         * Gets the key.
         *
         * @return the key
         */
        @Override
        public String getKey() {

            return "device123";
        }

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
            return "testservice";
        }

        /**
         * Gets the target device id.
         *
         * @return the target device id
         */
        @Override
        public Optional<String> getTargetDeviceId() {
            return Optional.of("device123");
        }

    }

}
