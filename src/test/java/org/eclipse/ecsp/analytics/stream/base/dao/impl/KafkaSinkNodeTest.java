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

package org.eclipse.ecsp.analytics.stream.base.dao.impl;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.SimplePropertiesLoader;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;



/**
 * Test class to verify the functionalities of KafkaSinkNodeTest class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/integration-test-application.properties")
/*@org.junit.experimental.categories.Category(NightlyBuildTestCase.class)*/
public class KafkaSinkNodeTest extends KafkaStreamsApplicationTestBase {

    /** The Constant SOURCE_TOPIC_NAME. */
    private static final String SOURCE_TOPIC_NAME = "KafkaSinkNodeTestSourceTopic";

    /** The Constant MESSAGE_KEY. */
    private static final String MESSAGE_KEY = "testKey";
    
    /** The Constant PAYLOAD. */
    private static final String PAYLOAD = "testMessage";

    /** The kafka sink node. */
    private final KafkaSinkNode kafkaSinkNode = new KafkaSinkNode();

    /** The properties. */
    private Properties properties;

    /**
     * setup().
     *
     * @throws Exception Exception
     */
    @Before
    public void setup() throws Exception {
        super.setup();
        createTopics(SOURCE_TOPIC_NAME);

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "ksn-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());

        SimplePropertiesLoader spl = new SimplePropertiesLoader();
        properties = spl.loadProperties(getClass().getClassLoader()
                .getResource("application-base-test.properties").getPath());
        properties.put(PropertyNames.BOOTSTRAP_SERVERS,
                KAFKA_CLUSTER.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
    }

    /**
     * Publising message to kafka and consume the same.
     *
     * @throws TimeoutException TimeoutException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testPut() throws TimeoutException, InterruptedException {
        properties.put(PropertyNames.KAFKA_DEVICE_EVENTS_ASYNC_PUTS, "true");
        kafkaSinkNode.init(properties);
        kafkaSinkNode.put(MESSAGE_KEY.getBytes(), PAYLOAD.getBytes(), SOURCE_TOPIC_NAME, "");
        kafkaSinkNode.flush();
        List<String[]> allMessages = KafkaTestUtils.getMessages(SOURCE_TOPIC_NAME,
                consumerProps, 1, Constants.THREAD_SLEEP_TIME_1000);
        boolean flag = false;

        for (String[] keyMessage : allMessages) {
            if (keyMessage.length == Constants.TWO) {
                if (MESSAGE_KEY.equals(keyMessage[0])
                        && PAYLOAD.equals(keyMessage[1])) {
                    flag = true;
                    break;
                }
            }
        }
        assertTrue("Dint get message which is sent", flag);
    }

    /**
     * Publising message to kafka usin when ssl enabled and consume the same.
     */
    public void testInitWithSSLEnabled() {
        String password = "password";
        String keystore = "src/test/resources/kafka.client.keystore.jks";
        String truststore = "src/test/resources/kafka.client.truststore.jks";
        String sslClientAuth = "required";
        properties.put(PropertyNames.KAFKA_CLIENT_KEYSTORE, keystore);
        properties.put(PropertyNames.KAFKA_CLIENT_KEYSTORE_PASSWORD, password);
        properties.put(PropertyNames.KAFKA_CLIENT_KEY_PASSWORD, password);
        properties.put(PropertyNames.KAFKA_CLIENT_TRUSTSTORE, truststore);
        properties.put(PropertyNames.KAFKA_CLIENT_TRUSTSTORE_PASSWORD, password);
        properties.put(PropertyNames.KAFKA_SSL_CLIENT_AUTH, sslClientAuth);
        properties.put(PropertyNames.KAFKA_SSL_ENABLE, true);
        kafkaSinkNode.init(properties);
        assertEquals("SSL", properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), 
                "Expected protocol set to SSL");
    }
    

    /**
     * Clean.
     */
    @After
    public void clean() {
        kafkaSinkNode.close();
    }

}
