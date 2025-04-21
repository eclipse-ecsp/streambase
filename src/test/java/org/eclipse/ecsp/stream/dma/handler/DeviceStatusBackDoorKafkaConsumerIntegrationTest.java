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

package org.eclipse.ecsp.stream.dma.handler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaConsumerCallback;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * class DeviceStatusBackDoorKafkaConsumerIntegrationTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/dma-handler-test.properties")
public class DeviceStatusBackDoorKafkaConsumerIntegrationTest extends KafkaStreamsApplicationTestBase {

    /** The device status back door kafka consumer. */
    @Autowired
    DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;

    /** The device status topic. */
    private String deviceStatusTopic;

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /**
     * setUp.
     *
     * @throws Exception Exception
     */
    @Before
    public void setup() throws Exception {
        super.setup();
        deviceStatusTopic = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName.toLowerCase();
        createTopics(deviceStatusTopic);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());

    }

    /**
     * Test kafka consumer client.
     *
     * @throws TimeoutException the timeout exception
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testKafkaConsumerClient() throws TimeoutException, ExecutionException, InterruptedException {
        TestCallBack callBack = new TestCallBack();
        deviceStatusBackDoorKafkaConsumer.addCallback(callBack, 0);

        Properties kafkaConsumerProps = deviceStatusBackDoorKafkaConsumer.getKafkaConsumerProps();
        kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        deviceStatusBackDoorKafkaConsumer.startBackDoorKafkaConsumer();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        Assert.assertTrue(deviceStatusBackDoorKafkaConsumer.isHealthy(true));
        String speedEvent = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
              + "\"Data\": {\"serviceName\":\"ecall\",\"connStatus\":\"ACTIVE\"},\"MessageId\": \"1235\","
               + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\",\"BizTransactionId\": \"Biz1234\"}";
        String deviceId = "12345";
        KafkaTestUtils.sendMessages(deviceStatusTopic, producerProps, deviceId.getBytes(), speedEvent.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        Assert.assertEquals(deviceId, callBack.getKey());
        Assert.assertEquals("DeviceConnStatus", callBack.getValue().getEventId());
        Assert.assertEquals("1.0", callBack.getValue().getVersion().getValue());
        Assert.assertEquals("DeviceConnStatusV1_0 [serviceName=ecall, connStatus=ACTIVE]", 
             callBack.getValue().getEventData().toString());
        Assert.assertEquals("1235", callBack.getValue().getMessageId());
        Assert.assertEquals("Vehicle12345", callBack.getValue().getVehicleId());
        Assert.assertEquals("Biz1234", callBack.getValue().getBizTransactionId());
        deviceStatusBackDoorKafkaConsumer.shutdown();
        Assert.assertFalse(deviceStatusBackDoorKafkaConsumer.isHealthy(false));
        Assert.assertFalse(deviceStatusBackDoorKafkaConsumer.isHealthy(true));
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
