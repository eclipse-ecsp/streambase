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

package org.eclipse.ecsp.stream.dma;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.handler.DeviceConnectionStatusHandler;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;



/**
 * class DMOfflineBufferIntegrationTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@TestPropertySource("/dma-offline-test.properties")
public class DMOfflineBufferIntegrationTest extends KafkaStreamsApplicationTestBase {
    
    /** The service name. */
    @Value("${service.name}")
    private String serviceName;
    
    /** The source topic. */
    @Value("${source.topic.name}")
    private String sourceTopic;
    
    /** The mqtt prefix. */
    @Value("${mqtt.service.topic.name.prefix}")
    private String mqttPrefix;
    
    /** The to device. */
    @Value("${" + PropertyNames.MQTT_TOPIC_TO_DEVICE_INFIX + ":" + Constants.TO_DEVICE + "}")
    private String toDevice;
    
    /** The mqtt topic. */
    @Value("${mqtt.service.topic.name}")
    private String mqttTopic;
    
    /** The device service. */
    @Autowired
    private DeviceStatusService deviceService;

    /** The offline buffer DAO. */
    @Autowired
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;
    
    /** The device status back door kafka consumer. */
    @Autowired
    DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;
    
    /** The device connection status handler. */
    @Autowired
    DeviceConnectionStatusHandler deviceConnectionStatusHandler;

    /** The device status topic name. */
    private String deviceStatusTopicName;
    
    /** The vehicle id. */
    private String vehicleId = "Vehicle12345";

    /**
     * setUp().
     *
     * @throws Exception setUp()
     * @throws MqttException setUp()
     */
    @Before
    public void init() throws Exception, MqttException {
        super.setup();
        deviceStatusTopicName = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName.toLowerCase();
        createTopics(sourceTopic, deviceStatusTopicName);
        Properties kafkaConsumerProps = deviceStatusBackDoorKafkaConsumer.getKafkaConsumerProps();
        kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        deviceStatusBackDoorKafkaConsumer.addCallback(deviceConnectionStatusHandler.new DeviceStatusCallBack(), 0);
        deviceStatusBackDoorKafkaConsumer.startBackDoorKafkaConsumer();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        launchApplication();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        subscibeToMqttTopic(mqttPrefix + "12345" + toDevice + "/" + mqttTopic);
    }

    /**
     * Test offliner buffer removals on device active.
     *
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     * @throws TimeoutException the timeout exception
     */
    @Test
    public void testOfflinerBufferRemovalsOnDeviceActive() 
           throws ExecutionException, InterruptedException, TimeoutException {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        String deviceInactive = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\",\"Data\": "
                + "{\"connStatus\":\"INACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\",\"VehicleId\": "
                + "\"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(deviceStatusTopicName, producerProps,
                Arrays.asList(vehicleId.getBytes(), deviceInactive.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        assertNull(deviceService.get(vehicleId, Optional.empty()));
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0},"
                + "\"MessageId\": \"1234\",\"CorrelationId\": \"1234\",\"BizTransactionId\": \"Biz1234\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(sourceTopic, producerProps,
                Arrays.asList(vehicleId.getBytes(), speedEvent.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        List<DMOfflineBufferEntry> bufferEntries = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, 
                false, Optional.empty(), Optional.empty());
        assertEquals("Expected one entry", 1, bufferEntries.size());
        String deviceActive = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\",\"Data\": "
                + "{\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\",\"VehicleId\": "
                + "\"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(deviceStatusTopicName, producerProps,
                Arrays.asList(vehicleId.getBytes(), deviceActive.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        List<DMOfflineBufferEntry> bufferEntries2 = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, 
                false, Optional.empty(), Optional.empty());
        assertEquals("Expected 0 entry", 0, bufferEntries2.size());
        String completeMqttTopic = mqttPrefix + "12345" + toDevice + "/" + mqttTopic;
        List<byte[]> messages = getMessagesFromMqttTopic(completeMqttTopic, 1, Constants.THREAD_SLEEP_TIME_60000);
        assertEquals("No of message expected", 1, messages.size());
    }
    
    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        deviceStatusBackDoorKafkaConsumer.shutdown();
    }

    /**
     * Test device id not matched.
     *
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     * @throws TimeoutException the timeout exception
     */
    @Test
    public void testDeviceIdNotMatched() throws ExecutionException, InterruptedException, TimeoutException {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        String deviceInactive = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\",\"Data\": "
                + "{\"connStatus\":\"INACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\",\"VehicleId\": "
                + "\"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(deviceStatusTopicName, producerProps,
                Arrays.asList(vehicleId.getBytes(), deviceInactive.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        assertNull(deviceService.get(vehicleId, Optional.empty()));
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": "
                + "{\"value\":20.0},\"MessageId\": \"1234\",\"CorrelationId\": \"1234\",\"BizTransactionId\":"
                + " \"Biz1234\",\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(sourceTopic, producerProps,
                Arrays.asList(vehicleId.getBytes(), speedEvent.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0},"
                + "\"MessageId\": \"12345\",\"CorrelationId\": \"12345\",\"BizTransactionId\": \"Biz12345\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"123456\"}";
        deviceInactive = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"INACTIVE\",\"serviceName\":\"eCall\"},"
                + "\"MessageId\": \"12345\",\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"123456\"}";
        sendMessages(deviceStatusTopicName, producerProps,
                Arrays.asList(vehicleId.getBytes(), deviceInactive.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        assertNull(deviceService.get(vehicleId, Optional.empty()));
        sendMessages(sourceTopic, producerProps,
                Arrays.asList(vehicleId.getBytes(), speedEvent.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        List<DMOfflineBufferEntry> bufferEntries = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, 
                false, Optional.empty(), Optional.empty());
        assertEquals("Expected two entry", TestConstants.TWO, bufferEntries.size());
        String deviceActive = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},"
                + "\"MessageId\": \"1234\",\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(deviceStatusTopicName, producerProps,
                Arrays.asList(vehicleId.getBytes(), deviceActive.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        List<DMOfflineBufferEntry> bufferEntries2 = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, 
                false, Optional.empty(), Optional.empty());
        assertEquals("Expected 1 entry", 1, bufferEntries2.size());
        String completeMqttTopic = mqttPrefix + "12345" + toDevice + "/" + mqttTopic;
        List<byte[]> messages = getMessagesFromMqttTopic(completeMqttTopic, 1, Constants.THREAD_SLEEP_TIME_60000);
        assertEquals("No of message expected", 1, messages.size());
    }

    /**
     * inner class  {@link DMOfflineBufferTestStreamProcessor} implements IgniteEventStreamProcessor.
     */
    public static class DMOfflineBufferTestStreamProcessor implements IgniteEventStreamProcessor {
        
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
            return "dma-sp";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            IgniteEvent value = kafkaRecord.value();
            if (!value.getEventId().equals(EventID.DEVICEMESSAGEFAILURE)) {
                ((AbstractIgniteEvent) value).setDeviceRoutable(true);
                kafkaRecord.withValue(value);
                spc.forward(kafkaRecord);
            }
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
    }
}
