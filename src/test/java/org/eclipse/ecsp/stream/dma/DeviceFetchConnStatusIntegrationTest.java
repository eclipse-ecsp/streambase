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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KeyValue;
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
import org.eclipse.ecsp.domain.FetchConnectionStatusEventData;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.handler.DeviceConnectionStatusHandler;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.eclipse.ecsp.transform.GenericIgniteEventTransformer;
import org.junit.After;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNull;


/**
 * Integration test case for "Fetching connection status of a device using presence-manager through kafka topic"
 * use case in stream-base.
 *
 * @author karora
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-handler-fetch-conn-status-test.properties")
public class DeviceFetchConnStatusIntegrationTest extends KafkaStreamsApplicationTestBase {

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
    
    /** The fetch conn status topic. */
    @Value("${fetch.connection.status.topic.name}")
    private String fetchConnStatusTopic;
    
    /** The device service. */
    @Autowired
    private DeviceStatusService deviceService;
    
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
     * Setup for this test case.
     *
     * @throws Exception exception
     */
    @Before
    public void setUp() throws Exception {
        super.setup();
        deviceStatusTopicName = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName.toLowerCase();
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-sp-consumer-group");
        createTopics(sourceTopic, deviceStatusTopicName, fetchConnStatusTopic);
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
     * Test fetch connection status event.
     *
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     * @throws TimeoutException the timeout exception
     * @throws JsonParseException the json parse exception
     * @throws JsonMappingException the json mapping exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test
    public void testFetchConnectionStatusEvent() throws ExecutionException, InterruptedException, 
        TimeoutException, JsonParseException, JsonMappingException, IOException {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        
        String deviceInactive = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"INACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(deviceStatusTopicName, producerProps, Arrays.asList(vehicleId.getBytes(), 
                deviceInactive.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        assertNull(deviceService.get(vehicleId, Optional.empty()));
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0},"
                + "\"MessageId\": \"1234\",\"CorrelationId\": \"1234\",\"BizTransactionId\": \"Biz1234\","
                + "\"Timezone\": \"60\",\"PlatformId\": \"Platform1\",\"VehicleId\": \"Vehicle12345\","
                + "\"SourceDeviceId\": \"12345\"}";
        sendMessages(sourceTopic, producerProps, Arrays.asList(vehicleId.getBytes(), speedEvent.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        
        List<KeyValue<byte[], byte[]>> receivedRecords = getKeyValueRecords(fetchConnStatusTopic, 
                consumerProps, TestConstants.SEVEN, TestConstants.INT_60000);
        IgniteEvent speedIgniteEvent = getIgniteEvent(receivedRecords.get(0).value);
        Assert.assertNotNull(speedIgniteEvent);
        Assert.assertEquals("Timezone in event received is not as expected", (short) TestConstants.INT_60, 
                speedIgniteEvent.getTimezone());
        Assert.assertEquals("EventID in event received is not as expected", EventID.FETCH_CONN_STATUS, 
                speedIgniteEvent.getEventId());
        FetchConnectionStatusEventData fetchConnStatusData = 
                (FetchConnectionStatusEventData) speedIgniteEvent.getEventData();
        Assert.assertEquals("PlatformID in FetchConnectionStatusEventData is not as expected", "Platform1", 
                fetchConnStatusData.getPlatformId());
        Assert.assertEquals("VehicleID in FetchConnectionStatusEventData is not as expected", "Vehicle12345", 
                fetchConnStatusData.getVehicleId());
    }
    
    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        deviceStatusBackDoorKafkaConsumer.shutdown();
    }
    
    /**
     * Gets the ignite event.
     *
     * @param eventData the event data
     * @return the ignite event
     * @throws JsonParseException the json parse exception
     * @throws JsonMappingException the json mapping exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private IgniteEvent getIgniteEvent(byte[] eventData) throws JsonParseException, JsonMappingException, IOException {
        GenericIgniteEventTransformer eventTransformer = new GenericIgniteEventTransformer();
        return eventTransformer.fromBlob(eventData, Optional.empty());
    }
    
    /**
     * Test stream processor class.
     */
    public static class FetchConnStatusTestStreamProcessor implements IgniteEventStreamProcessor {
        
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
            // todo Auto-generated method stub

        }

        /**
         * Close.
         */
        @Override
        public void close() {
            // todo Auto-generated method stub

        }

        /**
         * Config changed.
         *
         * @param props the props
         */
        @Override
        public void configChanged(Properties props) {
            // todo Auto-generated method stub

        }

        /**
         * Creates the state store.
         *
         * @return the harman persistent KV store
         */
        @Override
        public HarmanPersistentKVStore createStateStore() {
            // todo Auto-generated method stub
            return null;
        }

    }

}
