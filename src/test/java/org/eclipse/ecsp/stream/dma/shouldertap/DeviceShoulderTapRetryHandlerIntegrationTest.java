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

package org.eclipse.ecsp.stream.dma.shouldertap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
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
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.handler.DeviceConnectionStatusHandler;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.eclipse.ecsp.transform.GenericIgniteEventTransformer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;



/**
 * class DeviceShoulderTapRetryHandlerIntegrationTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-shouldertap-test.properties")
public class DeviceShoulderTapRetryHandlerIntegrationTest extends KafkaStreamsApplicationTestBase {
    
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceShoulderTapRetryHandlerIntegrationTest.class);
    
    /** The conn status topic. */
    private static String connStatusTopic;
    
    /** The source topic name. */
    private static String sourceTopicName;
    
    /** The i. */
    private static int i = 0;
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The max retry. */
    @Value("${" + PropertyNames.SHOULDER_TAP_MAX_RETRY + ":3}")
    private int maxRetry;
    
    /** The retry interval. */
    @Value("${" + PropertyNames.SHOULDER_TAP_RETRY_INTERVAL_MILLIS + ":60000}")
    private long retryInterval;
    
    /** The retry min threshold. */
    @Value("${" + PropertyNames.SHOULDER_TAP_RETRY_MIN_THRESHOLD_MILLIS + ":60000}")
    private long retryMinThreshold;
    
    /** The vehicle id. */
    private String vehicleId = "Vehicle12345";
    
    /** The device service. */
    @Autowired
    private DeviceStatusService deviceService;
    
    /** The shoulder tap invoker WAM impl. */
    @Autowired
    private ShoulderTapInvokerWAMImpl shoulderTapInvokerWAMImpl;
    
    /** The device status back door kafka consumer. */
    @Autowired
    DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;
    
    /** The device connection status handler. */
    @Autowired
    DeviceConnectionStatusHandler deviceConnectionStatusHandler;
    
    /** The web server. */
    @Rule
    public MockWebServer webServer = new MockWebServer();
    
    /** The thread delay. */
    long threadDelay;

    /**
     * setup().
     *
     * @throws Exception Exception
     */
    @Before
    public void setup() throws Exception {
        super.setup();
        i++;
        sourceTopicName = "sourceTopic" + i;
        connStatusTopic = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName.toLowerCase();
        createTopics(connStatusTopic, sourceTopicName);
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopicName);

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());

        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-sp-consumer-group");
        Properties kafkaConsumerProps = deviceStatusBackDoorKafkaConsumer.getKafkaConsumerProps();
        kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        deviceStatusBackDoorKafkaConsumer.addCallback(deviceConnectionStatusHandler.new DeviceStatusCallBack(), 0);
        deviceStatusBackDoorKafkaConsumer.startBackDoorKafkaConsumer();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMAShoulderTapServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        threadDelay = getScheduledThreadDelay();
    }
    
    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        deviceStatusBackDoorKafkaConsumer.shutdown();
    }

    /**
     * Test if shoulder tap msg delivered and device comes active then retry stops.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIfShoulderTapMsgDeliveredAndDeviceComesActiveThenRetryStops() throws Exception {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSUrl", "http://localhost:" + webServer.getPort() + "/");
        String transactionId = "f71e2395-eda2-4de9-ad0a-72e930111736";
        String shoulderTapSendSMSJsonResponse = "{\"message\": \"SUCCESS\",\"failureReasonCode\": null,"
                + "\"failureReason\": null,\"data\": {\"transactionId\": \""
                + transactionId + "\"}}";
        MockResponse mockShoulderTapResponse = getMockResponse(shoulderTapSendSMSJsonResponse);

        for (int index = 0; index < maxRetry + 1; index++) {
            webServer.enqueue(mockShoulderTapResponse);
        }

        String deviceConnInActiveStatusEvent = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\",\"Data\": "
                + "{\"connStatus\":\"INACTIVE\",\"serviceName\":\"ECall\"},\"MessageId\": \"1234\",\"VehicleId\": \""
                + vehicleId + "\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnInActiveStatusEvent.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        assertNull(deviceService.get(vehicleId, Optional.empty()));

        String messageId = "Message12345";
        String value = "20.0";
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":" + value
                + "},\"RequestId\":\"Request123\", \"MessageId\":\"" + messageId
                + "\",\"BizTransactionId\": \"Biz1237\",\"VehicleId\": \""
                + vehicleId + "\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(), speedEvent.getBytes());

        Thread.sleep(Constants.INT_45000);

        String deviceConnActiveStatusEvent = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\",\"Data\": "
                + "{\"connStatus\":\"ACTIVE\",\"serviceName\":\"ECall\"},\"MessageId\": \"1234\",\"VehicleId\": \""
                + vehicleId + "\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnActiveStatusEvent.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        assertNotNull(deviceService.get(vehicleId, Optional.empty()));

        // additional buffer of 120000 delay to cover up any time lag due to
        // kafka/retry thread.
        Thread.sleep(Constants.INT_120000 + (maxRetry * threadDelay));
        // Receive DeviceMessageFailureEventData
        List<KeyValue<byte[], byte[]>> receivedRecords =
                getKeyValueRecords(sourceTopicName, consumerProps, Constants.FOUR, Constants.THREAD_SLEEP_TIME_60000);

        // Assert no. of DeviceMessageFailureEventData:
        // 1) 1 speed event
        // 2) 1 failure event for device status inactive
        // 3) 1 failure event for first shoulder tap attempt
        // 4) 1 (1 retry attempt) failure event for shoulder tap retry
        assertEquals(Constants.FOUR, receivedRecords.size());

        // Assert speedEvent that was first sent on sourceTopic
        IgniteEvent speedIgniteEvent = getIgniteEvent(receivedRecords.get(0).value);
        assertEquals(messageId, speedIgniteEvent.getMessageId());
        assertEquals(vehicleId, speedIgniteEvent.getVehicleId());
        assertEquals(EventID.SPEED, speedIgniteEvent.getEventId());

        // Assert 1st DeviceMessageFailureEventData
        IgniteEvent firstDeviceMessageFailureEvent = getIgniteEvent(receivedRecords.get(1).value);
        assertEquals(EventID.DEVICEMESSAGEFAILURE, firstDeviceMessageFailureEvent.getEventId());

        DeviceMessageFailureEventDataV1_0 firstFailEventData =
                (DeviceMessageFailureEventDataV1_0) firstDeviceMessageFailureEvent.getEventData();
        assertEquals(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE, firstFailEventData.getErrorCode());
        assertEquals(0, firstFailEventData.getShoudlerTapRetryAttempts());
        assertEquals(true, firstFailEventData.isDeviceStatusInactive());

        // Assert failed DeviceMessage
        IgniteEvent failedIgniteEvent = firstFailEventData.getFailedIgniteEvent();
        assertEquals(messageId, failedIgniteEvent.getMessageId());
        assertEquals(vehicleId, failedIgniteEvent.getVehicleId());
        assertEquals(EventID.SPEED, failedIgniteEvent.getEventId());

        // Assert 4th DeviceMessageFailureEventData
        IgniteEvent fourthDeviceMessageFailureEvent = getIgniteEvent(receivedRecords.get(Constants.THREE).value);
        assertEquals(EventID.DEVICEMESSAGEFAILURE, fourthDeviceMessageFailureEvent.getEventId());
        
        DeviceMessageFailureEventDataV1_0 fourthFailEventData =
                (DeviceMessageFailureEventDataV1_0) fourthDeviceMessageFailureEvent.getEventData();
        assertEquals(DeviceMessageErrorCode.RETRYING_SHOULDER_TAP, fourthFailEventData.getErrorCode());
        assertEquals(1, fourthFailEventData.getShoudlerTapRetryAttempts());
        assertEquals(true, fourthFailEventData.isDeviceStatusInactive());
    }

    /**
     * Test if shoulder tap msg delivered and device remains in active then max retry is attempted.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIfShoulderTapMsgDeliveredAndDeviceRemainsInActiveThenMaxRetryIsAttempted() throws Exception {
        ReflectionTestUtils.setField(shoulderTapInvokerWAMImpl, "wamSendSMSUrl", "http://localhost:" + webServer.getPort() + "/");
        String transactionId = "f71e2395-eda2-4de9-ad0a-72e930111736";
        String shoulderTapSendSMSJsonResponse = "{\"message\": \"SUCCESS\",\"failureReasonCode\": null,"
                + "\"failureReason\": null,\"data\": {\"transactionId\": \""
                + transactionId + "\"}}";
        MockResponse mockShoulderTapResponse = getMockResponse(shoulderTapSendSMSJsonResponse);

        for (int index = 0; index < maxRetry + 1; index++) {

            webServer.enqueue(mockShoulderTapResponse);
        }

        String deviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\",\"Data\": "
                + "{\"connStatus\":\"INACTIVE\",\"serviceName\":\"ECall\"},\"MessageId\": \"1234\",\"VehicleId\": \""
                + vehicleId + "\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnStatusEvent.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        assertNull(deviceService.get(DMAConstants.VEHICLE_DEVICE_MAPPING + serviceName + vehicleId, Optional.empty()));

        String messageId = "Message12345";
        String value = "20.0";
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":" + value
                + "},\"RequestId\":\"Request123\", \"MessageId\":\""
                + messageId + "\",\"BizTransactionId\": \"Biz1237\",\"VehicleId\": \""
                + vehicleId + "\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(), speedEvent.getBytes());

        Thread.sleep(Constants.INT_20000);

        // additional buffer of 120000 delay to cover up any time lag due to
        // kafka/retry thread.
        Thread.sleep(Constants.INT_120000 + (maxRetry * threadDelay));
        // Receive DeviceMessageFailureEventData
        List<KeyValue<byte[], byte[]>> receivedRecords =
                getKeyValueRecords(sourceTopicName, consumerProps, Constants.SEVEN, Constants.THREAD_SLEEP_TIME_60000);

        // Assert no. of DeviceMessageFailureEventData:
        // 1) 1 speed event
        // 2) 1 failure event for device status inactive
        // 3) 1 failure event for first shoulder tap attempt
        // 4) 3 (maxRetry count) failure event for shoulder tap retry
        // 5) 1 failure event for shoulder tap retry attempt exceeded
        assertEquals(Constants.SEVEN, receivedRecords.size());

        // Assert speedEvent that was first sent on sourceTopic
        IgniteEvent speedIgniteEvent = getIgniteEvent(receivedRecords.get(0).value);
        assertEquals(messageId, speedIgniteEvent.getMessageId());
        assertEquals(vehicleId, speedIgniteEvent.getVehicleId());
        assertEquals(EventID.SPEED, speedIgniteEvent.getEventId());

        // Assert 1st DeviceMessageFailureEventData
        IgniteEvent firstDeviceMessageFailureEvent = getIgniteEvent(receivedRecords.get(1).value);
        assertEquals(EventID.DEVICEMESSAGEFAILURE, firstDeviceMessageFailureEvent.getEventId());

        DeviceMessageFailureEventDataV1_0 firstFailEventData =
                (DeviceMessageFailureEventDataV1_0) firstDeviceMessageFailureEvent.getEventData();
        assertEquals(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE, firstFailEventData.getErrorCode());
        assertEquals(0, firstFailEventData.getShoudlerTapRetryAttempts());
        assertEquals(true, firstFailEventData.isDeviceStatusInactive());

        // Assert failed DeviceMessage
        IgniteEvent failedIgniteEvent = firstFailEventData.getFailedIgniteEvent();
        assertEquals(messageId, failedIgniteEvent.getMessageId());
        assertEquals(vehicleId, failedIgniteEvent.getVehicleId());
        assertEquals(EventID.SPEED, failedIgniteEvent.getEventId());

        // Assert 3rd DeviceMessageFailureEventData
        IgniteEvent thirdDeviceMessageFailureEvent = getIgniteEvent(receivedRecords.get(Constants.THREE).value);
        assertEquals(EventID.DEVICEMESSAGEFAILURE, thirdDeviceMessageFailureEvent.getEventId());

        DeviceMessageFailureEventDataV1_0 thirdFailEventData =
                (DeviceMessageFailureEventDataV1_0) thirdDeviceMessageFailureEvent.getEventData();
        assertEquals(DeviceMessageErrorCode.RETRYING_SHOULDER_TAP, thirdFailEventData.getErrorCode());
        assertEquals(1, thirdFailEventData.getShoudlerTapRetryAttempts());
        assertEquals(true, thirdFailEventData.isDeviceStatusInactive());

        // Assert 6th DeviceMessageFailureEventData
        IgniteEvent sixthDeviceMessageFailureEvent = getIgniteEvent(receivedRecords.get(Constants.SIX).value);
        assertEquals(EventID.DEVICEMESSAGEFAILURE, sixthDeviceMessageFailureEvent.getEventId());

        DeviceMessageFailureEventDataV1_0 sixthFailEventData =
                (DeviceMessageFailureEventDataV1_0) sixthDeviceMessageFailureEvent.getEventData();
        assertEquals(DeviceMessageErrorCode.SHOULDER_TAP_RETRY_ATTEMPTS_EXCEEDED, sixthFailEventData.getErrorCode());
        assertEquals(maxRetry, sixthFailEventData.getShoudlerTapRetryAttempts());
        assertEquals(true, sixthFailEventData.isDeviceStatusInactive());
    }

    /**
     * Gets the mock response.
     *
     * @param shoulderTapSendSMSJsonResponse the shoulder tap send SMS json response
     * @return the mock response
     */
    private static MockResponse getMockResponse(String shoulderTapSendSMSJsonResponse) {
        MockResponse mockShoulderTapResponse = new MockResponse();
        mockShoulderTapResponse.setResponseCode(Constants.INT_202);
        mockShoulderTapResponse.setBody(shoulderTapSendSMSJsonResponse);
        return mockShoulderTapResponse;
    }

    /**
     * inner class DMAShoulderTapServiceProcessor implements IgniteEventStreamProcessor.
     */
    public static final class DMAShoulderTapServiceProcessor implements IgniteEventStreamProcessor {
        
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
            return "DMAShoulderTapServiceProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            IgniteEvent value = kafkaRecord.value();
            AbstractIgniteEvent event = (AbstractIgniteEvent) value;
            if (EventID.DEVICEMESSAGEFAILURE.equals(event.getEventId())) {
                LOGGER.debug("Received feedBackEvent: {}", value);
            } else {
                event.setDeviceRoutable(true);
                event.setShoulderTapEnabled(true);
                spc.forward(kafkaRecord.withValue(value));
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

        /**
         * Sources.
         *
         * @return the string[]
         */
        @Override
        public String[] sources() {
            return new String[] { sourceTopicName };
        }

        /**
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] { sourceTopicName };
        }
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
        IgniteEvent event = eventTransformer.fromBlob(eventData, Optional.empty());

        return event;
    }

    /**
     * Gets the scheduled thread delay.
     *
     * @return the scheduled thread delay
     */
    long getScheduledThreadDelay() {
        long freq = retryInterval / Constants.TWO;
        long delay = freq > retryMinThreshold ? freq : retryMinThreshold;
        return delay;
    }
}