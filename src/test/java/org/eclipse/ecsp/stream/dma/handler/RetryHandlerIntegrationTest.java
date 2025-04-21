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
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.DefaultMqttTopicNameGeneratorImpl;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.analytics.stream.base.utils.PahoMqttDispatcher;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


/**
 * class RetryHandlerIntegrationTest extends KafkaStreamsApplicationTestBase.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-handler-test.properties")
public class RetryHandlerIntegrationTest extends KafkaStreamsApplicationTestBase {
    
    /** The conn status topic. */
    private static String connStatusTopic;
    
    /** The source topic name. */
    private static String sourceTopicName;
    
    /** The i. */
    private static int i = 0;
    
    /** The failure event list. */
    private static LinkedList<IgniteEvent> failureEventList;
    
    /** The vehicle id. */
    private String vehicleId = "Vehicle12345";
    
    /** The retry test key. */
    private RetryTestKey retryTestKey = new RetryTestKey();
    
    /** The default mqtt topic name generator impl. */
    @Autowired
    private DefaultMqttTopicNameGeneratorImpl defaultMqttTopicNameGeneratorImpl;
    
    /** The transformer. */
    @Autowired
    private DeviceMessageIgniteEventTransformer transformer;
    
    /** The device conn status handler. */
    @Autowired
    private DeviceConnectionStatusHandler deviceConnStatusHandler;
    
    /** The device status back door kafka consumer. */
    @Autowired
    DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;
    
    /** The retry handler. */
    @Autowired
    private RetryHandler retryHandler;
    
    /** The paho mqtt dispatcher. */
    @Autowired
    private PahoMqttDispatcher pahoMqttDispatcher;
    
    /** The offline buffer DAO. */
    @Autowired
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /** The task id. */
    private String taskId = "taskId";

    /**
     * setup().
     *
     * @throws Exception Exception
     */
    @Before
    public void setup() throws Exception {
        failureEventList = new LinkedList<IgniteEvent>();
        retryTestKey.setKey(vehicleId);
        super.setup();
        i++;
        sourceTopicName = "sourceTopic" + i;
        connStatusTopic = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName.toLowerCase();
        createTopics(connStatusTopic, sourceTopicName);
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopicName);
        
        Properties kafkaConsumerProps = deviceStatusBackDoorKafkaConsumer.getKafkaConsumerProps();
        kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        deviceStatusBackDoorKafkaConsumer.addCallback(deviceConnStatusHandler.new DeviceStatusCallBack(), 0);
        deviceStatusBackDoorKafkaConsumer.startBackDoorKafkaConsumer();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        retryHandler.setup(taskId);
    }
    
    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        deviceStatusBackDoorKafkaConsumer.shutdown();
    }

    /**
     * Retry act as a passthrough.
     *
     * @throws MqttException MqttException
     * @throws Exception the exception
     * @throws InterruptedException InterruptedException
     * @throws ExecutionException ExecutionException
     */
    @Test
    public void retryHandlerTestAckNotSet() throws MqttException, Exception,
            InterruptedException, ExecutionException {
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMARetryTestAckNotSetServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        String deviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\","
                + "\"Version\": \"1.0\",\"Data\": {\"connStatus\":\"ACTIVE\","
                + "\"serviceName\":\"eCall\"},\"MessageId\": \"1234\",\"VehicleId\":"
                + " \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnStatusEvent.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        IgniteEventImpl retryEvent = new RetryTestEvent();
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(retryEvent),
                Version.V1_0, retryEvent, sourceTopicName, Constants.THREAD_SLEEP_TIME_60000);

        MqttClient client = getMqttClient(entity);
        List<String> messageList = new ArrayList<String>();
        client.setCallback(new MqttCallback() {
            String msgReceived;

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                msgReceived = message.toString();
                messageList.add(msgReceived);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }

            @Override
            public void connectionLost(Throwable cause) {

            }
        });

        String speedEventWithVehicleIdAndSourceDeviceId = "{\"EventID\": \"Speed\","
                + "\"Version\": \"1.0\",\"Data\": {\"value\":20.0},\"MessageId\":\"1237\","
                + "\"BizTransactionId\": \"Biz1237\",\"VehicleId\": \"Vehicle12345\","
                + "\"SourceDeviceId\": \"Device12345\"}";
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(),
                speedEventWithVehicleIdAndSourceDeviceId.getBytes());
        Thread.sleep(Constants.TWENTY_THOUSAND);
        // Retry will act as a passthrough in this scenario
        Assert.assertEquals(1, messageList.size());
        shutDown();
    }

    /**
     * Retry handler test when ack received.
     *
     * @throws MqttException the mqtt exception
     * @throws Exception the exception
     * @throws InterruptedException the interrupted exception
     * @throws ExecutionException the execution exception
     */
    @Test
    public void retryHandlerTestWhenAckReceived() throws MqttException,
            Exception, InterruptedException, ExecutionException {
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMARetryTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        DeviceMessage entity = getEntity();
        MqttClient client = getMqttClient(entity);
        List<String> messageList = new ArrayList<String>();
        client.setCallback(new MqttCallback() {
            String msgReceived;

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                msgReceived = message.toString();
                messageList.add(msgReceived);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }

            @Override
            public void connectionLost(Throwable cause) {

            }
        });

        String speedEventWithVehicleIdAndSourceDeviceId = "{\"EventID\": "
                + "\"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0},"
                + "\"MessageId\":\"1237\",\"BizTransactionId\": \"Biz1237\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(),
                speedEventWithVehicleIdAndSourceDeviceId.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_1000);
        String ackMsg = "{\"EventID\": \"Ack\",\"Version\": \"1.0\",\"Data\": {},"
                + "\"MessageId\":\"9876\",\"CorrelationId\":\"1237\",\"BizTransactionId\":"
                + " \"Biz1237\",\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(), ackMsg.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_15000);
        // once an ack is received Message should not be retried.
        int expectedAtmost = Constants.TWO;
        boolean flag = messageList.size() <= expectedAtmost;
        Assert.assertTrue(flag);
        shutDown();
    }

    /**
     * Retry handler test with ECU type based retries.
     *
     * @throws MqttException the mqtt exception
     * @throws Exception the exception
     * @throws InterruptedException the interrupted exception
     * @throws ExecutionException the execution exception
     */
    @Test
    public void retryHandlerTestWithECUTypeBasedRetries() throws
            MqttException, Exception, InterruptedException, ExecutionException {
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMARetryTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        String speedEventForTelematics = "{\"EventID\": \"Speed\",\"Version\": \"1.0\","
                + "\"Data\": {\"value\":20.0},\"MessageId\":\"1237\",\"BizTransactionId\": "
                + "\"Biz1237\",\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\","
                + "\"ecuType\": \"TELEMATICS\"}";
        String deviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\","
                + "\"Version\": \"1.0\",\"Data\": {\"connStatus\":\"ACTIVE\","
                + "\"serviceName\":\"eCall\"},\"MessageId\": \"1234\",\"VehicleId\": "
                + "\"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";

        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnStatusEvent.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(), speedEventForTelematics.getBytes());
        String speedEventForEcu1 = "{\"EventID\": \"Speed\",\"Version\": "
                + "\"1.0\",\"Data\": {\"value\":20.0},\"MessageId\":\"1237\","
                + "\"BizTransactionId\": \"Biz1237\",\"VehicleId\": \"Vehicle12345\","
                + "\"SourceDeviceId\": \"Device12345\",\"ecuType\": \"ecu1\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(), speedEventForEcu1.getBytes());

        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        /*
         * get a client to subscribe to the required topic topic
         */
        IgniteEventImpl retryEvent = new RetryTestEvent();
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(retryEvent),
                Version.V1_0, retryEvent, sourceTopicName, Constants.THREAD_SLEEP_TIME_60000);
        MqttClient client = getMqttClient(entity);
        List<String> messageList = new ArrayList<String>();
        client.setCallback(new MqttCallback() {
            String msgReceived;

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                msgReceived = message.toString();
                System.out.println("message arrived *************************************" + msgReceived);
                messageList.add(msgReceived);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }

            @Override
            public void connectionLost(Throwable cause) {

            }
        });

        int expectedAtmost = Constants.THREE;
        boolean flag = messageList.size() <= expectedAtmost;
        System.out.println("message list :---------------------->" + messageList.size());
        Assert.assertTrue(flag);
        shutDown();
    }

    /**
     * Retry handler test fallback to TTL on max retry exhausted.
     *
     * @throws MqttException the mqtt exception
     * @throws Exception the exception
     * @throws InterruptedException the interrupted exception
     * @throws ExecutionException the execution exception
     */
    @Test
    public void retryHandlerTestFallbackToTTLOnMaxRetryExhausted()
            throws MqttException, Exception, InterruptedException, ExecutionException {
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMARetryTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        String deviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\", \"Version\": "
                + "\"1.0\",\"Data\": {\"connStatus\":\"ACTIVE\", \"serviceName\":\"eCall\"},"
                + "\"MessageId\": \"1234\",\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnStatusEvent.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        /*
         * get a client to subscribe to the required topic topic
         */
        IgniteEventImpl retryEvent = new RetryTestEvent();
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(retryEvent),
                Version.V1_0, retryEvent, sourceTopicName, Constants.THREAD_SLEEP_TIME_60000);
        MqttClient client = getMqttClient(entity);
        List<String> messageList = new ArrayList<String>();
        client.setCallback(new MqttCallback() {
            String msgReceived;

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                msgReceived = message.toString();
                messageList.add(msgReceived);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }

            @Override
            public void connectionLost(Throwable cause) {

            }
        });

        String speedEventWithVehicleIdAndSourceDeviceId = "{\"EventID\": "
                + "\"test_Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0},"
                + "\"MessageId\":\"1237\",\"BizTransactionId\": \"Biz1237\",\"VehicleId\":"
                + " \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(),
                speedEventWithVehicleIdAndSourceDeviceId.getBytes());
        Thread.sleep(Constants.INT_20000);
        // message will be send once to device then retried Constants.THREE
        // (Constants.THREE is value of max retry set in property file) times.
        // So, totally each message will be sent 4 times to mqtt.
        // Expected noOfMessages * 4 messages.
        int expected = Constants.FOUR;
        Assert.assertEquals(expected, messageList.size());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        /*
         * Expected failed events = Constants.THREE for Constants.THREE RETRYING_DEVICE_MESSAGE
         * We don't send RETRY_ATTEMPTS_EXCEEDED one for this use case.
         */
        Assert.assertEquals(Constants.THREE, failureEventList.size());
        /*
         * Event will be saved to offline buffer collection in mongo
         * upon exhaustion of max retries.
         */
        Assert.assertEquals(1, offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(
                "Vehicle12345", false, Optional.of("Device12345"),
                Optional.empty()).size());
        DeviceMessageFailureEventDataV1_0 data1 = (DeviceMessageFailureEventDataV1_0) 
                failureEventList.get(0).getEventData();
        Assert.assertEquals(DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE, data1.getErrorCode());
        Assert.assertEquals(1, data1.getRetryAttempts());

        DeviceMessageFailureEventDataV1_0 data2 = (DeviceMessageFailureEventDataV1_0) 
                failureEventList.get(1).getEventData();
        Assert.assertEquals(DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE, data2.getErrorCode());
        Assert.assertEquals(Constants.TWO, data2.getRetryAttempts());

        DeviceMessageFailureEventDataV1_0 data3 = (DeviceMessageFailureEventDataV1_0) 
                failureEventList.get(Constants.TWO).getEventData();
        Assert.assertEquals(DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE, data3.getErrorCode());
        Assert.assertEquals(Constants.THREE, data3.getRetryAttempts());
        shutDown();
    }

    /**
     * Retry handler test.
     *
     * @throws MqttException the mqtt exception
     * @throws Exception the exception
     * @throws InterruptedException the interrupted exception
     * @throws ExecutionException the execution exception
     */
    @Test
    public void retryHandlerTest() throws MqttException, Exception, InterruptedException, ExecutionException {
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMARetryTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        DeviceMessage entity = getEntity();

        MqttClient client = getMqttClient(entity);
        List<String> messageList = new ArrayList<String>();
        client.setCallback(new MqttCallback() {
            String msgReceived;

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                msgReceived = message.toString();
                messageList.add(msgReceived);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }

            @Override
            public void connectionLost(Throwable cause) {

            }
        });

        String speedEventWithVehicleIdAndSourceDeviceId = "{\"EventID\": \"Speed\","
                + "\"Version\": \"1.0\",\"Data\": {\"value\":20.0},\"MessageId\":"
                + "\"1237\",\"BizTransactionId\": \"Biz1237\",\"VehicleId\": "
                + "\"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(),
                speedEventWithVehicleIdAndSourceDeviceId.getBytes());
        Thread.sleep(Constants.INT_20000);
        // message will be send once to device then retried 3
        // (3 is value of max retry set in property file) times.
        // So, totally each message will be sent 4 times to mqtt.
        // Expected noOfMessages * 4 messages.
        int expected = Constants.FOUR;
        Assert.assertEquals(expected, messageList.size());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        Assert.assertEquals(expected, failureEventList.size());

        DeviceMessageFailureEventDataV1_0 data1 = (DeviceMessageFailureEventDataV1_0)
                failureEventList.get(0).getEventData();
        Assert.assertEquals(data1.getErrorCode(),
                DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE);
        Assert.assertEquals(data1.getRetryAttempts(), 1);

        DeviceMessageFailureEventDataV1_0 data2 = (DeviceMessageFailureEventDataV1_0)
                failureEventList.get(1).getEventData();
        Assert.assertEquals(data2.getErrorCode(),
                DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE);
        Assert.assertEquals(data2.getRetryAttempts(), Constants.TWO);

        DeviceMessageFailureEventDataV1_0 data3 = (DeviceMessageFailureEventDataV1_0)
                failureEventList.get(Constants.TWO).getEventData();
        Assert.assertEquals(data3.getErrorCode(),
                DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE);
        Assert.assertEquals(data3.getRetryAttempts(), Constants.THREE);

        DeviceMessageFailureEventDataV1_0 data4 = (DeviceMessageFailureEventDataV1_0)
                failureEventList.get(Constants.THREE).getEventData();
        Assert.assertEquals(data4.getErrorCode(),
                DeviceMessageErrorCode.RETRY_ATTEMPTS_EXCEEDED);
        Assert.assertEquals(data3.getRetryAttempts(), Constants.THREE);

    }

    /**
     * Gets the entity.
     *
     * @return the entity
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     */
    private DeviceMessage getEntity() throws ExecutionException, InterruptedException {
        String deviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\","
                + "\"Version\": \"1.0\",\"Data\": {\"connStatus\":\"ACTIVE\","
                + "\"serviceName\":\"eCall\"},\"MessageId\": \"1234\",\"VehicleId\": "
                + "\"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnStatusEvent.getBytes());
        IgniteEventImpl retryEvent = new RetryTestEvent();
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(retryEvent),
                Version.V1_0, retryEvent, sourceTopicName, Constants.THREAD_SLEEP_TIME_60000);
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        return entity;
    }

    /**
     * Retry handler test retry attempts not reset to zero when event saved in offline buffer.
     *
     * @throws Exception the exception
     */
    @Test
    public void retryHandlerTestRetryAttemptsNotResetToZeroWhenEventSavedInOfflineBuffer()
        throws  Exception {
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMARetryTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        getEntity();
        /*
         * get a client to subscribe to the required topic topic
         */
        List<String> messageList = new ArrayList<String>();
        MqttClient client = getMqttClient();
        client.setCallback(new MqttCallback() {
            String msgReceived;

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                msgReceived = message.toString();
                messageList.add(msgReceived);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }

            @Override
            public void connectionLost(Throwable cause) {

            }
        });

        String speedEventWithVehicleIdAndSourceDeviceId = "{\"EventID\": "
                + "\"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0},"
                + "\"MessageId\":\"1237\",\"BizTransactionId\": \"Biz1237\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(),
                speedEventWithVehicleIdAndSourceDeviceId.getBytes());
        // below sleep is given to ensure that above sent event is retried at least once
        Thread.sleep(Constants.THREAD_SLEEP_TIME_60000);
        // making device inactive to save this event in mongo
        String inactiveDeviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\","
                + "\"Version\": \"1.0\",\"Data\": {\"connStatus\":\"INACTIVE\","
                + "\"serviceName\":\"eCall\"},\"MessageId\": \"1234\",\"VehicleId\":"
                + " \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), inactiveDeviceConnStatusEvent.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_3000);
        String deviceConnStatusEvent = setKafkaUtils();
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnStatusEvent.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_13000);
        // message will be send once to device then retried Constants.THREE
        // (Constants.THREE is value of max retry set in property file) times.
        // So, totally each message will be sent 4 times to mqtt.
        // Expected noOfMessages * 4 messages. And this proves that even though
        // event was saved in
        // offline buffer, retry count wasn't reset to 0.
        int expected = Constants.FOUR;
        Assert.assertEquals(expected, messageList.size());

        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        Assert.assertEquals(expected, failureEventList.size());

        DeviceMessageFailureEventDataV1_0 data1 = (DeviceMessageFailureEventDataV1_0)
                failureEventList.get(0).getEventData();
        Assert.assertEquals(data1.getErrorCode(), DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE);
        Assert.assertEquals(data1.getRetryAttempts(), 1);

        DeviceMessageFailureEventDataV1_0 data2 = (DeviceMessageFailureEventDataV1_0)
                failureEventList.get(1).getEventData();
        Assert.assertEquals(data2.getErrorCode(), DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE);
        Assert.assertEquals(data2.getRetryAttempts(), Constants.TWO);

        DeviceMessageFailureEventDataV1_0 data3 = (DeviceMessageFailureEventDataV1_0)
                failureEventList.get(Constants.TWO).getEventData();
        Assert.assertEquals(data3.getErrorCode(), DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE);
        Assert.assertEquals(data3.getRetryAttempts(), Constants.THREE);

        DeviceMessageFailureEventDataV1_0 data4 = (DeviceMessageFailureEventDataV1_0)
                failureEventList.get(Constants.THREE).getEventData();
        Assert.assertEquals(data4.getErrorCode(), DeviceMessageErrorCode.RETRY_ATTEMPTS_EXCEEDED);
        Assert.assertEquals(data4.getRetryAttempts(), Constants.THREE);
    }

    /**
     * Sets the kafka utils.
     *
     * @return the string
     * @throws Exception the exception
     */
    private String setKafkaUtils() throws Exception {
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        String deviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\","
                + "\"Version\": \"1.0\",\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\""
                + ":\"eCall\"},\"MessageId\": \"1234\",\"VehicleId\": \"Vehicle12345\","
                + "\"SourceDeviceId\": \"Device12345\"}";

        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnStatusEvent.getBytes());

        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        return deviceConnStatusEvent;
    }

    /**
     * Gets the mqtt client.
     *
     * @return the mqtt client
     * @throws MqttException the mqtt exception
     */
    private MqttClient getMqttClient() throws MqttException {
        IgniteEventImpl retryEvent = new RetryTestEvent();
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(retryEvent),
                Version.V1_0, retryEvent, sourceTopicName, Constants.THREAD_SLEEP_TIME_60000);

        MqttClient client = getMqttClient(entity);
        return client;
    }

    /**
     * Gets the mqtt client.
     *
     * @param entity the entity
     * @return the mqtt client
     * @throws MqttException the mqtt exception
     */
    private MqttClient getMqttClient(DeviceMessage entity) throws MqttException {
        String mqttTopicToSubscribe = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(retryTestKey, 
                entity.getDeviceMessageHeader(), null).get();

        MqttClient client = pahoMqttDispatcher.getMqttClient(PropertyNames.DEFAULT_PLATFORMID).get();
        client.subscribe(mqttTopicToSubscribe);
        return client;
    }

    /**
     * Retry handler test when devlivery cut offexceeded.
     *
     * @throws MqttException the mqtt exception
     * @throws Exception the exception
     * @throws InterruptedException the interrupted exception
     * @throws ExecutionException the execution exception
     */
    @Test
    public void retryHandlerTestWhenDevliveryCutOffexceeded() throws
            MqttException, Exception, InterruptedException, ExecutionException {
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMARetryTestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        String deviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\",\"Version\": "
                + "\"1.0\",\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},"
                + "\"MessageId\": \"1234\",\"VehicleId\": \"Vehicle12345\","
                + "\"SourceDeviceId\": \"Device12345\"}";

        KafkaTestUtils.sendMessages(connStatusTopic, producerProps,
                vehicleId.getBytes(), deviceConnStatusEvent.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        /*
         * get a client to subscribe to the required topic topic
         */
        IgniteEventImpl retryEvent = new RetryTestEvent();
        DeviceMessage entity = new DeviceMessage(transformer
                .toBlob(retryEvent), Version.V1_0, retryEvent,
                sourceTopicName, Constants.THREAD_SLEEP_TIME_60000);
        String mqttTopicToSubscribe = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(retryTestKey, 
                entity.getDeviceMessageHeader(), null).get();
        MqttClient client = pahoMqttDispatcher.getMqttClient(PropertyNames.DEFAULT_PLATFORMID).get();
        List<String> messageList = new ArrayList<String>();
        client.subscribe(mqttTopicToSubscribe);
        client.setCallback(new MqttCallback() {
            String msgReceived;

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                msgReceived = message.toString();
                messageList.add(msgReceived);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }

            @Override
            public void connectionLost(Throwable cause) {

            }
        });

        long pastTs = System.currentTimeMillis() - TestConstants.THREAD_SLEEP_TIME_10000;
        String speedEventWithVehicleIdAndSourceDeviceId = "{\"EventID\": "
                + "\"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0},"
                + "\"MessageId\":\"1237\",\"BizTransactionId\": \"Biz1237\",\"VehicleId\": "
                + "\"Vehicle12345\",\"SourceDeviceId\": \"Device12345\",\"DeviceDeliveryCutoff\": "
                + pastTs + "}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps,
                vehicleId.getBytes(),
                speedEventWithVehicleIdAndSourceDeviceId.getBytes());
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        // message will be send once to device then retried Constants.THREE
        // (Constants.THREE is value of max retry set in property file) times.
        // So, totally each message will be sent 4 times to mqtt.
        // Expected noOfMessages * 4 messages.
        int expected = 0;
        Assert.assertEquals(expected, messageList.size());
        shutDown();
    }

    /**
     * inner class DMARetryTestServiceProcessor implements IgniteEventStreamProcessor.
     */
    public static final class DMARetryTestServiceProcessor implements IgniteEventStreamProcessor {
        
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

            return "DMAretryTestServiceProcessor";
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
            if (!value.getEventId().equals(EventID.DEVICEMESSAGEFAILURE)) {
                if (!value.getEventId().equals("Ack")) {
                    event.setDeviceRoutable(true);
                    event.setResponseExpected(true);
                }
                spc.forward(kafkaRecord.withValue(event));
            } else {
                failureEventList.add(value);
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
            return new String[] {};
        }
    }

    /**
     * inner class DMARetryTestAckNotSetServiceProcessor implements IgniteEventStreamProcessor.
     */
    public static final class DMARetryTestAckNotSetServiceProcessor implements IgniteEventStreamProcessor {
        
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

            return "DMAretryTestServiceProcessor";
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
                AbstractIgniteEvent event = (AbstractIgniteEvent) value;
                event.setDeviceRoutable(true);
                kafkaRecord.withValue(event);
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
            return new String[] {};
        }
    }

}