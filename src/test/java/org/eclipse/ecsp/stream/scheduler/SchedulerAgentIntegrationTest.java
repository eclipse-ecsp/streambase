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

package org.eclipse.ecsp.stream.scheduler;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.SpeedV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.events.scheduler.CreateScheduleEventData;
import org.eclipse.ecsp.events.scheduler.DeleteScheduleEventData;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.transform.GenericIgniteEventTransformer;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.SCHEDULER_AGENT_TOPIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;


/**
 * class {@link SchedulerAgentIntegrationTest} extends {@link KafkaStreamsApplicationTestBase}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@TestPropertySource("/scheduler-agent-test.properties")
public class SchedulerAgentIntegrationTest extends KafkaStreamsApplicationTestBase {
    
    /** The test event type. */
    private static String testEventType;
    
    /** The service name. */
    @Value("${service.name}")
    private String serviceName;
    
    /** The source topic. */
    @Value("${source.topic.name}")
    private String sourceTopic;
    
    /** The scheduler agent topic. */
    @Value("${" + SCHEDULER_AGENT_TOPIC_NAME + "}")
    private String schedulerAgentTopic;

    /**
     *  setUp().
     *
     * @throws Exception Exception
     * @throws MqttException MqttException
     */
    @Before
    public void setUp() throws Exception, MqttException {
        createTopics(sourceTopic, schedulerAgentTopic);
        super.setup();
        ksProps.remove(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG);
        ksProps.remove(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG);
        ksProps.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        ksProps.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        launchApplication();
        await().atMost(TestConstants.LONG_30000, TimeUnit.MILLISECONDS);
    }

    /**
     * Test if create schedule event is forwarded to scheduler topic.
     */
    @Test
    public void testCreateScheduleEvent() {
        try {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

            CreateScheduleEventData createScheduleEventData = new CreateScheduleEventData();
            long recurrenceDelayMs = Constants.THREAD_SLEEP_TIME_60000;
            long initialDelayMs = Constants.THREAD_SLEEP_TIME_60000;
            createScheduleEventData.setRecurrenceDelayMs(recurrenceDelayMs);
            createScheduleEventData.setInitialDelayMs(initialDelayMs);
            int times = 1;
            createScheduleEventData.setFiringCount(times);
            String service = "ECall";
            createScheduleEventData.setServiceName(service);
            String notificationPayload = "executeMonthlyReportUpload";
            createScheduleEventData.setNotificationPayload(notificationPayload.getBytes());
            String notificationTopic = "scheduleNotificationTopic";
            createScheduleEventData.setNotificationTopic(notificationTopic);

            IgniteStringKey key = new IgniteStringKey();
            key.setKey("111");
            createScheduleEventData.setNotificationKey(key);
            IgniteEventImpl igniteEvent = new IgniteEventImpl();
            igniteEvent.setEventData(createScheduleEventData);

            byte[] eventData = getEventBlob(igniteEvent);

            testEventType = EventID.CREATE_SCHEDULE_EVENT;
            String vehicleId = getIgniteEvent(igniteEvent);
            sendMessages(sourceTopic, producerProps,
                    Arrays.asList(vehicleId.getBytes(), eventData));
            createConsumerProps();
            await().atMost(TestConstants.LONG_30000, TimeUnit.MILLISECONDS);
            List<KeyValue<byte[], byte[]>> scheduleNotificationAckRecords = getKeyValueRecords(schedulerAgentTopic, 
                    consumerProps, 1, Constants.THREAD_SLEEP_TIME_10000);
            assertEquals(1, scheduleNotificationAckRecords.size());
            KeyValue<byte[], byte[]> value = scheduleNotificationAckRecords.get(0);
            byte[] eventValue = value.value;
            IgniteEvent receivedEvent = getIgniteEvent(eventValue);

            assertEquals(EventID.CREATE_SCHEDULE_EVENT,
                    receivedEvent.getEventId());

            CreateScheduleEventData receivedEventData = (CreateScheduleEventData) receivedEvent.getEventData();
            assertEquals(EventID.CREATE_SCHEDULE_EVENT,
                    receivedEvent.getEventId());
            assertEquals(initialDelayMs,
                    receivedEventData.getInitialDelayMs());
            assertEquals(recurrenceDelayMs, receivedEventData.getRecurrenceDelayMs());
            assertTrue(Arrays.equals(notificationPayload.getBytes(),
                    receivedEventData.getNotificationPayload()));
            assertEquals(notificationTopic,
                    receivedEventData.getNotificationTopic());
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutDownApplication();
    }

    /**
     * Creates the consumer props.
     */
    private void createConsumerProps() {
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                "schedule-agent-consumer-group");
    }

    /**
     * Test if delete schedule event is forwarded to scheduler topic.
     */
    @Test
    public void testDeleteScheduleEvent() {
        try {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

            IgniteEventImpl igniteEvent = new IgniteEventImpl();
            igniteEvent.setEventId(EventID.DELETE_SCHEDULE_EVENT);
            igniteEvent.setTimestamp(System.currentTimeMillis());
            igniteEvent.setRequestId("Request123");
            igniteEvent.setCorrelationId("1234");
            igniteEvent.setBizTransactionId("Biz1234");
            String messageId = "Message1222";
            igniteEvent.setMessageId(messageId);
            igniteEvent.setSourceDeviceId("12345");
            String vehicleId = "Vehicle12345";
            igniteEvent.setVehicleId(vehicleId);
            igniteEvent.setVersion(Version.V1_0);

            String scheduleId = "scheduleId123";
            DeleteScheduleEventData deleteScheduleEventData = new DeleteScheduleEventData();
            deleteScheduleEventData.setScheduleId(scheduleId);

            igniteEvent.setEventData(deleteScheduleEventData);

            byte[] eventData = getEventBlob(igniteEvent);

            testEventType = EventID.DELETE_SCHEDULE_EVENT;
            sendMessages(sourceTopic, producerProps,
                    Arrays.asList(vehicleId.getBytes(), eventData));
            await().atMost(TestConstants.LONG_30000, TimeUnit.MILLISECONDS);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class);
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class);
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                    "schedule-agent-consumer-group");

            List<KeyValue<byte[], byte[]>> scheduleNotificationAckRecords =
                    getKeyValueRecords(schedulerAgentTopic, consumerProps, 1, Constants.THREAD_SLEEP_TIME_5000);

            assertEquals(1, scheduleNotificationAckRecords.size());

            KeyValue<byte[], byte[]> value = scheduleNotificationAckRecords.get(0);
            byte[] eventValue = value.value;
            IgniteEvent receivedEvent = getIgniteEvent(eventValue);

            DeleteScheduleEventData receivedEventData = (DeleteScheduleEventData) receivedEvent.getEventData();

            assertEquals(EventID.DELETE_SCHEDULE_EVENT,
                    receivedEvent.getEventId());
            assertEquals(scheduleId,
                    receivedEventData.getScheduleId());
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutDownApplication();
    }

    /**
     * Test if non schedule event is not forwarded to scheduler topic.
     */
    @Test
    public void testNonScheduleEvent() {
        try {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

            IgniteEventImpl igniteEvent = new IgniteEventImpl();
            igniteEvent.setEventId(EventID.SPEED);
            igniteEvent.setTimestamp(System.currentTimeMillis());
            igniteEvent.setRequestId("Request123");
            igniteEvent.setCorrelationId("1234");
            igniteEvent.setBizTransactionId("Biz1234");
            String messageId = "Message1222";
            igniteEvent.setMessageId(messageId);
            igniteEvent.setSourceDeviceId("12345");
            String vehicleId = "Vehicle12345";
            igniteEvent.setVehicleId(vehicleId);
            igniteEvent.setVersion(Version.V1_0);
            SpeedV1_0 speed = new SpeedV1_0();
            speed.setValue(TestConstants.DOUBLE_TWENTY);
            igniteEvent.setEventData(speed);
            byte[] eventData = getEventBlob(igniteEvent);
            testEventType = EventID.SPEED;
            sendMessages(sourceTopic, producerProps,
                    Arrays.asList(vehicleId.getBytes(), eventData));
            await().atMost(TestConstants.LONG_30000, TimeUnit.MILLISECONDS);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class);
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class);
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                    "schedule-agent-consumer-group");

            List<KeyValue<byte[], byte[]>> scheduleNotificationAckRecords =
                    getKeyValueRecords(schedulerAgentTopic, consumerProps, 1,
                    Constants.THREAD_SLEEP_TIME_5000);

            assertEquals(0, scheduleNotificationAckRecords.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutDownApplication();
    }

    /**
     * Gets the event blob.
     *
     * @param event the event
     * @return the event blob
     * @throws JsonProcessingException the json processing exception
     */
    private byte[] getEventBlob(IgniteEventImpl event) throws JsonProcessingException {
        GenericIgniteEventTransformer eventTransformer = new GenericIgniteEventTransformer();
        byte[] eventData = eventTransformer.toBlob(event);

        return eventData;
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
     * Gets the ignite event.
     *
     * @param igniteEvent the ignite event
     * @return the ignite event
     */
    private static String getIgniteEvent(IgniteEventImpl igniteEvent) {
        igniteEvent.setEventId(EventID.CREATE_SCHEDULE_EVENT);
        igniteEvent.setTimestamp(System.currentTimeMillis());
        igniteEvent.setRequestId("Request123");
        igniteEvent.setCorrelationId("1234");
        igniteEvent.setBizTransactionId("Biz1234");
        String messageId = "Message1222";
        igniteEvent.setMessageId(messageId);
        igniteEvent.setSourceDeviceId("12345");
        String vehicleId = "Vehicle12345";
        igniteEvent.setVehicleId(vehicleId);
        igniteEvent.setVersion(Version.V1_0);
        return vehicleId;
    }

    /**
     * inner class {@link SchedulerAgentTestStreamProcessor} implements {@link IgniteEventStreamProcessor}.
     */
    public static class SchedulerAgentTestStreamProcessor implements IgniteEventStreamProcessor {
        
        /** The spc. */
        protected StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

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
            return "scheduler-agent-test-sp";
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
        @SuppressWarnings("rawtypes")
        @Override
        public HarmanPersistentKVStore createStateStore() {
            return null;
        }

    }

    /**
     * inner  class {@link TestStreamPostProcessor} extends {@link SchedulerAgentTestStreamProcessor}.
     */
    public static class TestStreamPostProcessor extends SchedulerAgentTestStreamProcessor {
        
        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return "test-post-sp";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        @Test
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            IgniteEvent value = kafkaRecord.value();
            if (!value.getEventId().equals(EventID.DEVICEMESSAGEFAILURE)) {
                ((AbstractIgniteEvent) value).setDeviceRoutable(true);

                if (EventID.CREATE_SCHEDULE_EVENT.equals(testEventType)) {
                    assertEquals(EventID.CREATE_SCHEDULE_EVENT,
                            value.getEventId());
                } else if (EventID.DELETE_SCHEDULE_EVENT.equals(testEventType)) {
                    assertEquals(EventID.DELETE_SCHEDULE_EVENT,
                            value.getEventId());
                } else {
                    assertEquals(EventID.SPEED,
                            value.getEventId());
                }
                kafkaRecord.withValue(value);
                spc.forward(kafkaRecord);
            }
        }
    }
}
