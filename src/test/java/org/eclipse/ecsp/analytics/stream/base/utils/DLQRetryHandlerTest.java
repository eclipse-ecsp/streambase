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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamBaseConstant;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.discovery.PropBasedDiscoveryServiceImpl;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.domain.AbstractBlobEventData.Encoding;
import org.eclipse.ecsp.domain.BlobDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.IgniteBaseException;
import org.eclipse.ecsp.domain.IgniteEventSource;
import org.eclipse.ecsp.domain.IgniteExceptionDataV1_1;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteBlobEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.serializer.IngestionSerializerFstImpl;
import org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Properties;



/**
 * IT test class {@link DLQRetryHandlerTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dlq-reprocessing-test.properties")
public class DLQRetryHandlerTest extends KafkaStreamsApplicationTestBase {

    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(DLQRetryHandlerTest.class);
    
    /** The in topic name. */
    private static String inTopicName = "service-test";
    
    /** The device ID. */
    private static String deviceID = "DeviceId-1";
    
    /** The request id. */
    private static String requestId = "req-1";
    
    /** The vehicle id. */
    private static String vehicleId = "vehicle-1";
    
    /** The service name. */
    private static String serviceName = "Ecall";
    
    /** The dql topic name. */
    private static String dqlTopicName = "ecall" + StreamBaseConstant.DLQ_TOPIC_POSFIX;
    
    /** The value ser. */
    private static IngestionSerializerFstImpl valueSer = new IngestionSerializerFstImpl();
    
    /** The key ser. */
    private static IgniteKeyTransformerStringImpl keySer = new IgniteKeyTransformerStringImpl();
    
    /** The toggle DLQ. */
    private static boolean toggleDLQ = true;
    
    /** The mapper. */
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * Setup.
     *
     * @throws Exception the exception
     */
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        createTopics(inTopicName);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL,
                PropBasedDiscoveryServiceImpl.class.getName());
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, inTopicName);
        ksProps.put(PropertyNames.APPLICATION_ID, "dlq");

        ksProps.put("event.transformer.classes", "genericIgniteEventTransformer");
        ksProps.put("ignite.key.transformer.class", "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        ksProps.put("ingestion.serializer.class", "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");

    }

    /**
     * Tests the DLQ re-processing flow logic for max retry times and finally is forwarded to dlq.
     *
     * @throws Exception Exception
     */
    @Test
    public void testperformDLQReprocessingFailedAndFwdToDLQ() throws Exception {
        DLQReprocessingPreProcessorOne.count = 0;
        DLQReprocessingPreProcessorTwo.count = 0;
        DLQReprocessingPostProcessorOne.count = 0;
        DLQReprocessingPostProcessorTwo.count = 0;
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DLQReprocessingMaxRetryServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        launchApplication();
        IgniteStringKey igniteStringKey = new IgniteStringKey();
        igniteStringKey.setKey("key1");
        IgniteBlobEvent event = getDummyIgniteBlobEvent();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, keySer.toBlob(igniteStringKey), 
                valueSer.serialize(event));
        Thread.sleep(Constants.THREAD_SLEEP_TIME_60000);
        List<String[]> messages = KafkaTestUtils.getMessages(dqlTopicName, consumerProps, 1, 
                TestConstants.TEN_THOUSAND);
        try {
            IgniteStringKey key = mapper.readValue(messages.get(0)[0], IgniteStringKey.class);
            LOGGER.info("DLQ Reprocessing message {}", messages);
            Assert.assertEquals("key1", key.getKey());
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutDown();

    }

    /**
     * Tests the DLQ re-processing flow logic for successfully completions with dlq reprocessing performed once.
     *
     * @throws Exception Exception
     */

    @Test
    public void testDLQReprocessingForSuccess() throws Exception {
        // cleanup
        DLQReprocessingPreProcessorOne.count = 0;
        DLQReprocessingPreProcessorTwo.count = 0;
        DLQReprocessingPostProcessorOne.count = 0;
        DLQReprocessingPostProcessorTwo.count = 0;
        ksProps.put(PropertyNames.PRE_PROCESSORS,
                "org.eclipse.ecsp.analytics.stream.base.processors.ProtocolTranslatorPreProcessor,"
                        + DLQReprocessingPreProcessorOne.class.getName() + "," 
                        + DLQReprocessingPreProcessorTwo.class.getName());
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DLQReprocessingServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS,
                DLQReprocessingPostProcessorOne.class.getName() + ","
                        + DLQReprocessingPostProcessorTwo.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        launchApplication();
        IgniteStringKey igniteStringKey = new IgniteStringKey();
        igniteStringKey.setKey("key1");
        IgniteBlobEvent event = getDummyIgniteBlobEvent();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, keySer.toBlob(igniteStringKey), 
                valueSer.serialize(event));
        Thread.sleep(Constants.THREAD_SLEEP_TIME_60000);
        try {
            Assert.assertEquals(1, DLQReprocessingPreProcessorOne.count);
            Assert.assertEquals(1, DLQReprocessingPreProcessorTwo.count);
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutDown();
    }

    /**
     * Gets the dummy ignite blob event.
     *
     * @return the dummy ignite blob event
     */
    private IgniteBlobEvent getDummyIgniteBlobEvent() {

        IgniteBlobEvent igniteBlobEvent = new IgniteBlobEvent();
        igniteBlobEvent.setSourceDeviceId(deviceID);
        igniteBlobEvent.setEventId(EventID.BLOBDATA);
        igniteBlobEvent.setRequestId(requestId);
        igniteBlobEvent.setSchemaVersion(Version.V1_0);
        igniteBlobEvent.setTimestamp(System.currentTimeMillis());
        igniteBlobEvent.setVehicleId(vehicleId);
        igniteBlobEvent.setVersion(Version.V1_0);
        BlobDataV1_0 eventData = new BlobDataV1_0();
        eventData.setEncoding(Encoding.JSON);
        eventData.setEventSource(IgniteEventSource.IGNITE);
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0}}";
        eventData.setPayload(speedEvent.getBytes());
        igniteBlobEvent.setEventData(eventData);
        return igniteBlobEvent;
    }

    /**
     * inner class {@link DLQServiceProcessor}.
     */
    public static final class DLQServiceProcessor implements
            StreamProcessor<IgniteKey<?>, IgniteEvent, IgniteKey<?>, IgniteEvent> {

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return serviceName;
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {

            IgniteKey<?> key = kafkaRecord.key();
            IgniteEvent value = kafkaRecord.value();
            LOGGER.info("Test DLQ ---> Key {} Value {} event id {}", key, value, value.getEventId());
            throw new RuntimeException("DLQ testing");

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
            return new String[] { inTopicName };
        }

        /**
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] { "test-dlq" };
        }
    }

    /**
     * inner class {@link DLQReprocessingServiceProcessor}.
     */
    public static final class DLQReprocessingServiceProcessor
            implements StreamProcessor<IgniteKey<?>, IgniteEvent, IgniteKey<?>, IgniteEvent> {
        
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
            return "DLQReprocessingServiceProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {

            IgniteKey<?> key = kafkaRecord.key();
            IgniteEvent value = kafkaRecord.value();
            if (toggleDLQ) {
                toggleDLQ = false;
                LOGGER.info("Test DLQ Reprocessing ---> Key {} Value {} event id {}",
                        key, value, value.getEventId());
                throw new IgniteBaseException("Exception occured in DLQReprocessingServiceProcessor", true,
                        new RuntimeException("Connection refused."));
            } else {
                IgniteEvent updatedValue = new IgniteEventImpl();
                if (value.getEventData() instanceof IgniteExceptionDataV1_1) {
                    updatedValue = ((IgniteExceptionDataV1_1) value.getEventData()).getIgniteEvent();
                }
                Record<IgniteKey<?>, IgniteEvent> kafkaRecord1 = kafkaRecord;
                kafkaRecord1.withKey(key);
                kafkaRecord1.withValue(updatedValue);
                this.spc.forward(kafkaRecord1);
            }

            this.spc.forward(kafkaRecord);

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
            return new String[] { inTopicName };
        }

    }

    /**
     * inner class DLQReprocessingMaxRetryServiceProcessor implements StreamProcessor.
     */
    public static final class DLQReprocessingMaxRetryServiceProcessor
            implements StreamProcessor<IgniteKey<?>, IgniteEvent, IgniteKey<?>, IgniteEvent> {

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return "dlq-reprocessing-max-retry-test-processor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            IgniteEvent value = kafkaRecord.value();
            LOGGER.info("Test DLQ Reprocessing ---> Key {} Value {} event id {}", 
                    kafkaRecord.key(), value, value.getEventId());
            throw new IgniteBaseException("Exception occured in DLQReprocessingServiceProcessor", true,
                    new RuntimeException("Connection refused."));

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
            return new String[] { inTopicName };
        }

        /**
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] { "test-dlq" };
        }
    }

    /**
     * inner class {@link DLQReprocessingPreProcessorOne}.
     */
    public static final class DLQReprocessingPreProcessorOne
            implements StreamProcessor<IgniteKey<?>, IgniteEvent, IgniteKey<?>, IgniteEvent> {
        
        /** The Constant LOGGER. */
        private static final Logger LOGGER = LoggerFactory.getLogger(DLQReprocessingPreProcessorTwo.class);
        
        /** The count. */
        private static int count;
        
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
            return "DLQReprocessingPreProcessorOne";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            LOGGER.info("DLQReprocessingPreProcessorOne : Process {}", kafkaRecord);
            count++;
            this.spc.forward(kafkaRecord);

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

    /**
     * inner class {@link DLQReprocessingPreProcessorTwo}.
     */
    public static final class DLQReprocessingPreProcessorTwo
            implements StreamProcessor<IgniteKey<?>, IgniteEvent, IgniteKey<?>, IgniteEvent> {
        
        /** The Constant LOGGER. */
        private static final Logger LOGGER = LoggerFactory.getLogger(DLQReprocessingPreProcessorTwo.class);
        
        /** The count. */
        private static int count;
        
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
            return "DLQReprocessingPreProcessorTwor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            LOGGER.info("DLQReprocessingPreProcessorTwo : Process {}", kafkaRecord);
            count++;
            this.spc.forward(kafkaRecord);

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

    /**
     * inner class {@link DLQReprocessingPostProcessorOne}.
     */
    public static final class DLQReprocessingPostProcessorOne
            implements StreamProcessor<IgniteKey<?>, IgniteEvent, IgniteKey<?>, IgniteEvent> {
        
        /** The Constant LOGGER. */
        private static final Logger LOGGER = LoggerFactory.getLogger(DLQReprocessingPostProcessorOne.class);
        
        /** The count. */
        private static int count;
        
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
            return "DLQReprocessingPostProcessorOne";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            LOGGER.info("DLQReprocessingPostProcessorOne : Process {}", kafkaRecord);
            count++;
            this.spc.forward(kafkaRecord);

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

    /**
     * inner class {@link DLQReprocessingPostProcessorTwo}.
     */
    public static final class DLQReprocessingPostProcessorTwo
            implements StreamProcessor<IgniteKey<?>, IgniteEvent, IgniteKey<?>, IgniteEvent> {
        
        /** The Constant LOGGER. */
        private static final Logger LOGGER = LoggerFactory.getLogger(DLQReprocessingPostProcessorTwo.class);
        
        /** The count. */
        private static int count;

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return "DLQReprocessingPostProcessorTwo";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            LOGGER.info("DLQReprocessingPostProcessorTwo : Process {}", kafkaRecord);
            count++;

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
