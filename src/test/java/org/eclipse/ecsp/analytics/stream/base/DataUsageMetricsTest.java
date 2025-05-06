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

package org.eclipse.ecsp.analytics.stream.base;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.discovery.PropBasedDiscoveryServiceImpl;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.domain.DataUsageEventDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.SpeedV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.transform.GenericIgniteEventTransformer;
import org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl;
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

import java.util.List;
import java.util.Optional;
import java.util.Properties;


/**
 * class DataUsageMetricsTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/stream-base-test2.properties")
public class DataUsageMetricsTest extends KafkaStreamsApplicationTestBase {

    /** The key ser. */
    private static IgniteKeyTransformerStringImpl keySer = new IgniteKeyTransformerStringImpl();
    
    /** The source topic name. */
    private static String sourceTopicName;
    
    /** The sink topic name. */
    private static String sinkTopicName;
    
    /** The i. */
    private static int i = 0;

    /** The transformer. */
    @Autowired
    GenericIgniteEventTransformer transformer;

    /** The data usage test topic name. */
    @Value("${" + PropertyNames.KAFKA_DATA_CONSUMPTION_METRICS_KAFKA_TOPIC + ":}")
    private String dataUsageTestTopicName;

    /**
     * Setup.
     *
     * @throws Exception the exception
     */
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        i++;
        sourceTopicName = "sourceTopic" + i;
        sinkTopicName = "sinkTopic" + i;
        createTopics(sourceTopicName, sinkTopicName, dataUsageTestTopicName);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());

        ksProps.put("event.transformer.classes", "genericIgniteEventTransformer");
        ksProps.put("ignite.key.transformer.class",
                "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        ksProps.put("ingestion.serializer.class",
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        ksProps.put("sink.topic.name", sinkTopicName);
        ksProps.put(PropertyNames.PRE_PROCESSORS,
                "org.eclipse.ecsp.analytics.stream.base.processors.TaskContextInitializer,"
                        + "org.eclipse.ecsp.analytics.stream.base.processors.ProtocolTranslatorPreProcessor");
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, StreamServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS, StreamPostProcessor.class.getName());
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, PropBasedDiscoveryServiceImpl.class.getName());
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopicName);
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());

    }

    /**
     * Testing if the event size is pushed to data usage topic and it matches.
     *
     * @throws Exception the exception
     */
    @Test
    public void testDataUsageConsumptionMetricForNormalEvent() throws Exception {

        launchApplication();

        IgniteEventImpl event = getDummyIgniteBlobEvent("dummyDeviceID", "req"
                + System.currentTimeMillis(), "dummy_vid");

        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, keySer
                        .toBlob(new IgniteStringKey("dummyId")), transformer.toBlob(event));

        List<String[]> messages = KafkaTestUtils.readMessages(dataUsageTestTopicName, consumerProps, 1);
        for (int i = 0; i < messages.size(); i++) {
            IgniteEvent igniteEvent = transformer.fromBlob(messages.get(i)[1].getBytes(),
                    Optional.ofNullable(null));
            DataUsageEventDataV1_0 testDataUsageEventData
                    = (DataUsageEventDataV1_0) igniteEvent.getEventData();
            Assert.assertEquals((double) transformer.toBlob(event).length / Constants.BYTE_1024,
                    testDataUsageEventData.getPayLoadSize(), 0.0);
        }

        shutDownApplication();

    }

    /**
     * Gets the dummy ignite blob event.
     *
     * @param deviceID the device ID
     * @param requestId the request id
     * @param vehicleId the vehicle id
     * @return the dummy ignite blob event
     */
    private IgniteEventImpl getDummyIgniteBlobEvent(String deviceID, String requestId, String vehicleId) {

        IgniteEventImpl igniteBlobEvent = new IgniteEventImpl();
        igniteBlobEvent.setSourceDeviceId(deviceID);
        igniteBlobEvent.setEventId(EventID.SPEED);
        igniteBlobEvent.setRequestId(requestId);
        igniteBlobEvent.setSchemaVersion(Version.V1_0);
        igniteBlobEvent.setTimestamp(System.currentTimeMillis());
        igniteBlobEvent.setVehicleId(vehicleId);
        igniteBlobEvent.setVersion(Version.V1_0);
        SpeedV1_0 speedV10 = new SpeedV1_0();
        speedV10.setValue(TestConstants.HUNDRED_DOUBLE);
        igniteBlobEvent.setEventData(speedV10);
        return igniteBlobEvent;
    }

    /**
     * inner class StreamServiceProcessor implements StreamProcessor.
     */
    public static final class StreamServiceProcessor implements StreamProcessor<byte[], byte[], byte[], byte[]> {
        
        /** The spc. */
        private StreamProcessingContext<byte[], byte[]> spc;

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<byte[], byte[]> spc) {
            this.spc = spc;

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return "StreamServiceProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String fwdValue = new String(kafkaRecord.value()) + "_StreamServiceProcessor";
            spc.forward(new Record<byte[], byte[]>(kafkaRecord.key(), fwdValue.getBytes(), kafkaRecord.timestamp()));
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
     * inner class StreamPostProcessor implements StreamProcessor.
     */
    public static final class StreamPostProcessor implements StreamProcessor<byte[], byte[], byte[], byte[]> {
        
        /** The spc. */
        private StreamProcessingContext<byte[], byte[]> spc;

        /**
         * Instantiates a new stream post processor.
         */
        public StreamPostProcessor() {

        }

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<byte[], byte[]> spc) {
            this.spc = spc;

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return "StreamPostProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String fwdValue = new String(kafkaRecord.value()) + "StreamPostProcessor";
            spc.forward(new Record<byte[], byte[]>(kafkaRecord.key(), fwdValue.getBytes(), kafkaRecord.timestamp()));
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
