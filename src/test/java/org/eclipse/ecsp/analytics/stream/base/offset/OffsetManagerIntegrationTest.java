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

package org.eclipse.ecsp.analytics.stream.base.offset;

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
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Properties;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;



/**
 * class OffsetManagerIntegrationTest extends KafkaStreamsApplicationTestBase.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@TestPropertySource("/offsetmanager-test.properties")
public class OffsetManagerIntegrationTest extends KafkaStreamsApplicationTestBase {

    /** The source topic name. */
    private static String sourceTopicName;
    
    /** The event counter. */
    private static int eventCounter;
    
    /** The vehicle id. */
    private String vehicleId = "Vehicle12345";
    
    /** The offset persistence init delay. */
    @Value("${" + PropertyNames.KAFKA_STREAMS_OFFSET_PERSISTENCE_INIT_DELAY + ":10000}")
    private int offsetPersistenceInitDelay;
    
    /** The offset persistence delay. */
    @Value("${" + PropertyNames.KAFKA_STREAMS_OFFSET_PERSISTENCE_DELAY + ":60000}")
    private int offsetPersistenceDelay;
    
    /** The offset dao. */
    @Autowired
    private KafkaStreamsOffsetManagementDAOMongoImpl offsetDao;

    /**
     * setUp().
     *
     * @throws Exception Exception.
     */
    @Before
    public void setup() throws Exception {
        super.setup();
        sourceTopicName = "raw-events";
        createTopics(sourceTopicName);
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopicName);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        KafkaStreamsTopicOffset saveOffset =
                new KafkaStreamsTopicOffset(sourceTopicName, 0, TestConstants.LONG_50);
        offsetDao.save(saveOffset);
    }

    // Following parameters have been changed specifically for this test case in
    // offsetmanager-test.properties
    // kafka.streams.offset.persistence.delay=1000
    // kafka.streams.offset.persistence.init.delay=10
    // kafka.streams.offset.persistence.enabled=true
    // start.device.status.consumer=false
    /**
     * Test.
     *
     * @throws Exception the exception
     */
    // start.dff.feed.consumer=false
    @Test
    public void test() throws Exception {
        eventCounter = 0;
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, OffsetManagerServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "offset-sp" + System.currentTimeMillis());
        launchApplication();
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_10000, MILLISECONDS)).join();

        String event1 = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": "
                + "{\"value\":20.0},\"MessageId\":\"1237\",\"BizTransactionId\": \"Biz1237\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        for (int i = 0; i < TestConstants.THREAD_SLEEP_TIME_100; i++) {
            KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(), event1.getBytes());
        }

        long bufferTime = TestConstants.THREAD_SLEEP_TIME_5000;
        long sleepTime = offsetPersistenceInitDelay + offsetPersistenceDelay + bufferTime;
        runAsync(() -> {}, delayedExecutor(sleepTime, MILLISECONDS)).join();

        List<KafkaStreamsTopicOffset> list = offsetDao.findAll();
        Assert.assertEquals(1, list.size());
        KafkaStreamsTopicOffset offset = list.get(0);
        Assert.assertEquals(sourceTopicName, offset.getKafkaTopic());
        Assert.assertEquals(0, offset.getPartition());
        Assert.assertEquals(TestConstants.LONG_99, offset.getOffset());
        Assert.assertEquals(Constants.FIFTY, eventCounter);

    }

    /**
     * inner class OffsetManagerServiceProcessor implements IgniteEventStreamProcessor.
     */
    public static final class OffsetManagerServiceProcessor implements IgniteEventStreamProcessor {
        
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
            return "OffsetManagerServiceProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            eventCounter++;
            spc.forward(kafkaRecord);
        }

        /**
         * Punctuate.
         *
         * @param timestamp the timestamp
         */
        @Override
        public void punctuate(long timestamp) {
            // Nothing to do.
        }

        /**
         * Close.
         */
        @Override
        public void close() {
            // Nothing to do.
        }

        /**
         * Config changed.
         *
         * @param props the props
         */
        @Override
        public void configChanged(Properties props) {
            // Nothing to do.
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
