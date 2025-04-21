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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.discovery.PropBasedDiscoveryServiceImpl;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.stream.dma.handler.MaxFailuresUncaughtExceptionHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Integration test case.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/stream-base-test.properties")
public class KafkaStreamsMaxUncaughtReplaceThreadTest extends KafkaStreamsApplicationTestBase {

    public static int failureCount;
    private static String sourceTopicName;
    private static String sinkTopicName;
    private static int i = 0;
    private KafkaStreams streams;

    @Override
    @Before
    public void setup() throws Exception {

        super.setup();
        i++;
        sourceTopicName = "sourceTopic" + i;
        sinkTopicName = "sinkTopic" + i;
        createTopics(sourceTopicName, sinkTopicName);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                Serdes.ByteArray().serializer().getClass().getName());

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                Serdes.ByteArray().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                Serdes.ByteArray().deserializer().getClass().getName());

        ksProps.put("event.transformer.classes", "genericIgniteEventTransformer");
        ksProps.put("ignite.key.transformer.class", "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        ksProps.put("ingestion.serializer.class", "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        ksProps.put("sink.topic.name", sinkTopicName);
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, PropBasedDiscoveryServiceImpl.class.getName());
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopicName);
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());

    }

    @Test
    public void maxExceptionHandlerForReplaceThread() throws Exception {
        failureCount = TestConstants.TWO;
        startKafkaStreams();
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, "key1".getBytes(StandardCharsets.UTF_8), "value1"
                .getBytes(StandardCharsets.UTF_8));
        Thread.sleep(TestConstants.LONG_120000);
        Map<MetricName, ? extends Metric> map = streams.metrics();

        for (Map.Entry<MetricName, ? extends Metric> metricNameEntry : map.entrySet()) {
            if (metricNameEntry.getKey().name().equalsIgnoreCase("alive-stream-threads")) {
                Assert.assertEquals("4", metricNameEntry.getValue().metricValue().toString());
            }
        }
        Assert.assertEquals("2.0", String.valueOf(MaxFailuresUncaughtExceptionHandler.getThreadRecoveryTotal().get()));
    }

    private void startKafkaStreams() {
        ksProps.put(PropertyNames.NUM_STREAM_THREADS, TestConstants.FOUR);
        final Serializer<byte[]> keySerializer = Serdes.ByteArray().serializer();
        final Serializer<byte[]> valueSerializer = Serdes.ByteArray().serializer();
        ExecutorService service = Executors.newSingleThreadExecutor();
        Runnable runnable = () -> {
            final Topology topology = new Topology();

            topology.addSource("source", sourceTopicName)
                    .addProcessor("preprocess", () -> new StreamPreProcessor(), "source")
                    .addProcessor("process", () -> new StreamServiceProcessor(), "preprocess")
                    .addSink("sink", sinkTopicName, keySerializer, valueSerializer, "process");

            streams = new KafkaStreams(topology, ksProps);
            streams.cleanUp();
            streams.setUncaughtExceptionHandler(new MaxFailuresUncaughtExceptionHandler(TestConstants.THREE, 
                    TestConstants.LONG_30000));
            streams.start();
        };
        service.execute(runnable);
    }

    @After
    public void destroy() {
        streams.close();
    }

    /**
     * Test service processor.
     */
    public static final class StreamServiceProcessor implements Processor<byte[], byte[]> {
        private ProcessorContext spc;

        @Override
        public void init(ProcessorContext context) {
            this.spc = context;
        }

        @Override
        public void process(byte[] key, byte[] value) {
            spc.forward(key, value);
        }

        @Override
        public void close() {

        }

    }
    
    /**
     * Test pre processor.
     */
    public class StreamPreProcessor implements Processor<byte[], byte[]> {
        private ProcessorContext spc;

        public void reset(int value) {
            failureCount = value;
        }

        @Override
        public void init(ProcessorContext context) {
            this.spc = context;
        }

        @Override
        public void process(byte[] key, byte[] value) {
            if (failureCount > 0) {
                failureCount--;
                spc.forward(key, new String(value, StandardCharsets.UTF_8));
            }
            spc.forward(key, value);
        }

        @Override
        public void close() {

        }
    }
}
