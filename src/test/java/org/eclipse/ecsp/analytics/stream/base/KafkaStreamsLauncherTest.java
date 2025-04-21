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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.ecsp.analytics.stream.base.dao.GenericDAO;
import org.eclipse.ecsp.analytics.stream.base.discovery.StreamProcessorDiscoveryService;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.stores.MapObjectStateStore;
import org.eclipse.ecsp.analytics.stream.base.stores.ObjectStateStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutStringRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * class KafkaStreamsLauncherTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/stream-base-test.properties")
public class KafkaStreamsLauncherTest extends KafkaStreamsApplicationTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsLauncherTest.class);
    private static final String[] TOPIC_NAMES = new String[] { null, null, null };
    private static int i = 0;
    private String inTopicName;
    private String outTopicName;
    private String additionalInTopicName;
    @Autowired
    private RedissonClient redissonClient;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        i++;
        inTopicName = "raw-events-" + i;
        outTopicName = "output-topic-" + i;
        additionalInTopicName = "input-alerts-" + i;
        createTopics(inTopicName, additionalInTopicName, outTopicName);
        TOPIC_NAMES[0] = inTopicName;
        TOPIC_NAMES[1] = outTopicName;
        TOPIC_NAMES[Constants.TWO] = additionalInTopicName;
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.String().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.String().serializer().getClass().getName());
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, SimpleTestServiceDiscoveryImpl.class.getName());
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, inTopicName);
        ksProps.put(PropertyNames.APPLICATION_ID, "pt");
    }

    @Test
    public void testSetup() throws TimeoutException, ExecutionException, InterruptedException {
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        List<String[]> messages = KafkaTestUtils.readMessages(inTopicName, consumerProps, 
                Constants.THREAD_SLEEP_TIME_100);
        Assert.assertEquals(1, messages.size());
    }

    @Test
    public void testSingleProcessor() throws Exception {
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, SimpleTestServiceDiscoveryImpl.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "pt");
        launchApplication();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
        List<String[]> messages = KafkaTestUtils.getMessages(outTopicName,
                consumerProps, 1, Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals("key1", messages.get(0)[0]);
        Assert.assertEquals("value1", messages.get(0)[1]);
        shutDown();
    }

    @Test
    public void testSingleProcessorUsingIgniteCache() throws Exception {
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, IgniteCacheTestServiceDiscoveryImpl.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "ptRedis" + System.currentTimeMillis());
        launchApplication();
        String key = "key" + System.currentTimeMillis();
        String value = "value1";
        String actualValue = (String) redissonClient.getBucket(key).get();
        Assert.assertNull(actualValue);
        KafkaTestUtils.sendMessages(inTopicName, producerProps, key, value);
        Thread.sleep(Constants.THREAD_SLEEP_TIME_5000);
        actualValue = retryWithException(Constants.TEN, (v) -> {
            return (String) redissonClient.getBucket(key).get();
        });
        Assert.assertNotNull(actualValue);
        Assert.assertEquals(value, actualValue);
        shutDown();
    }

    @Test
    public void testSingleProcessorWithState() throws Exception {
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, StateTestServiceDiscoveryImpl.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "pts" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
        Thread.sleep(Constants.THREAD_SLEEP_TIME_10000);
        List<String[]> messages = KafkaTestUtils.getMessages(outTopicName, consumerProps, 1, 
                Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals("key1", messages.get(0)[0]);
        Assert.assertEquals("value1", messages.get(0)[1]);
        shutDown();
    }

    /**
     * The test case is same as testSingleProcessorWithState, only difference is
     * that in this case Streamprocessor is using hashmap as its
     * state store instead of rocksDB.
     *
     * @throws TimeoutException TimeoutException
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     * @throws ExecutionException ExecutionException
     */
    @Test
    public void testSingleProcessorWithHashMapAsState() throws Exception, InterruptedException, ExecutionException {
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, HashMapStateTestServiceDiscoveryImpl.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "pts" + System.currentTimeMillis());
        // set the state.store.type property to map
        ksProps.put(PropertyNames.STATE_STORE_TYPE, "map");
        launchApplication();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
        List<String[]> messages = KafkaTestUtils.getMessages(outTopicName,
                consumerProps, 1, Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals("key1", messages.get(0)[0]);
        Assert.assertEquals("value1", messages.get(0)[1]);
        shutDown();
    }

    /*
     * 2 input topics. 2 output topics. Message from first topic goes to output
     * topic + '-1'. Message from second topic goes to output topic+'-2'
     */
    @Test
    public void testSingleProcessorMultipleSourcesMultipleSinks()
            throws Exception {
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, MultipleSourcesServiceDiscoveryImpl.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "pts" + System.currentTimeMillis());
        launchApplication();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
        KafkaTestUtils.sendMessages(TOPIC_NAMES[Constants.TWO], producerProps, "key2", "value2");
        Thread.sleep(Constants.TWO_THOUSAND);
        List<String[]> messages = KafkaTestUtils.getMessages(TOPIC_NAMES[1] + "-1", consumerProps, 1, 
                Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals(1, messages.size());
        Assert.assertEquals("key1", messages.get(0)[0]);
        Assert.assertEquals("value1", messages.get(0)[1]);
        Thread.sleep(Constants.TWO_THOUSAND);
        messages = KafkaTestUtils.getMessages(TOPIC_NAMES[1] + "-2", consumerProps, 1, 
                Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals(1, messages.size());
        Assert.assertEquals("key2", messages.get(0)[0]);
        Assert.assertEquals("value2", messages.get(0)[1]);
        shutDown();
    }

    /**
     * Simple test case to verify the initialization of Map state store and map iterator.
     */
    // @Test
    public void testMapStateStore() {
        MapObjectStateStore mapStore = new MapObjectStateStore();

        String key1 = "key1";
        String val1 = "val1";

        String key2 = "key2";
        String val2 = "val2";

        // Push the key value to map store
        mapStore.put(key1, val1);
        mapStore.put(key2, val2);

        // initialize the result map
        Map<String, Object> resultMap = new HashMap<String, Object>();

        // get the iterator
        KeyValueIterator<String, Object> iter = null;
        iter = mapStore.all();
        while (iter.hasNext()) {
            KeyValue<String, Object> keyValue = iter.next();
            resultMap.put(keyValue.key, keyValue.value);
        }

        Assert.assertEquals("The size of key value pairs doesn't match.", Constants.TWO, resultMap.size());
        Assert.assertEquals("Key1 value doesn't match", "val1", resultMap.get(key1));
        Assert.assertEquals("Key2 value doesn't match", "val2", resultMap.get(key2));

        // clear the result map
        resultMap.clear();

        // Remove key1
        mapStore.delete("key1");

        // Now iterate
        iter = mapStore.all();
        while (iter.hasNext()) {
            KeyValue<String, Object> keyValue = iter.next();
            resultMap.put(keyValue.key, keyValue.value);
        }

        Assert.assertEquals("The size of key value pairs doesn't match.", 1, resultMap.size());
        Assert.assertEquals("Key2 value doesn't match", "val2", resultMap.get(key2));

    }

    /**
     * testSingleProcessorUsingKafkaStreams().
     *
     * @throws TimeoutException TimeoutException
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     * @throws ExecutionException ExecutionException
     */
    public void testSingleProcessorUsingKafkaStreams() throws TimeoutException,
            IOException, InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(PropertyNames.APPLICATION_ID, "passthrough-test-" + System.currentTimeMillis());
        props.put(PropertyNames.BOOTSTRAP_SERVERS, KAFKA_CLUSTER.bootstrapServers());
        props.put(PropertyNames.ZOOKEEPER_CONNECT, KAFKA_CLUSTER.zkconnectstring());
        props.put(PropertyNames.NUM_STREAM_THREADS, "1");
        props.put(PropertyNames.REPLICATION_FACTOR, "1");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        KafkaTestUtils.purgeLocalStreamsState(props);
        try (var streams = new KafkaStreams(new Topology().addSource("source", inTopicName)
                .addProcessor("pass-through", new ProcessorSupplier<byte[], byte[], String, String>() {
                    @Override
                    public Processor<byte[], byte[], String, String> get() {
                        return new Processor<byte[], byte[], String, String>() {
                            private ProcessorContext<String, String> ctx;

                            @Override
                            public void init(ProcessorContext<String, String> context) {
                                this.ctx = context;
                            }

                            @Override
                            public void process(Record<byte[], byte[]> kafkaRecord) {

                                ctx.forward(new Record<>(new String(kafkaRecord.key(), StandardCharsets.UTF_8),
                                        new String(kafkaRecord.value(), StandardCharsets.UTF_8),
                                        System.currentTimeMillis()));
                            }

                            public void punctuate(long timestamp) {
                                //
                            }

                            @Override
                            public void close() {
                                //
                            }
                        };
                    }

                }, "source")
                .addSink("sink", outTopicName, "pass-through"), props)) {
            streams.start();
            KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
            Thread.sleep(Constants.THREAD_SLEEP_TIME_4000);
            List<String[]> messages = KafkaTestUtils.getMessages(outTopicName, consumerProps, 1, 
                    Constants.THREAD_SLEEP_TIME_10000);
            Assert.assertEquals("key1", messages.get(0)[0]);
            Assert.assertEquals("value1", messages.get(0)[1]);
        }
        ;
    }

    /**
     * testSingleProcessorStateUsingKafkaStreams().
     *
     * @throws TimeoutException TimeoutException
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     * @throws ExecutionException ExecutionException
     */
    public void testSingleProcessorStateUsingKafkaStreams() throws TimeoutException,
            IOException, InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(PropertyNames.APPLICATION_ID, "passthrough-test-" + System.currentTimeMillis());
        // RTC-141484 - Kafka version upgrade from 1.0.0. to
        // 2.2.0 changes
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(PropertyNames.BOOTSTRAP_SERVERS, KAFKA_CLUSTER.bootstrapServers());
        props.put(PropertyNames.ZOOKEEPER_CONNECT, KAFKA_CLUSTER.zkconnectstring());
        props.put(PropertyNames.NUM_STREAM_THREADS, "1");
        props.put(PropertyNames.REPLICATION_FACTOR, "1");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        KafkaTestUtils.purgeLocalStreamsState(props);
        try (var streams = new KafkaStreams(new Topology().addSource("source", inTopicName)
                .addProcessor("pass-through", new ProcessorSupplier<String, String, String, String>() {
                    @Override
                    public Processor<String, String, String, String> get() {
                        return new Processor<String, String, String, String>() {
                            private ProcessorContext<String, String> ctx;

                            @Override
                            public void init(ProcessorContext<String, String> context) {
                                this.ctx = context;
                            }

                            @Override
                            public void process(Record<String, String> kafkaRecord) {
                                ctx.forward(kafkaRecord);
                            }

                            public void punctuate(long timestamp) {
                                //
                            }

                            @Override
                            public void close() {
                                //
                            }
                        };
                    }

                }, "source")
                .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("state"),
                                Serdes.String(), Serdes.String()),
                        "pass-through")
                .addSink("sink", outTopicName, "pass-through"), props)) {
            streams.start();
            KafkaTestUtils.sendMessages(inTopicName, producerProps, "key1", "value1");
            List<String[]> messages = KafkaTestUtils.getMessages(outTopicName,
                    consumerProps, 1, Constants.THREAD_SLEEP_TIME_10000);
            Assert.assertEquals("key1", messages.get(0)[0]);
            Assert.assertEquals("value1", messages.get(0)[1]);
        }
    }

    @Test
    public void testMetricsOfKafkaStreams() throws Exception {
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, SimpleTestServiceDiscoveryImpl.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "pt");
        ksProps.put(PropertyNames.NUM_STREAM_THREADS, 1);

        launchApplication();
        Map<MetricName, ? extends Metric> map = ctx.getBean(KafkaStreamsLauncher.class).getStreams().metrics();

        for (Map.Entry<MetricName, ? extends Metric> metricNameEntry : map.entrySet()) {
            if (metricNameEntry.getKey().name().equalsIgnoreCase("alive-stream-threads")) {
                Assert.assertEquals("4", metricNameEntry.getValue().metricValue().toString());
            }
        }
        killThreads(1);
        for (Map.Entry<MetricName, ? extends Metric> metricNameEntry : map.entrySet()) {
            if (metricNameEntry.getKey().name().equalsIgnoreCase("alive-stream-threads")) {
                Assert.assertEquals("4", metricNameEntry.getValue().metricValue().toString());
            }
        }
        shutDownApplication();
    }

    private void killThreads(int threadCount) {
        ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
        ThreadGroup parentGroup;

        while ((parentGroup = rootGroup.getParent()) != null) {
            rootGroup = parentGroup;
        }
        Thread[] threads = new Thread[rootGroup.activeCount()];
        while (rootGroup.enumerate(threads, true) == threads.length) {
            threads = new Thread[threads.length * Constants.TWO];
        }

        for (Thread thread : threads) {
            if (thread == null) {
                continue;
            }
            String name = thread.getName();
            if (name.endsWith("-StreamThread-" + threadCount)) {
                LOGGER.info("Killing thread {} ", name);
                thread.stop();
            }
        }

    }

    /**
     * class CustomTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService.
     */
    public static final class CustomTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService {
        @Override
        public List<StreamProcessor<?, ?, ?, ?>> discoverProcessors() {
            return Arrays.asList(new CustomTestProcessor());
        }
    }

    /**
     * class CustomTestProcessor implements StreamProcessor.
     */
    @Component
    public static final class CustomTestProcessor implements StreamProcessor<byte[], byte[], String, String> {

        ObjectMapper objectMapper = new ObjectMapper();
        private StreamProcessingContext<String, String> spc;
        private List<String> dtcList = null;
        private GenericDAO dao = null;

        @Override
        public void init(StreamProcessingContext<String, String> spc) {
            this.spc = spc;
            dtcList = dao.getRecords("", "v01", "DTC");
        }

        @Override
        public String name() {
            return "CustomTestProcessor";
        }

        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            StringBuilder dtcs = new StringBuilder();

            for (String dtc : dtcList) {
                dtcs.append(dtc).append(",");
            }
            if (dtcs.length() > 0) {
                dtcs.deleteCharAt(dtcs.length() - 1);
            }
            spc.forward(new Record<>("DTC", dtcs.toString(), kafkaRecord.timestamp()));
        }

        @Override
        public void punctuate(long timestamp) {
        }

        @Override
        public void close() {
        }

        @Override
        public String[] sinks() {
            return new String[] { TOPIC_NAMES[1] };
        }

        @Override
        public void configChanged(Properties props) {
        }

        @Override
        public HarmanPersistentKVStore createStateStore() {
            return null;
        }

        @Override
        public void updateSharedData(Object key, Object value, String streamName) {
            try {
                @SuppressWarnings("rawtypes")
                HashMap message = objectMapper.readValue(String.valueOf(value), HashMap.class);
                @SuppressWarnings("rawtypes")
                List list = (List) (((Map) message.get("v01")).get("DTC"));

                dtcList.addAll(list);
            } catch (IOException e) {
                LOGGER.error("Exception occurred while parsing value : " + value, e);
            }
        }

        @Override
        public void setExternalSharedDataSource(GenericDAO dao) {
            this.dao = dao;
        }
    }

    /**
     * class SimpleTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService.
     */
    @Component
    public static final class SimpleTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService {
        @Override
        public List<StreamProcessor<?, ?, ?, ?>> discoverProcessors() {
            return Arrays.asList(new PassThroughTestProcessor());
        }
    }

    /**
     * class IgniteCacheTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService.
     */
    @Component
    public static final class IgniteCacheTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService {
        @Override
        public List<StreamProcessor<?, ?, ?, ?>> discoverProcessors() {
            return Arrays.asList(new IgniteCacheTestProcessor());
        }
    }

    /**
     * innerc class  StateTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService.
     */
    @Component
    public static final class StateTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService {
        @Override
        public List<StreamProcessor<?, ?, ?, ?>> discoverProcessors() {
            return Arrays.asList(new StreamProcessorWithStateStore());
        }
    }

    /**
     * class StreamProcessorWithStateStore implements StreamProcessor.
     */
    @Component
    public static final class StreamProcessorWithStateStore implements StreamProcessor<byte[], byte[], String, Object> {
        private StreamProcessingContext<String, Object> spc;
        private KeyValueStore objectStore;

        @Override
        public void init(StreamProcessingContext<String, Object> spc) {
            this.spc = spc;
            objectStore = spc.getStateStore("state");
        }

        @Override
        public String name() {
            return "state-proc";
        }

        /**
         * process().
         *
         * @param kafkaRecord kafkaRecord
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            objectStore.put(new String(kafkaRecord.key()), new String(kafkaRecord.value()));
            String value = String.valueOf(objectStore.get(new String(kafkaRecord.key())));
            spc.forward(new Record<>(new String(kafkaRecord.key()),
                    new String(kafkaRecord.value()), System.currentTimeMillis()));
        }

        @Override
        public void punctuate(long timestamp) {
            KeyValueIterator<String, Object> i = objectStore.all();
            if (i.hasNext()) {
                KeyValue<String, Object> kv = i.next();
                spc.forward(new Record<>(kv.key, kv.value, System.currentTimeMillis()));
            }
        }

        @Override
        public void close() {
        }

        @Override
        public void configChanged(Properties props) {
        }

        @Override
        public HarmanPersistentKVStore createStateStore() {
            return new ObjectStateStore("state", true);
        }

        @Override
        public String[] sinks() {
            return new String[] { TOPIC_NAMES[1] };
        }
    }

    /**
     * class PassThroughTestProcessor implements StreamProcessor.
     */
    @Component
    public static final class PassThroughTestProcessor implements StreamProcessor<byte[], byte[], String, String> {
        private StreamProcessingContext<String, String> spc;
        private Properties config;

        public Properties getConfig() {
            return config;
        }

        @Override
        public void init(StreamProcessingContext<String, String> spc) {
            Assert.assertNotNull(config);
            Assert.assertFalse(config.isEmpty());
            this.spc = spc;
        }

        @Override
        public String name() {
            return "simple";
        }

        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String streamName = spc.streamName();
            int partition = spc.partition();
            long offset = spc.offset();
            System.out.println("Record metaData - streamName : " + streamName
                    + ", partition : " + partition + ", offset : " + offset);
            spc.forward(new Record<>(new String(kafkaRecord.key()),
                    new String(kafkaRecord.value()), kafkaRecord.timestamp()));
        }

        @Override
        public void punctuate(long timestamp) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configChanged(Properties props) {
        }

        @Override
        public void initConfig(Properties props) {
            this.config = props;
        }

        @Override
        public HarmanPersistentKVStore createStateStore() {
            return null;
        }

        @Override
        public String[] sinks() {
            return new String[] { TOPIC_NAMES[1] };
        }

    }

    /**
     * class IgniteCacheTestProcessor implements StreamProcessor.
     */
    @Component
    public static final class IgniteCacheTestProcessor implements StreamProcessor<byte[], byte[], byte[], byte[]> {
        private StreamProcessingContext<byte[], byte[]> spc;
        private Properties config;
        @Autowired
        private IgniteCache igniteCache;

        public Properties getConfig() {
            return config;
        }

        @Override
        public void init(StreamProcessingContext<byte[], byte[]> spc) {
            Assert.assertNotNull(config);
            Assert.assertFalse(config.isEmpty());
            this.spc = spc;
        }

        @Override
        public String name() {
            return "simple";
        }

        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            igniteCache.putString(new PutStringRequest().withKey(new String(kafkaRecord.key()))
                    .withValue(new String(kafkaRecord.value())).withNamespaceEnabled(false));
        }

        @Override
        public void punctuate(long timestamp) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configChanged(Properties props) {
        }

        @Override
        public void initConfig(Properties props) {
            this.config = props;
        }

        @Override
        public HarmanPersistentKVStore createStateStore() {
            return null;
        }

        @Override
        public String[] sinks() {
            return new String[] { TOPIC_NAMES[1] };
        }

    }

    /**
     * inner class MultipleSourcesServiceDiscoveryImpl implements StreamProcessorDiscoveryService.
     */
    public static class MultipleSourcesServiceDiscoveryImpl implements StreamProcessorDiscoveryService {

        @Override
        public List<StreamProcessor<?, ?, ?, ?>> discoverProcessors() {
            return Arrays.asList(new StreamProcessor[] { new StreamProcessoWithMultipleSourcesAndSinks()
            });
        }
    }

    /**
     * class StreamProcessoWithMultipleSourcesAndSinks
     *             implements StreamProcessor.
     */
    @Component
    public static class StreamProcessoWithMultipleSourcesAndSinks
            implements StreamProcessor<byte[], byte[], String, String> {
        private StreamProcessingContext<String, String> spc = null;

        @Override
        public void init(StreamProcessingContext<String, String> spc) {
            this.spc = spc;
        }

        @Override
        public String name() {
            return "multi-source-pass-through";
        }

        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String sinkName = null;
            if (spc.streamName().equals(TOPIC_NAMES[0])) {
                sinkName = TOPIC_NAMES[1] + "-1";
            } else {
                sinkName = TOPIC_NAMES[1] + "-2";
            }
            System.out.println("sinkName: " + sinkName);
            spc.forward(new Record<>(new String(kafkaRecord.key()),
                    new String(kafkaRecord.value()), kafkaRecord.timestamp()), sinkName);
        }

        @Override
        public void punctuate(long timestamp) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configChanged(Properties props) {
        }

        @Override
        public HarmanPersistentKVStore createStateStore() {
            return null;
        }

        @Override
        public String[] sources() {
            return new String[] { TOPIC_NAMES[0], TOPIC_NAMES[Constants.TWO] };
        }

        @Override
        public String[] sinks() {
            return new String[] { TOPIC_NAMES[1] + "-1", TOPIC_NAMES[1] + "-2" };
        }
    }

    /**
     * class HashMapStateTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService.
     */
    public static final class HashMapStateTestServiceDiscoveryImpl implements StreamProcessorDiscoveryService {
        @Override
        public List<StreamProcessor<?, ?, ?, ?>> discoverProcessors() {
            return Arrays.asList(new StreamProcessorWithHashMapStateStore());
        }
    }

    /**
     * This stream processor uses hash map as a state store.
     * Note that the createStateStore method is return null.
     * The functionality of the class is as same as StreamProcessorWithStateStore class.
     */
    @Component
    public static final class StreamProcessorWithHashMapStateStore
            implements StreamProcessor<byte[], byte[], String, Object> {
        private StreamProcessingContext<String, Object> spc;
        private KeyValueStore<String, Object> objectStore;

        @Override
        public void init(StreamProcessingContext<String, Object> spc) {
            this.spc = spc;
            objectStore = spc.getStateStore("state");
            // RTC-141484 - Kafka version upgrade from 1.0.0. to
            // 2.2.0 changes
            spc.schedule(Constants.THREAD_SLEEP_TIME_500, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {

                public void punctuate(long timestamp) {
                    KeyValueIterator<String, Object> i = objectStore.all();
                    if (i.hasNext()) {
                        KeyValue<String, Object> kv = i.next();
                        spc.forward(new Record<>(kv.key, kv.value, System.currentTimeMillis()));
                    }
                }
            });
        }

        @Override
        public String name() {
            return "hashmap-state-proc";
        }

        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            objectStore.put(new String(kafkaRecord.key()), new String(kafkaRecord.value()));
        }

        @Override
        public void punctuate(long timestamp) {
            KeyValueIterator<String, Object> i = objectStore.all();
            if (i.hasNext()) {
                KeyValue<String, Object> kv = i.next();
                spc.forward(new Record<>(kv.key, kv.value, System.currentTimeMillis()));
            }
        }

        @Override
        public void close() {
        }

        @Override
        public void configChanged(Properties props) {
        }

        @Override
        public HarmanPersistentKVStore createStateStore() {
            return null;
        }

        @Override
        public String[] sinks() {
            return new String[] { TOPIC_NAMES[1] };
        }
    }

}
