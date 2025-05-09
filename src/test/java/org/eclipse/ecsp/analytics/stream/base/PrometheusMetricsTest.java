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

import io.prometheus.client.CollectorRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.discovery.PropBasedDiscoveryServiceImpl;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.domain.AbstractBlobEventData.Encoding;
import org.eclipse.ecsp.domain.BlobDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.IgniteEventSource;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.CompositeIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.transform.GenericIgniteEventTransformer;
import org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;



/**
 * {@link PrometheusMetricsTest} extends {@link KafkaStreamsApplicationTestBase}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/stream-base-test2.properties")
@Ignore("Class not ready for tests")
public class PrometheusMetricsTest extends KafkaStreamsApplicationTestBase {
    
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsTest.class);
    
    /** The Constant KEY_SER. */
    private static final IgniteKeyTransformerStringImpl KEY_SER = new IgniteKeyTransformerStringImpl();
    
    /** The in topic name. */
    private static String inTopicName;
    
    /** The out topic name. */
    private static String outTopicName;
    
    /** The i. */
    private static int i = 0;
    
    /** The transformer. */
    @Autowired
    GenericIgniteEventTransformer transformer;
    /**
     * Prometheus Agent Port Number.
     **/
    @Value("${prometheus.agent.port:9100}")
    private int prometheusExportPort;

    /**
     * Send GET.
     *
     * @param url the url
     * @return the string
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static String sendGET(String url) throws IOException {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", "Mozilla/5.0");
        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            return response.toString();
        } else {
            System.out.println("GET request not worked");
        }
        return null;

    }

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
        inTopicName = "sourceTopic" + i;
        outTopicName = "sinkTopic" + i;
        createTopics(inTopicName, outTopicName);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.String().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.String().serializer().getClass().getName());
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, PropBasedDiscoveryServiceImpl.class.getName());
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, inTopicName);
        ksProps.put(PropertyNames.APPLICATION_ID, "pt");
    }

    /**
     * Testing thread recovery metrics by killing a stream thread and
     * checking the thread recovery count by hitting Prometheus Endpoint.
     *
     * @throws Exception Exception
     */

    @Test
    public void maxExceptionHandlerForReplaceThread() throws Exception {

        ksProps.put(PropertyNames.PRE_PROCESSORS, StreamPreProcessor.class.getName());
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, StreamServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS, StreamPostProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        ksProps.put(PropertyNames.NUM_STREAM_THREADS, "5");

        launchApplication();
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
            if (name.endsWith("-StreamThread-4")) {
                LOGGER.info("Killing thread {} ", name);
                thread.stop();
            }
        }
        await().atMost(Constants.THREAD_SLEEP_TIME_120000, TimeUnit.MILLISECONDS);
        String metricsGet = sendGET("http://localhost:" + prometheusExportPort);
        String threadRecoveryMetric = metricsGet.substring(metricsGet.indexOf(
                "Total number of times thread recovered"));
        if (threadRecoveryMetric.indexOf("#") != Constants.NEGATIVE_ONE) {
            threadRecoveryMetric = threadRecoveryMetric.substring(threadRecoveryMetric.indexOf(
                    "countertotal_number_of_times_thread_recovered"), threadRecoveryMetric.indexOf("# HELP"));
            threadRecoveryMetric = threadRecoveryMetric.substring(threadRecoveryMetric.lastIndexOf(" ") + 1);
        }
        String uncaughtExceptionMetric = metricsGet.substring(metricsGet.indexOf(
                "Total number of times uncaught exception occurs"));
        if (uncaughtExceptionMetric.indexOf("#") != Constants.NEGATIVE_ONE) {
            uncaughtExceptionMetric = uncaughtExceptionMetric.substring(
                    uncaughtExceptionMetric.indexOf("countertotal_number_of_times_uncaught_exception_occurs"),
                    uncaughtExceptionMetric.indexOf("# HELP"));
            uncaughtExceptionMetric = uncaughtExceptionMetric.substring(uncaughtExceptionMetric.lastIndexOf(" ") + 1);
        }
        Assert.assertEquals("1.0", threadRecoveryMetric);
        Assert.assertEquals("1.0", uncaughtExceptionMetric);

    }

    /**
     * Testing client shut down metric by killing  stream threads and
     * checking the client shutdown count by hitting Prometheus Endpoint.
     *
     * @throws Exception Exception
     */
    @Test
    public void maxExceptionHandlerForShutDownClient() throws Exception {
        ksProps.put(PropertyNames.PRE_PROCESSORS, StreamPreProcessor.class.getName());
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, StreamServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS, StreamPostProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        ksProps.put(PropertyNames.NUM_STREAM_THREADS, "5");

        launchApplication();
        ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
        ThreadGroup parentGroup;

        while ((parentGroup = rootGroup.getParent()) != null) {
            rootGroup = parentGroup;
        }
        Thread[] threads = new Thread[rootGroup.activeCount()];
        while (rootGroup.enumerate(threads, true) == threads.length) {
            threads = new Thread[threads.length * Constants.TWO];
        }
        int failureCount = Constants.THREE;
        for (Thread thread : threads) {
            if (thread == null) {
                continue;
            }
            String name = thread.getName();

            if (name.contains("-StreamThread-") && name.startsWith("chaining") && failureCount != 0) {
                LOGGER.info("Killing thread {} ", name);
                thread.stop();
                await().atMost(Constants.THREAD_SLEEP_TIME_30000, TimeUnit.MILLISECONDS);
                failureCount--;
            }
        }
        await().atMost(Constants.THREAD_SLEEP_TIME_60000, TimeUnit.MILLISECONDS);
        String metricsGet = sendGET("http://localhost:" + prometheusExportPort);
        String shutdownClientMetric = metricsGet.substring(metricsGet.indexOf(
                "Total number of times client shuts down"));
        if (shutdownClientMetric.indexOf("#") != Constants.NEGATIVE_ONE) {
            shutdownClientMetric = shutdownClientMetric.substring(shutdownClientMetric.indexOf(
                    "countertotal_number_of_times_client_shuts_down"), shutdownClientMetric.indexOf("# HELP"));
            shutdownClientMetric = shutdownClientMetric.substring(shutdownClientMetric.lastIndexOf(" ") + 1);
        }
        String uncaughtExceptionMetric = metricsGet.substring(metricsGet.indexOf(
                "Total number of times uncaught exception occurs"));
        if (uncaughtExceptionMetric.indexOf("#") != Constants.NEGATIVE_ONE) {
            uncaughtExceptionMetric = uncaughtExceptionMetric.substring(
                    uncaughtExceptionMetric.indexOf("countertotal_number_of_times_uncaught_exception_occurs"),
                    uncaughtExceptionMetric.indexOf("# HELP"));
            uncaughtExceptionMetric = uncaughtExceptionMetric.substring(uncaughtExceptionMetric.lastIndexOf(" ") + 1);
        }
        String threadRecoveryMetric = metricsGet.substring(metricsGet.indexOf(
                "Total number of times thread recovered"));
        if (threadRecoveryMetric.indexOf("#") != Constants.NEGATIVE_ONE) {
            threadRecoveryMetric = threadRecoveryMetric.substring(threadRecoveryMetric.indexOf(
                    "countertotal_number_of_times_thread_recovered"), threadRecoveryMetric.indexOf("# HELP"));
            threadRecoveryMetric = threadRecoveryMetric.substring(threadRecoveryMetric.lastIndexOf(" ") + 1);
        }
        Assert.assertEquals("1.0", shutdownClientMetric);
        Assert.assertEquals("2.0", threadRecoveryMetric);
        Assert.assertEquals("3.0", uncaughtExceptionMetric);

    }

    /**
     * Testing thread liveness metrics by killing a stream thread and
     * checking the thread alive count by hitting Prometheus Endpoint.
     *
     * @throws Exception the exception
     */
    @Test
    public void testThreadCountAfterOneThreadKill() throws Exception {
        ksProps.put(PropertyNames.PRE_PROCESSORS, StreamPreProcessor.class.getName());
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, StreamServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS, StreamPostProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        ksProps.put(PropertyNames.NUM_STREAM_THREADS, "10");
        launchApplication();
        KafkaTestUtils.sendMessages(inTopicName, producerProps, "key", "value1");

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
            if (name.endsWith("-StreamThread-4")) {
                LOGGER.info("Killing thread {} ", name);
                thread.stop();
            }
        }
        await().atMost(Constants.THREAD_SLEEP_TIME_30000, TimeUnit.MILLISECONDS);
        // Get metric and parse for thread count
        String metricsGet = sendGET("http://localhost:" + prometheusExportPort);
        String liveThreadMetric = metricsGet.substring(metricsGet.indexOf("thread_liveness{service"));
        if (liveThreadMetric.indexOf("#") != Constants.NEGATIVE_ONE) {
            liveThreadMetric = liveThreadMetric.substring(0, liveThreadMetric.indexOf("#"));
            assertTrue(liveThreadMetric.endsWith("10.0"));
        } else { // if this is the last metric on Prometheus
            assertTrue(liveThreadMetric.endsWith("9.0"));
        }
        CollectorRegistry.defaultRegistry.clear(); // must clear registry
        // otherwise metrics register
        // in last test launch will
        // conflict
        shutDownApplication();
    }

    /**
     * Testing Processor for service consumption metric.
     *
     * @throws Exception the exception
     */
    @Test
    public void testServiceConsumptionMetric() throws Exception {
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, PropBasedDiscoveryServiceImpl.class.getName());
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, inTopicName);
        ksProps.put(PropertyNames.APPLICATION_ID, "pt");

        ksProps.put("event.transformer.classes", "genericIgniteEventTransformer");
        ksProps.put("ignite.key.transformer.class", "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        ksProps.put("ingestion.serializer.class", "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        ksProps.put("sink.topic.name", outTopicName);

        ksProps.put(PropertyNames.PRE_PROCESSORS,
                "org.eclipse.ecsp.analytics.stream.base.processors.TaskContextInitializer,"
                        + "org.eclipse.ecsp.analytics.stream.base.processors.ProtocolTranslatorPreProcessor");
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, StreamServiceProcessor.class.getName());
        ksProps.put(PropertyNames.POST_PROCESSORS, StreamPostProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "chaining" + System.currentTimeMillis());
        launchApplication();

        launchApplication();
        long pushMessageTimeMs = System.currentTimeMillis() + Constants.THREAD_SLEEP_TIME_10000;
        while (pushMessageTimeMs > System.currentTimeMillis()) {
            IgniteStringKey igniteStringKey = new IgniteStringKey();
            igniteStringKey.setKey("dummy_key");

            IgniteEventImpl event = getDummyIgniteBlobEvent("dummy_ID",
                    "req" + System.currentTimeMillis(), "dummy_vid");
            KafkaTestUtils.sendMessages(inTopicName, producerProps,
                    KEY_SER.toBlob(igniteStringKey), transformer.toBlob(event));
        }

        IgniteEventImpl event = getDummyIgniteBlobEvent("dummy_ID", "req" + System.currentTimeMillis(), "dummy_vid");
        List<IgniteEvent> nestedEvents = new ArrayList<>();
        nestedEvents.add(event);
        CompositeIgniteEvent event2 = new CompositeIgniteEvent();
        event2.setEventId(EventID.COMPOSITE_EVENT);
        event2.setNestedEvents(nestedEvents);

        KafkaTestUtils.sendMessages(inTopicName, producerProps,
                KEY_SER.toBlob(new IgniteStringKey("dummy_id")), transformer.toBlob(event2));
        String metricsGet = sendGET("http://localhost:" + prometheusExportPort);
        String liveThreadMetric = metricsGet.substring(metricsGet.indexOf("service_data_consumption_count{svc="));
        liveThreadMetric = liveThreadMetric.substring(0, liveThreadMetric.indexOf("service_data_consumption_sum{svc="));
        assertNotNull(liveThreadMetric); // assert metric is created.
        CollectorRegistry.defaultRegistry.clear(); // must clear registry
        // otherwise metrics register
        // in last test launch will
        // conflict
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
     * inner class StreamPostProcessor implements StreamProcessor.
     */
    public static final class StreamPostProcessor implements StreamProcessor<byte[], byte[], String, String> {
        
        /** The spc. */
        private StreamProcessingContext<String, String> spc;

        /**
         * Instantiates a new stream post processor.
         */
        public StreamPostProcessor() {
            // Nothing to do.
        }

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<String, String> spc) {
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
            byte[] value = kafkaRecord.value();
            String fwdValue = new String(value) + "_StreamPostProcessor";
            spc.forward(new Record<String, String>(new String(kafkaRecord.key()), fwdValue, kafkaRecord.timestamp()));

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
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] { outTopicName };
        }

    }

    /**
     * inner class class StreamPreProcessor implements StreamProcessor.
     */
    public static final class StreamPreProcessor implements StreamProcessor<byte[], byte[], byte[], byte[]> {
        
        /** The spc. */
        private StreamProcessingContext<byte[], byte[]> spc;

        /**
         * Instantiates a new stream pre processor.
         */
        public StreamPreProcessor() {
            // Nothing to do.
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

            return "StreamPreProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String fwdValue = new String(kafkaRecord.value()) + "_StreamPreProcessor";
            spc.forward(new Record<byte[], byte[]>(kafkaRecord.key(), fwdValue.getBytes(), kafkaRecord.timestamp()));
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
            return new String[] { inTopicName };
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
     * inner class StreamPreProcessor2 implements StreamProcessor.
     */
    public static final class StreamPreProcessor2 implements StreamProcessor<byte[], byte[], byte[], byte[]> {
        
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

            return "StreamPreProcessor_2";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<byte[], byte[]> kafkaRecord) {
            String fwdValue = new String(kafkaRecord.value()) + "_StreamPreProcessor2";
            spc.forward(new Record<byte[], byte[]>(kafkaRecord.key(), fwdValue.getBytes(), kafkaRecord.timestamp()));
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
            return new String[] { inTopicName };
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
     *  class StreamServiceProcessor implements StreamProcessor.
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
            return new String[] { inTopicName };
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
