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

package org.eclipse.ecsp.analytics.stream.base.processors;

import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * test class for {@link MsgSeqPreProcessor}.
 */

public class MsgSeqPreProcessorTest {

    /** The Constant TASK_ID. */
    private static final String TASK_ID = "msg-seq-task-1";
    
    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(MsgSeqPreProcessorTest.class);
    
    /** The spc. */
    private MsgSeqStreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /**
     * Setup.
     */
    @Before
    public void setup() {

    }

    /**
     * Test discard old message.
     */
    @Test
    public void testDiscardOldMessage() {
        MsgSeqPreProcessor msgSeqPreProcessor = new MsgSeqPreProcessor();
        spc = new MsgSeqStreamProcessingContext<IgniteKey<?>, IgniteEvent>(msgSeqPreProcessor);
        msgSeqPreProcessor.setStateStore(new TestMsgSeqStreamStateStore());
        msgSeqPreProcessor.setMsgSeqTimeIntInMillis(Constants.THREAD_SLEEP_TIME_200);
        msgSeqPreProcessor.setMsgSeqTopicName("test-seq-topic");
        msgSeqPreProcessor.setSequenceBufferImplClass("org.eclipse.ecsp.analytics."
             + "stream.base.SequenceBufferTreeMapImpl");
        msgSeqPreProcessor.init(spc);

        IgniteStringKey igniteStringKey1 = new IgniteStringKey();
        igniteStringKey1.setKey("TestMsgSeq1");

        IgniteEventImpl eventImpl1 = new IgniteEventImpl();
        eventImpl1.setTimestamp(System.currentTimeMillis());
        eventImpl1.setEventId(EventID.SPEED);
        eventImpl1.setRequestId("e1");

        msgSeqPreProcessor.process(new Record<>(igniteStringKey1, eventImpl1, System.currentTimeMillis()));

        Assert.assertEquals(0, spc.getListOfEventsAfterOrdering().size());
    }

    /**
     * Test disabled ordering.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testDisabledOrdering() throws InterruptedException {
        MsgSeqPreProcessor msgSeqPreProcessor = new MsgSeqPreProcessor();
        spc = new MsgSeqStreamProcessingContext<IgniteKey<?>, IgniteEvent>(msgSeqPreProcessor);
        msgSeqPreProcessor.setStateStore(new TestMsgSeqStreamStateStore());
        msgSeqPreProcessor.setMsgSeqTimeIntInMillis(0);
        msgSeqPreProcessor.setMsgSeqTopicName("test-seq-topic");
        msgSeqPreProcessor.setSequenceBufferImplClass("org.eclipse.ecsp.analytics."
             + "stream.base.SequenceBufferTreeMapImpl");
        msgSeqPreProcessor.init(spc);

        long currentTime = System.currentTimeMillis();

        IgniteStringKey igniteStringKey1 = new IgniteStringKey();
        igniteStringKey1.setKey("TestMsgSeq1");

        IgniteEventImpl eventImpl1 = new IgniteEventImpl();
        eventImpl1.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_100);
        eventImpl1.setEventId(EventID.SPEED);
        eventImpl1.setRequestId("e1");

        IgniteStringKey igniteStringKey2 = new IgniteStringKey();
        igniteStringKey2.setKey("TestMsgSeq2");

        IgniteEventImpl eventImpl2 = new IgniteEventImpl();
        eventImpl2.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_200);
        eventImpl2.setEventId(EventID.SPEED);
        eventImpl2.setRequestId("e2");

        IgniteStringKey igniteStringKey3 = new IgniteStringKey();
        igniteStringKey3.setKey("TestMsgSeq3");

        IgniteEventImpl eventImpl3 = new IgniteEventImpl();
        eventImpl3.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_300);
        eventImpl3.setEventId(EventID.SPEED);
        eventImpl3.setRequestId("e3");

        IgniteStringKey igniteStringKey4 = new IgniteStringKey();
        igniteStringKey4.setKey("TestMsgSeq4");

        IgniteEventImpl eventImpl4 = new IgniteEventImpl();
        eventImpl4.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_4000);
        eventImpl4.setEventId(EventID.SPEED);
        eventImpl4.setRequestId("e4");

        // Send messaging randomly, but it should reach in sequence e1,e2,e3,e4
        msgSeqPreProcessor.process(new Record<>(igniteStringKey4, eventImpl4, System.currentTimeMillis()));
        msgSeqPreProcessor.process(new Record<>(igniteStringKey2, eventImpl2, System.currentTimeMillis()));
        msgSeqPreProcessor.process(new Record<>(igniteStringKey1, eventImpl1, System.currentTimeMillis()));
        msgSeqPreProcessor.process(new Record<>(igniteStringKey3, eventImpl3, System.currentTimeMillis()));
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_5000, MILLISECONDS)).join();

        Assert.assertEquals("e4", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());
        Assert.assertEquals("e2", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());
        Assert.assertEquals("e1", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());
        Assert.assertEquals("e3", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());

    }

    /**
     * Test ordering.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testOrdering() throws InterruptedException {
        logger.info("Test Ordering is started.");
        MsgSeqPreProcessor msgSeqPreProcessor = new MsgSeqPreProcessor();
        spc = new MsgSeqStreamProcessingContext(msgSeqPreProcessor);
        msgSeqPreProcessor.setStateStore(new TestMsgSeqStreamStateStore());
        msgSeqPreProcessor.setMsgSeqTopicName("test-seq-topic");
        msgSeqPreProcessor.setSequenceBufferImplClass("org.eclipse.ecsp.analytics."
             + "stream.base.SequenceBufferTreeMapImpl");
        // Set as 3 sec, buffer interval
        // Flush thread starts after 3 Sec
        msgSeqPreProcessor.setMsgSeqTimeIntInMillis(Constants.THREE * Constants.THREAD_SLEEP_TIME_1000);
        msgSeqPreProcessor.init(spc);

        long currentTime = System.currentTimeMillis();

        IgniteStringKey igniteStringKey1 = new IgniteStringKey();
        igniteStringKey1.setKey("TestMsgSeq1");

        // Start bucketing of the events between 3 secs [first bucket]

        IgniteStringKey igniteStringKey2 = new IgniteStringKey();
        igniteStringKey2.setKey("TestMsgSeq2");
        
        final IgniteEventImpl eventImpl1 = getIgniteEvent(currentTime);
        
        IgniteEventImpl eventImpl2 = new IgniteEventImpl();

        // Set event time after 1.5 sec
        eventImpl2.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_1500);
        eventImpl2.setEventId(EventID.SPEED);
        eventImpl2.setRequestId("e2");
        eventImpl2.setSourceDeviceId("deviceId2");

        IgniteStringKey igniteStringKey3 = new IgniteStringKey();
        igniteStringKey3.setKey("TestMsgSeq3");

        IgniteEventImpl eventImpl3 = new IgniteEventImpl();
        // Set event time after 2 sec
        eventImpl3.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_2000);
        eventImpl3.setEventId(EventID.SPEED);
        eventImpl3.setRequestId("e3");
        eventImpl3.setSourceDeviceId("deviceId3");
        // End bucketing of the events between 3 secs [first bucket]

        // Start bucketing of the events next 3 secs [Second bucket]
        currentTime += Constants.THREAD_SLEEP_TIME_3000;
        IgniteStringKey igniteStringKey4 = new IgniteStringKey();
        igniteStringKey4.setKey("TestMsgSeq4");

        IgniteEventImpl eventImpl4 = new IgniteEventImpl();
        eventImpl4.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_1000);
        eventImpl4.setEventId(EventID.SPEED);
        eventImpl4.setRequestId("e4");
        eventImpl4.setSourceDeviceId("deviceId4");

        IgniteStringKey igniteStringKey5 = new IgniteStringKey();
        igniteStringKey5.setKey("TestMsgSeq5");

        IgniteEventImpl eventImpl5 = new IgniteEventImpl();
        eventImpl5.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_2000);
        eventImpl5.setEventId(EventID.SPEED);
        eventImpl5.setRequestId("e5");
        eventImpl5.setSourceDeviceId("deviceId5");

        // Start bucketing of the events next 3 secs [Second bucket]

        // Send messaging randomly, but it should reach in sequence: first
        // bucket- [e1,e2,e3] second bucket- [e4,e5]
        msgSeqPreProcessor.process(new Record<>(igniteStringKey4, eventImpl4, System.currentTimeMillis()));
        msgSeqPreProcessor.process(new Record<>(igniteStringKey1, eventImpl1, System.currentTimeMillis()));
        msgSeqPreProcessor.process(new Record<>(igniteStringKey2, eventImpl2, System.currentTimeMillis()));

        // Purposefully send the third event after 4000 milis. Bucket holds the
        // events just double the configured interval, so that boundary event is
        // also ordered.
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_4000, MILLISECONDS)).join();
        msgSeqPreProcessor.process(new Record<>(igniteStringKey3, eventImpl3, System.currentTimeMillis()));
        msgSeqPreProcessor.process(new Record<>(igniteStringKey5, eventImpl5, System.currentTimeMillis()));

        // Flush Event will every 3 sec (as per above configuration). Flush
        // event's timestamp is just behind the configured bucket interval.
        // Because of that need to wait for near to double time to flush the
        // events. After 6000 milis, second bucket will flush, becasuse of that
        // only wait still 5900 to flush the first bucket.
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_5900, MILLISECONDS)).join();
        logger.info("Size of fowareded messages: {}", spc.getListOfEventsAfterOrdering().size());
        Assert.assertEquals("e1", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());
        Assert.assertEquals("e2", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());
        Assert.assertEquals("e3", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());

        // Test the second bucket is flushed or not
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_9000, MILLISECONDS)).join();
        logger.info("Size of fowareded messages: {}", spc.getListOfEventsAfterOrdering().size());
        Assert.assertEquals("e4", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());
        Assert.assertEquals("e5", ((IgniteEventImpl) spc.getAndRemove()).getRequestId());
    }

    /**
     * Gets the ignite event.
     *
     * @param currentTime the current time
     * @return the ignite event
     */
    @NotNull
    private static IgniteEventImpl getIgniteEvent(long currentTime) {
        IgniteEventImpl eventImpl1 = new IgniteEventImpl();
        // Set event time after 1 sec
        eventImpl1.setTimestamp(currentTime + Constants.THREAD_SLEEP_TIME_1000);
        eventImpl1.setEventId(EventID.SPEED);
        eventImpl1.setRequestId("e1");
        eventImpl1.setSourceDeviceId("deviceId1");
        return eventImpl1;
    }

    /**
     * The Class MsgSeqStreamProcessingContext.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    class MsgSeqStreamProcessingContext<K, V> implements StreamProcessingContext<K, V> {
        
        /** The msg seq pre processor. */
        private MsgSeqPreProcessor msgSeqPreProcessor;
        
        /** The list of events after ordering. */
        private List listOfEventsAfterOrdering = new ArrayList();

        /**
         * Instantiates a new msg seq stream processing context.
         *
         * @param msgSeqPreProcessor the msg seq pre processor
         */
        public MsgSeqStreamProcessingContext(MsgSeqPreProcessor msgSeqPreProcessor) {
            this.msgSeqPreProcessor = msgSeqPreProcessor;
        }

        /**
         * Stream name.
         *
         * @return the string
         */
        @Override
        public String streamName() {
            return "test-msg-seq-processor";
        }

        /**
         * Partition.
         *
         * @return the int
         */
        @Override
        public int partition() {
            return 0;
        }

        /**
         * Offset.
         *
         * @return the long
         */
        @Override
        public long offset() {
            return 0;
        }

        /**
         * Checkpoint.
         */
        @Override
        public void checkpoint() {


        }

        /**
         * Gets the state store.
         *
         * @param name the name
         * @return the state store
         */
        @Override
        public KeyValueStore getStateStore(String name) {
            return new TestMsgSeqStreamStateStore();
        }


        /**
         * Forward directly.
         *
         * @param key the key
         * @param value the value
         * @param topic the topic
         */
        @Override
        public void forwardDirectly(String key, String value, String topic) {
        }

        /**
         * Forward directly.
         *
         * @param key the key
         * @param value the value
         * @param topic the topic
         */
        @Override
        public void forwardDirectly(@SuppressWarnings("rawtypes") IgniteKey key, IgniteEvent value, String topic) {
            msgSeqPreProcessor.process(new Record<>(key, value, System.currentTimeMillis()));
        }

        /**
         * Gets the task ID.
         *
         * @return the task ID
         */
        @Override
        public String getTaskID() {
            return TASK_ID;
        }

        /**
         * Gets the metric registry.
         *
         * @return the metric registry
         */
        @Override
        public MetricRegistry getMetricRegistry() {

            return null;
        }

        /**
         * Schedule.
         *
         * @param interval the interval
         * @param punctuationType the punctuation type
         * @param punctuator the punctuator
         */
        @Override
        public void schedule(long interval, PunctuationType punctuationType, Punctuator punctuator) {


        }

        /**
         * Forward.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void forward(Record<K, V> kafkaRecord) {
            K key = kafkaRecord.key();
            V value = kafkaRecord.value();
            listOfEventsAfterOrdering.add(value);
            logger.info("Msg k {}, requestId {}", key, ((IgniteEventImpl) value).getRequestId());
        }

        /**
         * Forward.
         *
         * @param kafkaRecord the kafka record
         * @param name the name
         */
        @Override
        public void forward(Record<K, V> kafkaRecord, String name) {


        }

        /**
         * Gets the list of events after ordering.
         *
         * @return the list of events after ordering
         */
        public List getListOfEventsAfterOrdering() {
            return listOfEventsAfterOrdering;
        }

        /**
         * Reset list of events after ordering.
         *
         * @param listOfEventsAfterOrdering the list of events after ordering
         */
        public void resetListOfEventsAfterOrdering(List listOfEventsAfterOrdering) {
            this.listOfEventsAfterOrdering.clear();
        }

        /**
         * Gets the and remove.
         *
         * @return the and remove
         */
        public Object getAndRemove() {
            return listOfEventsAfterOrdering.remove(0);
        }
    }

    /**
     * The Class TestMsgSeqStreamStateStore.
     */
    class TestMsgSeqStreamStateStore extends HarmanPersistentKVStore<String, Object> {

        /** The name. */
        String name = "TestMsgSeqStreamStateStore";
        
        /** The data. */
        Map<String, Object> data = new HashMap<>();

        /**
         * Instantiates a new test msg seq stream state store.
         */
        public TestMsgSeqStreamStateStore() {
            super("TestMsgSeqStreamStateStore",
                    true, null, null, new Properties());
        }

        /**
         * Instantiates a new test msg seq stream state store.
         *
         * @param name the name
         * @param changeLoggingEnabled the change logging enabled
         * @param keySerde the key serde
         * @param valueSerde the value serde
         * @param properties the properties
         */
        public TestMsgSeqStreamStateStore(String name, boolean changeLoggingEnabled,
                Serde<String> keySerde, Serde<Object> valueSerde,
                Properties properties) {
            super(name, changeLoggingEnabled, keySerde, valueSerde, properties);
        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return "TestMsgSeqStreamStateStore";
        }

        /**
         * Inits the.
         *
         * @param context the context
         * @param root the root
         */
        @Override
        public void init(ProcessorContext context, StateStore root) {

        }

        /**
         * Flush.
         */
        @Override
        public void flush() {


        }

        /**
         * Close.
         */
        @Override
        public void close() {


        }

        /**
         * Persistent.
         *
         * @return true, if successful
         */
        @Override
        public boolean persistent() {

            return false;
        }

        /**
         * Checks if is open.
         *
         * @return true, if is open
         */
        @Override
        public boolean isOpen() {

            return false;
        }

        /**
         * Gets the.
         *
         * @param key the key
         * @return the object
         */
        @Override
        public Object get(String key) {

            return data.get(key);
        }

        /**
         * Range.
         *
         * @param from the from
         * @param to the to
         * @return the key value iterator
         */
        @Override
        public KeyValueIterator<String, Object> range(String from, String to) {

            return null;
        }

        /**
         * All.
         *
         * @return the key value iterator
         */
        @Override
        public KeyValueIterator<String, Object> all() {

            return null;
        }

        /**
         * Approximate num entries.
         *
         * @return the long
         */
        @Override
        public long approximateNumEntries() {

            return 0;
        }

        /**
         * Put.
         *
         * @param key the key
         * @param value the value
         */
        @Override
        public void put(String key, Object value) {
            data.put(key, value);
        }

        /**
         * Put if absent.
         *
         * @param key the key
         * @param value the value
         * @return the object
         */
        @Override
        public Object putIfAbsent(String key, Object value) {

            return null;
        }

        /**
         * Put all.
         *
         * @param entries the entries
         */
        @Override
        public void putAll(List<KeyValue<String, Object>> entries) {


        }

        /**
         * Delete.
         *
         * @param key the key
         * @return the object
         */
        @Override
        public Object delete(String key) {
            data.remove(key);
            return null;
        }

    }
}
