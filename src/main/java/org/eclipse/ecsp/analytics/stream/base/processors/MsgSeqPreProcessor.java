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

import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.SequenceBuffer;
import org.eclipse.ecsp.analytics.stream.base.StreamBaseConstant;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.stores.ObjectStateStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.ObjectUtils;
import org.eclipse.ecsp.analytics.stream.base.utils.Pair;
import org.eclipse.ecsp.analytics.stream.base.utils.ThreadUtils;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.metrics.IgniteErrorCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MsgSeqPreProcessor is preprocessor which is responsible for ordering the messages.
 * This Processor is disabled [means msg.seq.time.interval.in.millis's value is not positive] by default.
 * Time-based ordering is applicable for a specific time boundary. The
 * ordering of the messages are across all Kafka topics and per partition.
 */
public class MsgSeqPreProcessor implements IgniteEventStreamProcessor {
    
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(MsgSeqPreProcessor.class);
    
    /** The Constant MSG_SEQ_STATE_STORE_NAME. */
    private static final String MSG_SEQ_STATE_STORE_NAME = "msg-seq-state-store";
    
    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;
    
    /** The state store. */
    private KeyValueStore<String, Object> stateStore;
    // SequenceBuffer stores only the list of Keys. Key is nothing but a salted
    // unique Id which maps with a IgniteEvent is StateStore. For example, when
    // new event comes into this Processor, Processor will store that event in
    // StateStore with key as
    // <NanoTimeStamp>_<deviceId>_<eventid>_<some salted random number>
    // and that generated key will store in SequenceBuffer. So now
    // SequenceBuffer only updates these keys in StateStore frequently and size
    // of the Set is also less.

    /** The sequence buffer. */
    private SequenceBuffer sequenceBuffer;
    
    /** The task id. */
    private String taskId;
    
    /** The last processed key. */
    private IgniteKey<?> lastProcessedKey;
    
    /** The msg seq topic name. */
    @Value("${msg.seq.topic.name}")
    private String msgSeqTopicName;
    
    /** The msg seq time int in millis. */
    @Value("${msg.seq.time.interval.in.millis:0}")
    private long msgSeqTimeIntInMillis;
    
    /** The change log enabled. */
    @Value("${msg.seq.state.store.changelog.enabled:true}")
    private boolean changeLogEnabled;
    
    /** The sequence buffer impl class. */
    @Value("${msg.seq.buffer.impl.class}")
    private String sequenceBufferImplClass;
    
    /** The last flush timestamp. */
    private long lastFlushTimestamp;
    
    /** The exec. */
    private ScheduledExecutorService exec = null;

    /** The error counter. */
    @Autowired
    private IgniteErrorCounter errorCounter;

    /**
     * At the time of initialization, MsgSeqProcessor spawns the fixed delayed
     * Thread which sends the FlushEvent to flush the sequence
     * buffer [backed by state store] messages.
     *
     * @param spc the spc
     */
    @Override
    public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        SequenceBuffer storedSequenceBuffer;
        this.spc = spc;
        this.taskId = spc.getTaskID();
        if (msgSeqTimeIntInMillis > 0) {
            LOGGER.info("Initializing Message Ordering Processor");
            ObjectUtils.requireNonEmpty(msgSeqTopicName, "msg.seq.topic.name should be defined");
            try {
                this.stateStore = this.spc.getStateStore(MSG_SEQ_STATE_STORE_NAME);
                ObjectUtils.requireNonNull(stateStore, "State Store should not be null.");
                sequenceBuffer = (SequenceBuffer) getClass().getClassLoader().loadClass(sequenceBufferImplClass)
                        .getDeclaredConstructor().newInstance();
                storedSequenceBuffer = (SequenceBuffer) stateStore.get(StreamBaseConstant.MSG_SEQ_PREFIX + taskId);
                if (storedSequenceBuffer != null) {
                    sequenceBuffer.init(storedSequenceBuffer);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(sequenceBufferImplClass + " refers to a class that is not "
                        + "available on the classpath");
            }
            exec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setName("msgseq:" + Thread.currentThread().getName() + ":" + spc.getTaskID() + ":" 
                        + System.currentTimeMillis());
                return t;
            });

            exec.scheduleWithFixedDelay(() -> {
                try {
                    sendFlushEvent();
                } catch (Exception e) {
                    errorCounter.incErrorCounter(Optional.ofNullable(taskId), e.getClass());
                    LOGGER.warn("Error while sending Flush Event.", e);
                }
            }, msgSeqTimeIntInMillis, msgSeqTimeIntInMillis, TimeUnit.MILLISECONDS);
        } else {
            LOGGER.info("Message ordering is disabled, because msgSeqTimeIntInMilis {} [zero]", msgSeqTimeIntInMillis);
        }
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return "msg-seq-pre-processor";
    }

    /**
     * Sets the msg seq time int in millis.
     *
     * @param msgSeqTimeIntInMillis the new msg seq time int in millis
     */
    void setMsgSeqTimeIntInMillis(long msgSeqTimeIntInMillis) {
        this.msgSeqTimeIntInMillis = msgSeqTimeIntInMillis;
    }

    /**
     * Sets the state store.
     *
     * @param stateStore the state store
     */
    void setStateStore(HarmanPersistentKVStore<String, Object> stateStore) {
        this.stateStore = stateStore;
    }

    /**
     * Sets the sequence buffer impl class.
     *
     * @param sequenceBufferImplClass the new sequence buffer impl class
     */
    void setSequenceBufferImplClass(String sequenceBufferImplClass) {
        this.sequenceBufferImplClass = sequenceBufferImplClass;
    }

    /**
     * Sets the msg seq topic name.
     *
     * @param msgSeqTopicName the new msg seq topic name
     */
    void setMsgSeqTopicName(String msgSeqTopicName) {
        this.msgSeqTopicName = msgSeqTopicName;
    }

    /**
     * If the event is a FlushEvent, then it forwards all the sequence buffer
     * [backed by state-store] messages to the next Processor in the
     * chain. Delete those messages from In-memory Cached which are forwarded to next Processor.
     * If the event is not a FlushEvent, then it
     * stores the message in sequence buffer [backed by state-store].
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {

        IgniteKey<?> key = kafkaRecord.key();
        IgniteEvent value = kafkaRecord.value();

        if (msgSeqTimeIntInMillis == 0) {
            LOGGER.trace("Message ordering is disabled, so simply forwarding the Ignite event to next processor.");
            spc.forward(kafkaRecord);
            return;
        }
        lastProcessedKey = key;
        Long currentProcessingEt = value.getTimestamp();
        long totalFlushEventCount = 0;
        if (lastFlushTimestamp > currentProcessingEt) {
            LOGGER.warn("Discarding event {} with time {} as it is older than "
                            + "the last flush event time {}", value, currentProcessingEt,
                    lastFlushTimestamp);
            return;
        }
        if (value.getEventId().equals(EventID.MSG_SEQ_BUF_FLUSH_EVENT)) {
            totalFlushEventCount = 0;
            long flushTime = value.getTimestamp();

            // Get the Time based buckets from sequence buffer
            Map<Long, List<String>> flushEntries = sequenceBuffer.head(flushTime);
            LOGGER.debug("Found {} sequence buckets eligible for flushing for timestamp {}",
                    flushEntries.size(), flushTime);

            // flushing the ordered events one by one
            for (Entry<Long, List<String>> flushEntry : flushEntries.entrySet()) {
                Long flushBucketTime = flushEntry.getKey();
                List<String> flushSequenceEventKeyIds = flushEntry.getValue();
                LOGGER.debug("Flushing {} events for timestamp {}", flushSequenceEventKeyIds.size(), flushBucketTime);
                for (String storedEventKeyId : flushSequenceEventKeyIds) {
                    // StoreEventKeyId is the Id which points to the actual
                    // IgniteKey and IgniteEvent in StateStore. So get the
                    // stored IgniteEvent from StateStore.
                    Pair<IgniteKey<?>, IgniteEvent> data = (Pair) stateStore.get(storedEventKeyId);
                    if (data != null) {
                        // Forwarding the cached IgniteEvent to down stream processor
                        spc.forward(new Record<>(data.getA(), data.getB(), kafkaRecord.timestamp()));
                        totalFlushEventCount++;

                        // Remove the all time based keys from Cache
                        sequenceBuffer.remove(flushBucketTime, storedEventKeyId);

                        // Flush Event arrives, add event has been forwarded to
                        // next processor, so need to update sequence buffer in
                        // state store
                        storeSequenceBuffer();

                        // Now removing the IgniteEvent from StateStore.
                        stateStore.delete(storedEventKeyId);
                        LOGGER.debug("Flushed key {} for timestamp {} from sequence buffer "
                                        + "and deleted from state store", storedEventKeyId,
                                flushBucketTime);
                    } else {
                        LOGGER.warn("Event to be flushed not found in state store. Event keyId is {}",
                                storedEventKeyId);
                    }
                }
                // Removing Sequence key from sequence buffer
                sequenceBuffer.removeSequence(flushBucketTime);
                // Sequence key was removed, so updating the state store
                storeSequenceBuffer();
            }
            LOGGER.debug("Successfully flushed {} from sequence buffer", totalFlushEventCount);
        } else {
            addToSequenceBuffer(key, value);
        }

    }

    /**
     * Adds the to sequence buffer.
     *
     * @param key the key
     * @param value the value
     */
    private void addToSequenceBuffer(IgniteKey<?> key, IgniteEvent value) {
        StringBuilder keyIdBuffer = new StringBuilder();
        keyIdBuffer.append(System.nanoTime()).append(Constants.HYPHEN);
        keyIdBuffer.append(value.getSourceDeviceId()).append(Constants.HYPHEN);
        keyIdBuffer.append(value.getEventId()).append(Constants.HYPHEN);
        keyIdBuffer.append(System.currentTimeMillis());
        String keyId = keyIdBuffer.toString();
        Pair<IgniteKey<?>, IgniteEvent> pair = new Pair<>(key, value);
        // Updating sequence buffer
        sequenceBuffer.add(keyId, pair);
        // Either General Event is added to sequence buffer need to store
        // sequence buffer in state store
        storeSequenceBuffer();
        // store the pair in state store
        stateStore.put(keyId, pair);
        LOGGER.trace("keyId {} is added in sequence buffer {}", keyId, pair);
    }

    /**
     * Store sequence buffer.
     */
    private void storeSequenceBuffer() {
        sequenceBuffer.setLastFlushTimestamp(System.currentTimeMillis());
        stateStore.put(StreamBaseConstant.MSG_SEQ_PREFIX + taskId, sequenceBuffer);
        LOGGER.debug("Sequence buffer is updated in state store");
    }

    /**
     * Punctuate.
     *
     * @param timestamp the timestamp
     */
    @Override
    public void punctuate(long timestamp) {
        //overridden method
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        if (exec != null && !exec.isShutdown()) {
            ThreadUtils.shutdownExecutor(exec, Constants.THREAD_SLEEP_TIME_10000, true);
            LOGGER.info("pool thread shutdown completed for {} ", Thread.currentThread().getName());
        }
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        //overridden method
    }

    /**
     * Creates the state store.
     *
     * @return the harman persistent KV store
     */
    @Override
    public HarmanPersistentKVStore createStateStore() {
        return new ObjectStateStore(MSG_SEQ_STATE_STORE_NAME, changeLogEnabled);
    }

    /**
     * Send flush event.
     */
    private void sendFlushEvent() {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sending FlushEvent for the taskId:{}", taskId);
        }
        IgniteEvent flushEvent = createFlushEvent();
        // To forward a FlushEvent, Processor needs a Key. So key is picked
        // by two ways: [Statestore has some values to process] 1) if it is not
        // set yet, but sequenceBuffer (backed by statestore) has the entries,
        // so it picks the first key from StateStore. [use case, first time
        // boot] 2) It is set by process method. The first key which comes from
        // kafka source topic.
        if (lastProcessedKey == null && sequenceBuffer != null) {
            Pair<IgniteKey<?>, IgniteEvent> firstData = 
                    (Pair<IgniteKey<?>, IgniteEvent>) stateStore.get(sequenceBuffer.head());
            if (firstData != null) {
                lastProcessedKey = firstData.getA();
            }
        }

        if (lastProcessedKey != null) {
            LOGGER.debug("lastProcessedKey {} is  found, so sending Flushevent.", lastProcessedKey);
            spc.forwardDirectly(lastProcessedKey, flushEvent, msgSeqTopicName);
        } else {
            LOGGER.warn("Could not send flush event as lastProcessedKey couldn't be determined");
        }

    }

    /**
     * Creates the flush event.
     *
     * @return the ignite event
     */
    private IgniteEvent createFlushEvent() {
        IgniteEventImpl igniteEventImpl = new IgniteEventImpl();
        igniteEventImpl.setEventId(EventID.MSG_SEQ_BUF_FLUSH_EVENT);
        // Set the time interval which will be used for flushing those many
        // events from the treemap.
        // To insure the ordering for a particular interval, need to hold the
        // events for just double interval. So that the boundary event should
        // also ordered. That is why the Flush event's timestamp is just behind
        // the configured interval time.
        igniteEventImpl.setTimestamp(System.currentTimeMillis() - msgSeqTimeIntInMillis);
        igniteEventImpl.setVersion(Version.V1_0);
        return igniteEventImpl;
    }
}
