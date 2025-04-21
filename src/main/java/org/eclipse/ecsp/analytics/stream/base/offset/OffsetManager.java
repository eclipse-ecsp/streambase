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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.ThreadUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Offset manager is responsible to ensure that the services do not process duplicate offsets.
 * It also facilitates storage of offsets
 * periodically to a persistent storage layer in order to ensure that state is maintained.
 *
 * @author avadakkootko
 */
@Service
public class OffsetManager {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(OffsetManager.class);
    
    /** The offset dao. */
    @Autowired
    private KafkaStreamsOffsetManagementDAOMongoImpl offsetDao;
    
    /** The offsets mgmt executor. */
    private volatile ScheduledExecutorService offsetsMgmtExecutor = null;
    
    /** The offset persistence delay. */
    @Value("${" + PropertyNames.KAFKA_STREAMS_OFFSET_PERSISTENCE_DELAY + ":60000}")
    private int offsetPersistenceDelay;
    
    /** The offset persistence init delay. */
    @Value("${" + PropertyNames.KAFKA_STREAMS_OFFSET_PERSISTENCE_INIT_DELAY + ":10000}")
    private int offsetPersistenceInitDelay;
    
    /** The offset persistence enabled. */
    @Value("${" + PropertyNames.KAFKA_STREAMS_OFFSET_PERSISTENCE_ENABLED + ":false}")
    private boolean offsetPersistenceEnabled;
    
    /** The persist offset map. */
    // Current offsets that needs to be persisted to mongo will be stored here.
    private volatile ConcurrentHashMap<String, KafkaStreamsTopicOffset> persistOffsetMap =
            new ConcurrentHashMap<String, KafkaStreamsTopicOffset>();

    // Reference map will be updated once from mongo. This is to reduce
    // frequent queries to mongo in case the persistence map doesnt have any
    /** The refrence map. */
    // value in it.
    private volatile ConcurrentHashMap<String, KafkaStreamsTopicOffset> refrenceMap =
            new ConcurrentHashMap<String, KafkaStreamsTopicOffset>();
    
    /** The started offsets mgmt executor. */
    private final AtomicBoolean startedOffsetsMgmtExecutor = new AtomicBoolean(false);
    
    /** The closed offsets mgmt executor. */
    private final AtomicBoolean closedOffsetsMgmtExecutor = new AtomicBoolean(false);
    

    /**
     * Update previously processed offset in local memory.
     *
     * @param kafkaTopic kafkaTopic
     * @param partition partition
     * @param offset offset
     */
    public void updateProcessedOffset(String kafkaTopic, int partition, long offset) {
        if (offsetPersistenceEnabled) {
            String key = getKey(kafkaTopic, partition);
            KafkaStreamsTopicOffset kafkaStreamsTopicOffset = persistOffsetMap.get(key);
            if (kafkaStreamsTopicOffset == null) {
                kafkaStreamsTopicOffset = new KafkaStreamsTopicOffset(kafkaTopic, partition, offset);
                persistOffsetMap.put(key, kafkaStreamsTopicOffset);
            } else {
                kafkaStreamsTopicOffset.setOffset(offset);
            }
        }
    }

    /**
     * Check if this offset should be processed.
     *
     * @param kafkaTopic kafkaTopic
     * @param partition partition
     * @param offset offset
     * @return boolean
     */
    public boolean doSkipOffset(String kafkaTopic, int partition, long offset) {
        boolean skipOffset = false;
        if (offsetPersistenceEnabled) {
            String key = getKey(kafkaTopic, partition);
            KafkaStreamsTopicOffset currVal = persistOffsetMap.get(key);
            KafkaStreamsTopicOffset refVal = refrenceMap.get(key);
            if (currVal != null && offset < currVal.getOffset()) {
                logger.debug("Skipping offset {} being processed in offsetmanager is less that offset {} "
                        + "from persistent map for key {}", offset, currVal.getOffset(), key);
                skipOffset = true;
            } else if (currVal == null && refVal != null && offset < refVal.getOffset()) {
                logger.debug("Skipping offset {} being processed in offsetmanager is less that offset {} "
                        + "from reference map for key {}", offset, refVal.getOffset(), key);
                skipOffset = true;
            }
        }
        return skipOffset;
    }

    /**
     * Gets the thread factory.
     *
     * @param threadName the thread name
     * @return the thread factory
     */
    private ThreadFactory getThreadFactory(String threadName) {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(threadName);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((thread1, t) -> 
                logger.error("Uncaught exception detected in offsetmanager! " 
                        + t + " st: " + Arrays.toString(t.getStackTrace())));
            return thread;
        };
    }

    /**
     * Initialize refrence map.
     */
    protected void initializeRefrenceMap() {
        List<KafkaStreamsTopicOffset> topicOffsetList = offsetDao.findAll();
        logger.info("TopicOffset list of size {} for offsetmanager", topicOffsetList.size());
        topicOffsetList.parallelStream().forEach(topicOffset -> refrenceMap.put(topicOffset.getId(), topicOffset));
    }

    /**
     * Gets the key.
     *
     * @param topic the topic
     * @param partition the partition
     * @return the key
     */
    protected String getKey(String topic, int partition) {
        return topic + ":" + partition;
    }

    /**
     * This method will be invoked when the sp chages state to RUNNING.
     */
    public void setUp() {
        if (offsetPersistenceEnabled && !startedOffsetsMgmtExecutor.get()) {
            startedOffsetsMgmtExecutor.set(true);
            offsetsMgmtExecutor = Executors.newSingleThreadScheduledExecutor(getThreadFactory("kafkaStreamsOffsetDt"));
            closedOffsetsMgmtExecutor.set(false);
            initializeRefrenceMap();
            logger.info("Running offsetmanager executer");
            offsetsMgmtExecutor.scheduleWithFixedDelay(() -> {
                try {
                    logger.trace("Executing offsetmanager at fixed delay");
                    persistOffset();
                } catch (Exception e) {
                    logger.error("Error offsetmanager :", e);
                }
            }, offsetPersistenceInitDelay, offsetPersistenceDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("Not attempting to run offsetmanager executer as offsetPersistence flag is {} "
                    + "and started state is {}", offsetPersistenceEnabled, startedOffsetsMgmtExecutor.get());
        }
    }

    /**
     * This method is used to save the offset data per topic per partition in a periodic fashion to mongo.
     */
    protected void persistOffset() {
        if (persistOffsetMap != null && !persistOffsetMap.isEmpty()) {
            for (KafkaStreamsTopicOffset topicOffset : persistOffsetMap.values()) {
                try {
                    offsetDao.save(topicOffset);
                    logger.debug("Persisted kafka topic offset to database by offsetmanager. {}",
                            topicOffset.toString());
                } catch (Exception e) {
                    logger.error("Error occured while persisting offset by kafka streams offsetmanager:", e);
                }
            }
        } else {
            logger.trace("No offset to persist for kafka streams by offsetmanager");
        }
    }

    /**
     * shutdown(): to close opened resources.
     */
    public void shutdown() {
        if (startedOffsetsMgmtExecutor.get() && !closedOffsetsMgmtExecutor.get() && offsetPersistenceEnabled) {
            startedOffsetsMgmtExecutor.set(false);
            persistOffset();
            persistOffsetMap.clear();
            refrenceMap.clear();
            ThreadUtils.shutdownExecutor(offsetsMgmtExecutor, Constants.THREAD_SLEEP_TIME_10000, false);
            logger.info("Closing kafka streams offsetmanager");
            closedOffsetsMgmtExecutor.set(true);
        }
    }

    /**
     * Sets the offset persistence enabled.
     *
     * @param offsetPersistenceEnabled the new offset persistence enabled
     */
    // Used for test case
    void setOffsetPersistenceEnabled(boolean offsetPersistenceEnabled) {
        this.offsetPersistenceEnabled = offsetPersistenceEnabled;
    }

    /**
     * Sets the persist offset map.
     *
     * @param persistOffsetMap the persist offset map
     */
    // Used for test case
    void setPersistOffsetMap(ConcurrentHashMap<String, KafkaStreamsTopicOffset> persistOffsetMap) {
        this.persistOffsetMap = persistOffsetMap;
    }

    /**
     * Sets the refrence map.
     *
     * @param refrenceMap the refrence map
     */
    // Used for test case
    void setRefrenceMap(ConcurrentHashMap<String, KafkaStreamsTopicOffset> refrenceMap) {
        this.refrenceMap = refrenceMap;
    }

}
