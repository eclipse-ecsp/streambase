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

package org.eclipse.ecsp.analytics.stream.base.stores;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.ThreadUtils;
import org.eclipse.ecsp.cache.DeleteEntryRequest;
import org.eclipse.ecsp.cache.DeleteMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutEntityRequest;
import org.eclipse.ecsp.cache.PutMapOfEntitiesRequest;
import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.stream.dma.dao.DMCacheEntityDAOMongoImpl;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link CacheBypass} class offers a reliable persistence of messages coming 
 * to DMA to be forwarded to MQTT topic. For eg. DMA stores RetryRecords in redis for its 
 * Retry feature, and if redis is unavailable, then these records are stored in a 
 * Queue from where one or more threads will poll records from queue and try to 
 * complete the redis transaction for each one of them.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@Component
public class CacheBypass<K extends CacheKeyConverter<K>, V extends IgniteEntity> {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(CacheBypass.class);

    /** The queue. */
    private BlockingQueue<CacheEntity<K, V>> queue;
    
    /** The cache bypass executor service. */
    private ExecutorService cacheBypassExecutorService;

    /** The cache. */
    @Autowired
    private IgniteCache cache;

    /** The dm cache entity dao. */
    @Autowired
    private DMCacheEntityDAOMongoImpl dmCacheEntityDao;

    /** The queue capacity. */
    @Value("${" + PropertyNames.CACHE_BYPASS_QUEUE_INITIAL_CAPACITY + "}")
    private int queueCapacity;
    
    /** The num cache bypass threads. */
    @Value("${" + PropertyNames.DMA_NUM_CACHE_BYPASS_THREADS + "}")
    private int numCacheBypassThreads;
    
    /** The wait time. */
    @Value("${" + PropertyNames.CACHE_BYPASS_THREADS_SHUTDOWN_WAIT_TIME + "}")
    private int waitTime;

    /** The is executor service initialized. */
    private AtomicBoolean isExecutorServiceInitialized = new AtomicBoolean(false);

    /**
     * Sets the cache.
     *
     * @param cache the new cache
     */
    public void setCache(IgniteCache cache) {
        this.cache = cache;
    }

    /**
     * Gets the queue.
     *
     * @return the queue
     */
    public Queue<CacheEntity<K, V>> getQueue() {
        return this.queue;
    }

    /**
     * Sets the queue.
     *
     * @param queue the queue
     */
    public void setQueue(BlockingQueue<CacheEntity<K, V>> queue) {
        this.queue = queue;
    }

    /**
     * Validate.
     */
    protected void validate() {
        if (this.queueCapacity <= 0) {
            throw new IllegalArgumentException("Queue capacity must be greater than 0.");
        }
        if (waitTime <= 0) {
            throw new IllegalArgumentException("CacheBypass thread shutdown wait time must be greater than 0.");
        }
        if (numCacheBypassThreads < Constants.ONE) {
            throw new IllegalArgumentException("Number of threads for CacheBypass thread-pool must be greater than 1.");
        }
    }

    /**
     * Sort.
     *
     * @param list the list
     * @return the list
     */
    @SuppressWarnings("rawtypes")
    List<CacheEntity> sort(List<CacheEntity> list) {
        Collections.sort(list, (entity1, entity2) -> 
            entity1.getLastUpdatedTime().compareTo(entity2.getLastUpdatedTime()));
        return list;
    }

    /**
     * Populate queue.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void populateQueue() {
        List<CacheEntity> entitiesList = dmCacheEntityDao.findAll();
        entitiesList = sort(entitiesList);
        queue.addAll((Collection) entitiesList);
        logger.info("CacheBypass queue populated with {} cache entities.", queue.size());
        dmCacheEntityDao.deleteAll();
        logger.info("dmCacheEntities collection cleared.");    
    }

    /**
     * Sets up and starts the CacheBypass thread pool for this task id. 
     * The threads from the thread pool continuously poll records from the queue. 
     * If redis is up and running then the queue will be empty, else, there will be 
     * data in the queue for the threads to process.
     **/
    @PostConstruct
    public void setup() {
        validate();
        if (cacheBypassExecutorService == null || cacheBypassExecutorService.isShutdown()) {
            if (queue == null) {
                queue = new PriorityBlockingQueue<>(queueCapacity, 
                    (CacheEntity<K, V> e1, CacheEntity<K, V> e2) -> 
                        e1.getLastUpdatedTime().compareTo(e2.getLastUpdatedTime()));
            }
            populateQueue();
            checkAndProcessQueue();
        }
    }
    
    /**
     * Check and process queue.
     */
    private void checkAndProcessQueue() {
        if (!queue.isEmpty()) {
            logger.info("Pulled {} records from dmCacheEntities collection at startup. "
                + "Processing them..", queue.size());
            isExecutorServiceInitialized.set(true);
            initializeCacheBypassExecutorService();
        }
    }
    
    /**
     * Initialize cache bypass executor service.
     */
    private void initializeCacheBypassExecutorService() {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern(Constants.CACHE_BYPASS_REDIS_RETRY_THREAD + "-%d")
                .build();
        cacheBypassExecutorService = Executors.newFixedThreadPool(numCacheBypassThreads, threadFactory);
        for (int i = 0; i < numCacheBypassThreads; i++) {
            cacheBypassExecutorService.execute(this::pollAndProcessEvents);
        }
        logger.debug("Thread pool for CacheBypass created: {}", cacheBypassExecutorService.toString());       
    }
    
    /**
     * Poll and process events.
     */
    /*
     * polls and sends a cache entity to processEvents to get processed.
     */
    private void pollAndProcessEvents() {
        logger.debug("Executing pollAndProcessEvents(), queue size: {}", queue.size());
        while (!queue.isEmpty()) {
            CacheEntity<K, V> entity =  queue.poll();
            if (entity != null) {
                logger.info("Entity with Key {} and Value {} polled from the queue. Current size of queue is: {}", 
                    entity.getKey().convertToString(), entity.getValue(), queue.size());
                processEvents(entity);
            }
        }
    }

    /**
     * Takes an entity and processes it based on operation set on that entity.
     * If the operation for a particular entity fails, then the control will jump to the catch block, 
     * and the entity will be added to the {@link CacheBypass#queue} and CacheBypass ExecutorService 
     * will be initialized {@link CacheBypass#cacheBypassExecutorService} and threads from the pool
     * will keep polling entries and keep retrying the redis operation set on them until
     * the operation is successful.
     *Lazy initialization of ExecutorService on discovering that the application is unable to 
     *perform the Redis operations. Once the {@link CacheBypass#queue} is emptied, 
     *the ExecutorService will be shut down.
     *
     * @param entity {@link CacheEntity}
     */
    public void processEvents(CacheEntity<K, V> entity) {
        try {
            switch (entity.getOperation()) {
                case PUT:
                    putToCache(entity);
                    break;
                case PUT_TO_MAP:
                    putToMapCache(entity);
                    break;
                case DEL:
                    deleteFromCache(entity);
                    break;
                case DEL_FROM_MAP:
                    deleteFromMapCache(entity);
                    break;
                default:
                    logger.error("Operation not supported.");
            }
            if (isExecutorServiceInitialized.get() && queue.isEmpty()) {
                isExecutorServiceInitialized.set(false);
                logger.info("CacheBypass queue is now empty. Shutting down the executor service.");
                shutdownExecutorService();
            }
        } catch (Exception e) {
            logger.error("Operation: {} unsuccessful for key: {} and Value: {}. "
                + "Added to CacheBypass queue to be retried.", 
                    entity.getOperation(), entity.getKey().convertToString(), entity.getValue());
            queue.add(entity);
            if (!isExecutorServiceInitialized.get()) {
                isExecutorServiceInitialized.set(true);
                initializeCacheBypassExecutorService();
            }
        } 
    }

    /**
     * Puts this entity into Redis.
     *
     * @param entity the entity
     */
    public void putToCache(CacheEntity<K, V> entity) {
        PutEntityRequest<V> putRequest = new PutEntityRequest<>();
        putRequest.withKey(entity.getKey().convertToString());
        putRequest.withValue(entity.getValue());
        putRequest.withNamespaceEnabled(false);
        Optional<MutationId> mutationId = entity.getMutationId();
        if (mutationId.isPresent()) {
            putRequest.withMutationId(String.valueOf(mutationId.get()));
        }
        cache.putEntity(putRequest);
        logger.debug("PutEntity operation complete for Key {} and Value {} ", 
            entity.getKey().convertToString(), entity.getValue());
    }

    /**
     * Puts the key-value pair in redis under a specific map key(parent key).
     *
     * @param entity {@link CacheEntity}
     */
    public void putToMapCache(CacheEntity<K, V> entity) {
        PutMapOfEntitiesRequest<IgniteEntity> putRequest = new PutMapOfEntitiesRequest<>();
        putRequest.withKey(entity.getMapKey());
        Map<String, IgniteEntity> map = new HashMap<>();
        map.put(entity.getKey().convertToString(), entity.getValue());
        putRequest.withValue(map);
        putRequest.withNamespaceEnabled(false);
        Optional<MutationId> mutationId = entity.getMutationId();
        if (mutationId.isPresent()) {
            putRequest.withMutationId(String.valueOf(mutationId.get()));
        }
        cache.putMapOfEntities(putRequest);
        logger.debug("PutMapOfEntities operation complete for key {} and value {} ", 
            entity.getKey().convertToString(), entity.getValue());
    }

    /**
     * Deletes the entity from Redis.
     *
     * @param entity the entity
     */
    public void deleteFromCache(CacheEntity<K, V> entity) {
        DeleteEntryRequest deleteRequest = new DeleteEntryRequest();
        deleteRequest.withKey(entity.getKey().convertToString());
        Optional<MutationId> mutationId = entity.getMutationId();
        deleteRequest.withNamespaceEnabled(false);
        if (mutationId.isPresent()) {
            deleteRequest.withMutationId(String.valueOf(mutationId.get()));
        }
        cache.delete(deleteRequest);
        logger.debug("Delete operation complete for key {} and value {} ", 
            entity.getKey().convertToString(), entity.getValue());
    }

    /**
     * Deletes a key-value pair from redis from under a specific map key(parent key).
     *
     * @param entity {@link CacheEntity}
     */
    public void deleteFromMapCache(CacheEntity<K, V> entity) {
        DeleteMapOfEntitiesRequest deleteMapRequest = new DeleteMapOfEntitiesRequest();
        deleteMapRequest.withKey(entity.getMapKey());
        Set<String> subKeys = new HashSet<>();
        subKeys.add(entity.getKey().convertToString());
        deleteMapRequest.withFields(subKeys);
        deleteMapRequest.withNamespaceEnabled(false);
        Optional<MutationId> mutationId = entity.getMutationId();
        if (mutationId.isPresent()) {
            deleteMapRequest.withMutationId(String.valueOf(mutationId.get()));
        }
        cache.deleteMapOfEntities(deleteMapRequest);
        logger.debug("DeleteMapOfEntities operation complete for key {} and value {} ",
            entity.getKey().convertToString(), entity.getValue());
    }

    /**
     * saving the data of queue to mongo at the time of shutdown hook.
     *
     * @param queue the queue
     */
    public synchronized void saveToMongo(Queue<CacheEntity<K, V>> queue) {
        if (queue != null && !queue.isEmpty()) {
            CacheEntity<K, V>[] entitiesArray = queue.toArray(new CacheEntity[queue.size()]);
            dmCacheEntityDao.saveAll(entitiesArray);
            queue.removeAll(getQueue());
            logger.info("Flushed {} cache entity entries in to mongo", entitiesArray.length);
        } else {
            logger.info("CacheBypass queue is empty. Nothing to flush into MongoDB.");
        }
    }

    /**
     * At the time of shutdown hook, this method will be invoked, and saveToMongo will be invoked.
     **/
    public void close() {
        saveToMongo(queue);
        shutdownExecutorService();
    }
    
    /**
     * Shutdown executor service.
     */
    private void shutdownExecutorService() {
        if (cacheBypassExecutorService != null && !cacheBypassExecutorService.isShutdown()) {
            logger.info("Shutting down the CacheBypass Executor threads.");
            ThreadUtils.shutdownExecutor(cacheBypassExecutorService, waitTime, false);
        }
    }

    /**
     * Sets the queue capacity.
     *
     * @param capacity the new queue capacity
     */
    // below methods are for testing purposes
    void setQueueCapacity(int capacity) {
        this.queueCapacity = capacity;
    }

    /**
     * Sets the wait time.
     *
     * @param waitTime the new wait time
     */
    void setWaitTime(int waitTime) {
        this.waitTime = waitTime;
    }

    /**
     * Sets the dm cache entity dao.
     *
     * @param dmCacheEntityDao the new dm cache entity dao
     */
    void setDmCacheEntityDao(DMCacheEntityDAOMongoImpl dmCacheEntityDao) {
        this.dmCacheEntityDao = dmCacheEntityDao;
    }
}