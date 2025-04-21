package org.eclipse.ecsp.analytics.stream.base.stores;

import dev.morphia.AdvancedDatastore;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.cache.GetEntityRequest;
import org.eclipse.ecsp.cache.GetMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.stream.dma.dao.DMCacheEntityDAOMongoImpl;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;
import redis.embedded.RedisServer408;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;


/**
 * Integration test case for {@link CacheBypass} use case of stream-base library.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@TestPropertySource("/cache-bypass-test.properties")
public class CacheBypassIntegrationTest extends KafkaStreamsApplicationTestBase {
    
    /** The mutation id. */
    private Optional<MutationId> mutationId = Optional.empty();
    
    /** The id. */
    private String id = "test_id";
    
    /** The queue. */
    private BlockingQueue<CacheEntity> queue;
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(CacheBypassIntegrationTest.class);

    /** The mongo server. */
    @ClassRule
    public static EmbeddedMongoDB mongoServer = new EmbeddedMongoDB();

    /** The redis server. */
    @ClassRule
    public static EmbeddedRedisServer redisServer = new EmbeddedRedisServer();

    /** The bypass. */
    @Autowired
    private CacheBypass bypass;

    /** The dm cache entity DAO. */
    @Autowired
    private DMCacheEntityDAOMongoImpl dmCacheEntityDAO;

    /** The ds. */
    @Autowired
    private AdvancedDatastore ds;
    
    /** The cache. */
    @Autowired
    private IgniteCache cache;

    /**
     * Setup.
     *
     * @throws Exception the exception
     * @throws MqttException the mqtt exception
     */
    @Before
    public void setup() throws Exception, MqttException {
        super.setup();
        queue = new LinkedBlockingDeque<CacheEntity>();
    }

    /**
     * Test populate queue.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testPopulateQueue() throws InterruptedException {
        bypass.setDmCacheEntityDao(dmCacheEntityDAO);

        for (int i = 0; i < TestConstants.FOUR; i++) {
            CacheEntity<StringKey, IgniteEventImpl> entityi = new CacheEntity<>();
            IgniteEventImpl eventi = new IgniteEventImpl();
            eventi.setEventId("testId");
            entityi.withKey(new StringKey("xyz")).withValue(eventi).withMapKey("abc_" + i).withMutationId(mutationId);
            switch (i) {
                case 0:
                    entityi.withOperation(Operation.PUT);
                    entityi.setLastUpdatedTime(LocalDateTime.parse("2020-03-12T12:30:38.839"));
                    break;
                case 1:
                    entityi.withOperation(Operation.DEL);
                    entityi.setLastUpdatedTime(LocalDateTime.parse("2020-03-12T12:30:30.839"));
                    break;
                case TestConstants.TWO:
                    entityi.withOperation(Operation.DEL_FROM_MAP);
                    entityi.setLastUpdatedTime(LocalDateTime.parse("2020-03-12T12:40:38.839"));
                    break;
                case TestConstants.THREE:
                    entityi.withOperation(Operation.PUT_TO_MAP);
                    entityi.setLastUpdatedTime(LocalDateTime.parse("2020-03-12T10:30:38.839"));
                    break;
                default:
                    //Nothing to do.
            }
            dmCacheEntityDAO.save(entityi);
        }
        bypass.setup();
        Assert.assertTrue(!bypass.getQueue().isEmpty());
    }

    /**
     * Test save to mongo.
     */
    @Test
    public void testSaveToMongo() {
        BlockingQueue<CacheEntity> queue = new PriorityBlockingQueue<>(TestConstants.INT_30, 
               (CacheEntity e1, CacheEntity e2) -> e1.getLastUpdatedTime().compareTo(e2.getLastUpdatedTime()));
        for (int i = 0; i < TestConstants.FOUR; i++) {
            CacheEntity<StringKey, IgniteEventImpl> entityi = new CacheEntity<>();
            IgniteEventImpl eventi = new IgniteEventImpl();
            eventi.setEventId("testId");
            entityi.withKey(new StringKey("xyz")).withValue(eventi).withMapKey("abc_" + i).withMutationId(mutationId);
            entityi.setLastUpdatedTime(LocalDateTime.now());
            setOperationOnEntity(entityi, i);
            queue.add(entityi);
        }
        bypass.setQueue(queue);
        bypass.close();
        List<CacheEntity> list = dmCacheEntityDAO.findAll();
        dmCacheEntityDAO.deleteAll();
        int putOperationCounter = 0;
        int putToMapOperationCounter = 0;
        int delOperationCounter = 0;
        int delFromMapOperationCounter = 0;

        for (CacheEntity entity : list) {
            Assert.assertEquals("xyz", entity.getKey().convertToString());
            IgniteEventImpl event = (IgniteEventImpl) entity.getValue();
            Assert.assertEquals("testId", event.getEventId());
            Operation op = entity.getOperation();
            switch (op) {
                case PUT:
                    putOperationCounter++;
                    break;
                case DEL:
                    delOperationCounter++;
                    break;
                case DEL_FROM_MAP:
                    delFromMapOperationCounter++;
                    break;
                case PUT_TO_MAP:
                    putToMapOperationCounter++;
                    break;
                default:
                    //Nothing to do.
            }
        }
        Assert.assertEquals(TestConstants.FOUR, list.size());
        Assert.assertTrue(
                putOperationCounter == 1 
                && putToMapOperationCounter == 1 
                && delOperationCounter == 1 
                && delFromMapOperationCounter == 1);
    }
    
    /**
     * Sets the operation on entity.
     *
     * @param entityi the entityi
     * @param i the i
     */
    private void setOperationOnEntity(CacheEntity<StringKey, IgniteEventImpl> entityi, int i) {
        switch (i) {
            case 0:
                entityi.withOperation(Operation.PUT);
                break;
            case 1:
                entityi.withOperation(Operation.DEL);
                break;
            case TestConstants.TWO:
                entityi.withOperation(Operation.DEL_FROM_MAP);
                break;
            case TestConstants.THREE:
                entityi.withOperation(Operation.PUT_TO_MAP);
                break;
            default:
                //Nothing to do.
        }
    }
    
    /**
     * Test cache bypass if redis unavailable.
     *
     * @throws Exception the exception
     */
    @Test
    public void testCacheBypassIfRedisUnavailable() throws Exception {
        RedisServer408 redis = (RedisServer408) ReflectionTestUtils.getField(redisServer, "redis");
        redis.stop();
        //bypass.setup(id);
        for (int i = 0; i < TestConstants.TWO; i++) {
            CacheEntity<StringKey, IgniteEventImpl> entityi = new CacheEntity<>();
            IgniteEventImpl eventi = new IgniteEventImpl();
            eventi.setEventId("testId" + i);
            entityi.withKey(new StringKey("xyz" + i)).withValue(eventi).withMapKey("abc_" + i)
            .withMutationId(mutationId);
            entityi.setLastUpdatedTime(LocalDateTime.now());
            try {
                if (i % TestConstants.FOUR == 0) {
                    entityi.withOperation(Operation.PUT);
                    bypass.processEvents(entityi);                
                }
                if (i % TestConstants.FOUR == TestConstants.ONE) {
                    entityi.withOperation(Operation.PUT_TO_MAP);
                    bypass.processEvents(entityi);
                }
            } catch (Exception e) {
                logger.error("*********Redis error encountered**********");
            }
        }
        redis.start();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);    
        Assert.assertEquals(TestConstants.TWO, getRecordsFromRedis().size());
    }
    
    /**
     * Gets the records from redis.
     *
     * @return the records from redis
     */
    private List<IgniteEventImpl> getRecordsFromRedis() {
        List<IgniteEventImpl> entities = new ArrayList<>();
        GetEntityRequest request = new GetEntityRequest();
        request.withKey("xyz0");
        entities.add(cache.getEntity(request));
        GetMapOfEntitiesRequest requestWithMapKey = new GetMapOfEntitiesRequest();
        requestWithMapKey.withKey("abc_1");
        entities.add(cache.getEntity(request));
        return entities;
    }
    
    /**
     * Test cache bypass with load if redis unavailable.
     *
     * @throws Exception the exception
     */
    @Test
    public void testCacheBypassWithLoadIfRedisUnavailable() throws Exception {
        RedisServer408 redis = (RedisServer408) ReflectionTestUtils.getField(redisServer, "redis");
        redis.stop();
        //bypass.setup(id);
        startWorkerThreads();
        Thread.sleep(TestConstants.LONG_11000);
        redis.start();
        logger.debug("********REDIS IS NOW UP***********");
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals(0, bypass.getQueue().size());
        ExecutorService executorService = (ExecutorService) 
            ReflectionTestUtils.getField(bypass, "cacheBypassExecutorService");
        Assert.assertTrue(executorService.isShutdown());
        /*
         * Simulating the normal flow after Redis is back up.
         */
        for (int i = 0; i < TestConstants.TWO; i++) {
            CacheEntity<StringKey, IgniteEventImpl> entityi = new CacheEntity<>();
            IgniteEventImpl eventi = new IgniteEventImpl();
            eventi.setEventId("testId" + i);
            entityi.withKey(new StringKey("xyz" + i)).withValue(eventi).withMapKey("abc_" + i)
            .withMutationId(mutationId);
            entityi.setLastUpdatedTime(LocalDateTime.now());
            if (i % TestConstants.FOUR == 0) {
                entityi.withOperation(Operation.PUT);
                bypass.processEvents(entityi);                
            }
            if (i % TestConstants.FOUR == TestConstants.ONE) {
                entityi.withOperation(Operation.PUT_TO_MAP);
                bypass.processEvents(entityi);
            }
        }
        Assert.assertEquals(TestConstants.TWO, getRecordsFromRedis().size());
    }
    
    /**
     * Start worker threads.
     */
    private void startWorkerThreads() {
        Thread t1 = getThread1();
        t1.start();
        logger.debug("Thread 1 started");
        Thread t2 = getThread2();
        t2.start();
        logger.debug("Thread 2 started");
    }
    
    /**
     * Gets the thread 1.
     *
     * @return the thread 1
     */
    private Thread getThread1() {
        Thread t1 = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i <= TestConstants.INT_499; i++) {
                    CacheEntity<StringKey, IgniteEventImpl> entityi = new CacheEntity<>();
                    IgniteEventImpl eventi = new IgniteEventImpl();
                    eventi.setEventId("testId" + i);
                    entityi.withKey(new StringKey("xyz")).withValue(eventi).withMapKey("abc_" + i)
                        .withMutationId(mutationId);
                    entityi.setLastUpdatedTime(LocalDateTime.now());
                    try {
                        if (i % TestConstants.FOUR == 0) {
                            entityi.withOperation(Operation.PUT);
                            bypass.processEvents(entityi);                
                        }
                        if (i % TestConstants.FOUR == TestConstants.ONE) {
                            entityi.withOperation(Operation.PUT_TO_MAP);
                            bypass.processEvents(entityi);
                        }
                        if (i % TestConstants.FOUR == TestConstants.TWO) {
                            entityi.withOperation(Operation.DEL);
                            bypass.processEvents(entityi);
                        }
                        if (i % TestConstants.FOUR == TestConstants.THREE) {
                            entityi.withOperation(Operation.DEL_FROM_MAP);
                            bypass.processEvents(entityi);
                        }
                    } catch (Exception e) {
                        logger.error("*********Redis error encountered**********");
                    }
                }
            }
        };
        return t1;
    }
    
    /**
     * Gets the thread 2.
     *
     * @return the thread 2
     */
    private Thread getThread2() {
        Thread t2 = new Thread() {
            @Override
            public void run() {
                for (int i = TestConstants.INT_500; i < TestConstants.INT_1000; i++) {
                    CacheEntity<StringKey, IgniteEventImpl> entityi = new CacheEntity<>();
                    IgniteEventImpl eventi = new IgniteEventImpl();
                    eventi.setEventId("testId" + i);
                    entityi.withKey(new StringKey("xyz")).withValue(eventi).withMapKey("abc_" + i)
                        .withMutationId(mutationId);
                    entityi.setLastUpdatedTime(LocalDateTime.now());
                    try {
                        if (i % TestConstants.FOUR == 0) {
                            entityi.withOperation(Operation.PUT);
                            bypass.processEvents(entityi);                
                        }
                        if (i % TestConstants.FOUR == TestConstants.ONE) {
                            entityi.withOperation(Operation.PUT_TO_MAP);
                            bypass.processEvents(entityi);
                        }
                        if (i % TestConstants.FOUR == TestConstants.TWO) {
                            entityi.withOperation(Operation.DEL);
                            bypass.processEvents(entityi);
                        }
                        if (i % TestConstants.FOUR == TestConstants.THREE) {
                            entityi.withOperation(Operation.DEL_FROM_MAP);
                            bypass.processEvents(entityi);
                        }
                    } catch (Exception e) {
                        logger.error("*********Redis error encountered**********");
                    }
                }
            }
        };
        return t2;
    }

    /**
     * Test implementation for IgniteKey for this test class.
     */
    public static class StringKey implements CacheKeyConverter<StringKey> {

        /**
         * Instantiates a new string key.
         */
        public StringKey() {
        }

        /**
         * Instantiates a new string key.
         *
         * @param key the key
         */
        public StringKey(String key) {
            this.key = key;
        }

        /** The key. */
        private String key;

        /**
         * Gets the key.
         *
         * @return the key
         */
        public String getKey() {
            return key;
        }

        /**
         * Sets the key.
         *
         * @param key the new key
         */
        public void setKey(String key) {
            this.key = key;
        }

        /**
         * Convert from.
         *
         * @param key the key
         * @return the string key
         */
        @Override
        public StringKey convertFrom(String key) {
            return new StringKey(key);
        }

        /**
         * Convert to string.
         *
         * @return the string
         */
        @Override
        public String convertToString() {
            return key;
        }

    }
}