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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import dev.morphia.AdvancedDatastore;
import dev.morphia.mapping.Mapper;
import dev.morphia.query.Query;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.cache.DeleteEntryRequest;
import org.eclipse.ecsp.cache.DeleteMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutEntityRequest;
import org.eclipse.ecsp.cache.PutMapOfEntitiesRequest;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.stream.dma.dao.DMCacheEntityDAOMongoImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * Test class for {@link CacheBypass}.
 */
public class CacheBypassTest {
    
    /** The thrown. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    /** The ignite event. */
    private IgniteEventImpl igniteEvent;
    
    /** The mutation id. */
    private Optional<MutationId> mutationId = Optional.empty();
    
    /** The id. */
    private String id = "test_id";
    
    /** The queue. */
    private BlockingDeque<CacheEntity<?, ?>> queue;
    
    /** The entity. */
    private CacheEntity<StringKey, IgniteEventImpl> entity;
    
    /** The bypass. */
    @InjectMocks
    private CacheBypass bypass;
    
    /** The bypass mock. */
    @Mock
    private CacheBypass bypassMock;
    
    /** The cache. */
    @Mock
    private IgniteCache cache;
    
    /** The ds. */
    @Mock
    private AdvancedDatastore ds;
    
    /** The mongo database. */
    @Mock
    private MongoDatabase mongoDatabase;
    
    /** The mongo collection. */
    @Mock
    private MongoCollection mongoCollection;
    
    /** The mapper. */
    @Mock
    private Mapper mapper;
    
    /** The query. */
    @Mock
    private Query<CacheEntity> query;
    
    /** The write result. */
    @Mock
    private DeleteResult writeResult;
    
    /** The dm cache entity dao. */
    @Mock
    private DMCacheEntityDAOMongoImpl dmCacheEntityDao;
    
    /** The collection. */
    private String collection;

    /**
     * setup method is for setting up igniteEvent just after the class initialization.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        collection = dmCacheEntityDao.getOverridingCollectionName();

        igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");

        queue = new LinkedBlockingDeque<CacheEntity<?, ?>>();
        bypass.setCache(cache);
        bypass.setDmCacheEntityDao(dmCacheEntityDao);
        bypass.setQueueCapacity(Constants.THREAD_SLEEP_TIME_1000);

        entity = new CacheEntity<>();
        IgniteEventImpl event = new IgniteEventImpl();
        event.setEventId("test");
        entity.withKey(new StringKey("xyz")).withValue(event).withMutationId(mutationId).withMapKey("abc_1");
        entity.setLastUpdatedTime(LocalDateTime.now());

        Mockito.when(ds.find(collection, CacheEntity.class))
                .thenReturn(query);
        Mockito.when(ds.getMapper()).thenReturn(mapper);
        Mockito.when(ds.getDatabase())
                .thenReturn(mongoDatabase);
        Mockito.when(mongoDatabase.getCollection(collection, CacheEntity.class))
                .thenReturn(mongoCollection);
        Mockito.when(ds.delete(query))
                .thenReturn(writeResult);
    }

    /**
     * Test sort.
     */
    @Test
    public void testSort() {
        List<CacheEntity> entitiesList = new ArrayList<>();

        for (int i = 0; i < TestConstants.FOUR; i++) {
            CacheEntity<StringKey, IgniteEventImpl> entityi = new CacheEntity<>();
            IgniteEventImpl eventi = new IgniteEventImpl();
            eventi.setEventId("testId" + i);
            entityi.withKey(new StringKey("xyz" + i)).withValue(eventi)
                    .withMapKey("abc_" + i).withMutationId(mutationId);
            switch (i) {
                case 0:
                    entityi.withOperation(Operation.PUT);
                    entityi.setLastUpdatedTime(LocalDateTime.parse("2020-03-12T12:30:38.839"));
                    break;
                case TestConstants.ONE:
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
                    break;
            }
            entitiesList.add(entityi);
        }
        entitiesList = bypass.sort(entitiesList);

        for (int i = 0; i < entitiesList.size() - 1; i++) {
            Assert.assertTrue(entitiesList.get(i).getLastUpdatedTime()
                    .isBefore(entitiesList.get(i + 1).getLastUpdatedTime()));
        }
    }

    /**
     * Test populate queue.
     */
    @Test
    public void testPopulateQueue() {
        BlockingDeque<CacheEntity<?, ?>> queue = new LinkedBlockingDeque<>();
        for (int i = 0; i < TestConstants.FOUR; i++) {
            CacheEntity<StringKey, IgniteEventImpl> entityi = new CacheEntity<>();
            IgniteEventImpl eventi = new IgniteEventImpl();
            eventi.setEventId("testId" + i);
            entityi.withKey(new StringKey("xyz" + i)).withValue(eventi)
                    .withMapKey("abc_" + i).withMutationId(mutationId);
            entityi.setLastUpdatedTime(LocalDateTime.now());
            switch (i) {
                case 0:
                    entityi.withOperation(Operation.PUT);
                    break;
                case TestConstants.ONE:
                    entityi.withOperation(Operation.DEL);
                    break;
                case TestConstants.TWO:
                    entityi.withOperation(Operation.DEL_FROM_MAP);
                    break;
                case TestConstants.THREE:
                    entityi.withOperation(Operation.PUT_TO_MAP);
                    break;
                default:
                    break;
            }
            dmCacheEntityDao.save(entityi);
        }

        Mockito.verify(dmCacheEntityDao, Mockito.times(TestConstants.FOUR)).save(Mockito.any(CacheEntity.class));

        bypass.setQueue(queue);
        ReflectionTestUtils.setField(bypass, "numCacheBypassThreads", TestConstants.ONE);
        ReflectionTestUtils.setField(bypass, "waitTime", TestConstants.INT_1000);
        bypass.setup();
    }

    /**
     * Test cache bypass thread for put operation.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testCacheBypassThreadForPutOperation() throws InterruptedException {
        entity.withOperation(Operation.PUT);
        bypass.setQueueCapacity(Constants.THREAD_SLEEP_TIME_1000);
        ReflectionTestUtils.setField(bypass, "numCacheBypassThreads", TestConstants.ONE);
        ReflectionTestUtils.setField(bypass, "waitTime", TestConstants.INT_1000);
        bypass.setup();
        bypass.processEvents(entity);
        ArgumentCaptor<PutEntityRequest> putRequestArgument = ArgumentCaptor.forClass(PutEntityRequest.class);
        Mockito.verify(cache, Mockito.times(1)).putEntity(putRequestArgument.capture());

        PutEntityRequest putRequest = putRequestArgument.getValue();
        String actualKey = putRequest.getKey();
        IgniteEventImpl actualValue = (IgniteEventImpl) putRequest.getValue();

        Assert.assertEquals(entity.getKey().convertToString(), actualKey);
        Assert.assertEquals(entity.getValue(), actualValue);
    }

    /**
     * Test cache bypass thread for put to map operation.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testCacheBypassThreadForPutToMapOperation() throws InterruptedException {
        entity.withOperation(Operation.PUT_TO_MAP);
        ReflectionTestUtils.setField(bypass, "numCacheBypassThreads", TestConstants.ONE);
        ReflectionTestUtils.setField(bypass, "waitTime", TestConstants.INT_1000);
        bypass.setup();
        bypass.processEvents(entity);
        ArgumentCaptor<PutMapOfEntitiesRequest> putRequestArgument
                = ArgumentCaptor.forClass(PutMapOfEntitiesRequest.class);
        Mockito.verify(cache, Mockito.times(1)).putMapOfEntities(putRequestArgument.capture());

        PutMapOfEntitiesRequest putRequest = putRequestArgument.getValue();
        String actualMapKey = putRequest.getKey();

        Assert.assertEquals(entity.getMapKey(), actualMapKey);
    }

    /**
     * Test cache bypass thread for delete operation.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testCacheBypassThreadForDeleteOperation() throws InterruptedException {
        entity.withOperation(Operation.DEL);
        ReflectionTestUtils.setField(bypass, "numCacheBypassThreads", TestConstants.ONE);
        ReflectionTestUtils.setField(bypass, "waitTime", TestConstants.INT_1000);
        bypass.setup();
        bypass.processEvents(entity);
        ArgumentCaptor<DeleteEntryRequest> deleteRequestArgument = ArgumentCaptor.forClass(DeleteEntryRequest.class);
        Mockito.verify(cache, Mockito.times(1)).delete(deleteRequestArgument.capture());

        DeleteEntryRequest deleteRequest = deleteRequestArgument.getValue();
        String actualKey = deleteRequest.getKey();

        Assert.assertEquals(entity.getKey().convertToString(), actualKey);

    }

    /**
     * Test cache bypass thread for delete from map operation.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testCacheBypassThreadForDeleteFromMapOperation() throws InterruptedException {
        entity.withOperation(Operation.DEL_FROM_MAP);
        ReflectionTestUtils.setField(bypass, "numCacheBypassThreads", TestConstants.ONE);
        ReflectionTestUtils.setField(bypass, "waitTime", TestConstants.INT_1000);
        bypass.setup();
        bypass.processEvents(entity);
        ArgumentCaptor<DeleteMapOfEntitiesRequest> deleteRequestArgument
                = ArgumentCaptor.forClass(DeleteMapOfEntitiesRequest.class);
        Mockito.verify(cache, Mockito.times(1)).deleteMapOfEntities(deleteRequestArgument.capture());

        DeleteMapOfEntitiesRequest deleteRequest = deleteRequestArgument.getValue();
        String actualMapKey = deleteRequest.getKey();

        Assert.assertEquals(entity.getMapKey(), actualMapKey);
    }

    /**
     * Test setup with negative queue capacity.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetupWithNegativeQueueCapacity() {
        bypass.setQueueCapacity(-Constants.THREAD_SLEEP_TIME_1000);
        bypass.setup();
        String message = "Queue capacity should be greater than 100.";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(message);
    }

    /**
     * Inner Class {@link org.eclipse.ecsp.analytics.stream.base.stores.CacheBypassIntegrationTest.StringKey}.
     * Implements {@link CacheKeyConverter}.
     */
    public class StringKey implements CacheKeyConverter<StringKey> {

        /** The key. */
        private String key;

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

        /**
         * Hash code.
         *
         * @return the int
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        /**
         * Equals.
         *
         * @param obj the obj
         * @return true, if successful
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StringKey other = (StringKey) obj;
            if (!getOuterType().equals(other.getOuterType())) {
                return false;
            }
            if (key == null) {
                if (other.key != null) {
                    return false;
                }
            } else if (!key.equals(other.key)) {
                return false;
            }
            return true;
        }

        /**
         * Gets the outer type.
         *
         * @return the outer type
         */
        private CacheBypassTest getOuterType() {
            return CacheBypassTest.this;
        }

    }
}