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

import org.eclipse.ecsp.analytics.stream.base.stores.CacheBypass;
import org.eclipse.ecsp.analytics.stream.base.stores.CacheEntity;
import org.eclipse.ecsp.analytics.stream.base.stores.CacheKeyConverter;
import org.eclipse.ecsp.analytics.stream.base.stores.CachedMapStateStore;
import org.eclipse.ecsp.cache.DeleteMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.GetMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutEntityRequest;
import org.eclipse.ecsp.cache.PutMapOfEntitiesRequest;
import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.utils.metrics.InternalCacheGuage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Test Class for {@link CachedMapStateStore}.
 */
public class CacheMapStateStoreTest {

    /** The key. */
    private String key = "abc";
    
    /** The string key. */
    private StringKey stringKey;
    
    /** The ignite event. */
    private IgniteEventImpl igniteEvent;

    /** The id. */
    private String id = "test_id";

    /** The store. */
    @InjectMocks
    private CachedMapStateStore<StringKey, IgniteEventImpl> store;

    /** The cache. */
    @Mock
    private IgniteCache cache;

    /** The bypass. */
    @Mock
    private CacheBypass bypass;

    /** The cache guage. */
    @Mock
    private InternalCacheGuage cacheGuage;

    /**
     * setup method is for setting up igniteEvent just after the class initialization.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        //cache = Mockito.mock(IgniteCacheRedisImpl.class);

        igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");
        stringKey = new StringKey(key);
        store.setTaskId(id);
    }

    /**
     * Close.
     */
    @After
    public void close() {
        store.close();

    }

    /**
     * Test set task id.
     */
    @Test
    public void testSetTaskId() {
        store.setTaskId(null);
        store.setTaskId(id);
        String taskId = (String) ReflectionTestUtils.getField(store, "taskId");
        Assert.assertEquals(id, taskId);
    }

    /**
     * Test put if persist in ignite cache.
     */
    @Test
    public void testPutIfPersistInIgniteCache() {
        store.setPersistInIgniteCache(true);
        store.setCache(cache);
        store.setBypass(bypass);
        store.put(stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test delete if persist in ignite cache.
     */
    @Test
    public void testDeleteIfPersistInIgniteCache() {
        store.setPersistInIgniteCache(true);
        store.setCache(cache);
        store.setBypass(bypass);
        store.delete(stringKey, Optional.empty(), "dummy_cache");
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test sync with redis.
     */
    @Test
    public void testSyncWithRedis() {
        Map<String, IgniteEntity> map = new HashMap<String, IgniteEntity>();
        map.put(key, igniteEvent);
        Mockito.when(cache.getKeyValuePairsForRegex("regex", Optional.of(false))).thenReturn(map);
        store.setCache(cache);
        store.syncWithcache("regex", new StringKey());
        Assert.assertEquals(igniteEvent, store.get(stringKey));

    }

    /**
     * Test put without mutation id.
     */
    @Test
    public void testPutWithoutMutationId() {
        store.put(stringKey, igniteEvent, "dummy_cache");
        Assert.assertEquals(igniteEvent, store.get(stringKey));
    }

    /**
     * Test put to cache.
     */
    @Test
    public void testPutToCache() {
        store.setPersistInIgniteCache(false);
        store.put(stringKey, igniteEvent);
        Assert.assertEquals(igniteEvent, store.get(stringKey));
        Mockito.verify(cache, Mockito.atMost(0)).putEntity(Mockito.any(PutEntityRequest.class));
    }

    /**
     * Test put with mutation id.
     */
    @Test
    public void testPutWithMutationId() {
        store.put(stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        Assert.assertEquals(igniteEvent, store.get(stringKey));
    }

    /**
     * Test delete without mutation id.
     */
    @Test
    public void testDeleteWithoutMutationId() {
        store.put(stringKey, igniteEvent);
        store.delete(stringKey, "dummy_cache");
        Assert.assertNull(store.get(stringKey));
    }

    /**
     * Test delete with mutation id.
     */
    @Test
    public void testDeleteWithMutationId() {
        store.put(stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        store.delete(stringKey);
        Assert.assertNull(store.get(stringKey));
    }

    /**
     * Test put to map.
     */
    @Test
    public void testPutToMap() {
        store.setPersistInIgniteCache(true);
        store.putToMap("prefix", stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        store.setBypass(bypass);
        Assert.assertEquals(igniteEvent, store.get(stringKey));
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test put to map if persistance false.
     */
    @Test
    public void testPutToMapIfPersistanceFalse() {
        store.setPersistInIgniteCache(false);
        store.putToMap("prefix", stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        Mockito.verify(cache, Mockito.atMost(0)).putMapOfEntities(Mockito.any(PutMapOfEntitiesRequest.class));
        Assert.assertEquals(igniteEvent, store.get(stringKey));
    }

    /**
     * Test put to map if absent.
     */
    @Test
    public void testPutToMapIfAbsent() {
        store.setPersistInIgniteCache(true);
        store.putToMapIfAbsent("prefix", stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
        Assert.assertEquals(igniteEvent, store.get(stringKey));
    }

    /**
     * Test put to map if absent persistance false.
     */
    @Test
    public void testPutToMapIfAbsentPersistanceFalse() {
        store.setPersistInIgniteCache(false);
        store.putToMapIfAbsent("prefix", stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        Mockito.verify(cache, Mockito.atMost(0)).putMapOfEntities(Mockito.any(PutMapOfEntitiesRequest.class));
        Assert.assertEquals(igniteEvent, store.get(stringKey));
    }

    /**
     * Test put to map if absent value present.
     */
    @Test
    public void testPutToMapIfAbsentValuePresent() {
        CachedMapStateStore<StringKey, IgniteEventImpl> storeMock = Mockito.mock(CachedMapStateStore.class);
        IgniteCache cacheMock = Mockito.mock(IgniteCache.class);
        storeMock.setPersistInIgniteCache(true);
        Mockito.when(storeMock.putIfAbsent(stringKey, igniteEvent)).thenReturn(igniteEvent);
        storeMock.putToMapIfAbsent("prefix", stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        Mockito.verify(bypass, Mockito.atMost(0)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test delete from map.
     */
    @Test
    public void testDeleteFromMap() {
        store.setPersistInIgniteCache(true);
        store.deleteFromMap("prefix", stringKey, Optional.empty(), "dummy_cache");
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test delete from map if persistence false.
     */
    @Test
    public void testDeleteFromMapIfPersistenceFalse() {
        store.setPersistInIgniteCache(false);
        store.deleteFromMap("prefix", stringKey, Optional.empty(), "dummy_cache");
        Mockito.verify(cache, Mockito.atMost(0)).deleteMapOfEntities(Mockito.any(DeleteMapOfEntitiesRequest.class));
    }

    /**
     * Test sync with map cache.
     */
    @Test
    public void testSyncWithMapCache() {
        Map<String, IgniteEntity> pairs = new HashMap<String, IgniteEntity>();
        pairs.put(stringKey.convertToString(), igniteEvent);
        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(pairs);
        store.syncWithMapCache("prefix", stringKey, "dummy_cache");
        
        Mockito.verify(cache, Mockito.atMost(1)).getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class));
        ArgumentCaptor<GetMapOfEntitiesRequest> argument = ArgumentCaptor.forClass(GetMapOfEntitiesRequest.class);
        Mockito.verify(cache).getMapOfEntities(argument.capture());
        GetMapOfEntitiesRequest req = argument.getValue();
        Assert.assertEquals("prefix", req.getKey());
        IgniteEventImpl actual = store.get(stringKey);
        Assert.assertNotNull(actual);
        Assert.assertEquals(igniteEvent.getEventId(), actual.getEventId());
    }

    /**
     * Test sync with map cache in case of sub services.
     */
    @Test
    public void testSyncWithMapCacheInCaseOfSubServices() {
        ReflectionTestUtils.setField(store, "subServices", "fleet");
        Map<String, IgniteEntity> pairs = new HashMap<String, IgniteEntity>();
        pairs.put(stringKey.convertToString(), igniteEvent);

        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(pairs);
        store.syncWithMapCache("VEHICLE_DEVICE_MAPPING:abc:fleet", stringKey, "dummy_cache");

        Mockito.verify(cache, Mockito.atMost(1)).getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class));
        ArgumentCaptor<GetMapOfEntitiesRequest> argument = ArgumentCaptor.forClass(GetMapOfEntitiesRequest.class);
        Mockito.verify(cache).getMapOfEntities(argument.capture());
        GetMapOfEntitiesRequest req = argument.getValue();
        Assert.assertEquals("VEHICLE_DEVICE_MAPPING:abc:fleet", req.getKey());
        StringKey actualKey = new StringKey(key + ";fleet");
        IgniteEventImpl actual = store.get(actualKey);
        Assert.assertNotNull(actual);
        Assert.assertEquals(igniteEvent.getEventId(), actual.getEventId());
    }

    /**
     * Test sync with map cache without sub services.
     */
    @Test
    public void testSyncWithMapCacheWithoutSubServices() {
        Map<String, IgniteEntity> pairs = new HashMap<String, IgniteEntity>();
        pairs.put(stringKey.convertToString(), igniteEvent);

        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(pairs);
        store.syncWithMapCache("VEHICLE_DEVICE_MAPPING:abc", stringKey, "dummy_cache");

        Mockito.verify(cache, Mockito.atMost(1)).getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class));
        ArgumentCaptor<GetMapOfEntitiesRequest> argument = ArgumentCaptor.forClass(GetMapOfEntitiesRequest.class);
        Mockito.verify(cache).getMapOfEntities(argument.capture());
        GetMapOfEntitiesRequest req = argument.getValue();
        Assert.assertEquals("VEHICLE_DEVICE_MAPPING:abc", req.getKey());
        IgniteEventImpl actual = store.get(stringKey);
        Assert.assertNotNull(actual);
        Assert.assertEquals(igniteEvent.getEventId(), actual.getEventId());
    }

    /**
     * Test force get.
     */
    @Test
    public void testForceGet() {
        Mockito.when(cache.getEntity(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(igniteEvent);
        store.forceGet("prefix", stringKey);
        Mockito.verify(cache, Mockito.atMost(1)).getEntity(Mockito.any(GetMapOfEntitiesRequest.class));
        ArgumentCaptor<GetMapOfEntitiesRequest> argument = ArgumentCaptor.forClass(GetMapOfEntitiesRequest.class);
        Mockito.verify(cache).getMapOfEntities(argument.capture());
        GetMapOfEntitiesRequest req = argument.getValue();
        Assert.assertEquals("prefix", req.getKey());
        Assert.assertEquals(1, req.getFields().size());
        Assert.assertTrue(req.getFields().contains(key));
    }

    /**
     * Testdelete from map.
     */
    @Test
    public void testdeleteFromMap() {
        Map<String, IgniteEntity> pairs = new HashMap<String, IgniteEntity>();
        pairs.put(stringKey.convertToString(), igniteEvent);
        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(pairs);
        store.deleteFromMap("prefix", stringKey, Optional.empty(), "dummy_cache");
        ArgumentCaptor<GetMapOfEntitiesRequest> argument = ArgumentCaptor.forClass(GetMapOfEntitiesRequest.class);
        IgniteEventImpl actual = store.get(stringKey);
        Assert.assertNull(actual);
    }

    /**
     * Testput if absent.
     */
    @Test
    public void testputIfAbsent() {
        CachedMapStateStore<StringKey, IgniteEventImpl> storeMock = Mockito.mock(CachedMapStateStore.class);
        IgniteCache cacheMock = Mockito.mock(IgniteCache.class);
        storeMock.putIfAbsent(null, null);
        Mockito.when(storeMock.putIfAbsent(stringKey, igniteEvent)).thenReturn(igniteEvent);
        storeMock.putToMapIfAbsent("prefix", stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        Mockito.verify(bypass, Mockito.atMost(0)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test put if absent if persistance enabled.
     */
    @Test
    public void testPutIfAbsentIfPersistanceEnabled() {
        store.setPersistInIgniteCache(true);
        store.putIfAbsent(stringKey, igniteEvent, Optional.empty(), "dummy_cache");
        IgniteEventImpl igniteEvent2 = new IgniteEventImpl();
        igniteEvent2.setEventId("test2");
        IgniteEventImpl oldValue = (IgniteEventImpl) store.putIfAbsent(stringKey,
                igniteEvent2, Optional.empty(), "dummy_cache");

        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
        Assert.assertEquals("test", oldValue.getEventId());
    }

    /**
     * Inner Class StrinkKey.
     *Implements {@link CacheKeyConverter}
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
        private CacheMapStateStoreTest getOuterType() {
            return CacheMapStateStoreTest.this;
        }

    }

}