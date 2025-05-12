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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.analytics.stream.base.stores.CacheBypass;
import org.eclipse.ecsp.analytics.stream.base.stores.CacheEntity;
import org.eclipse.ecsp.analytics.stream.base.stores.CachedSortedMapStateStore;
import org.eclipse.ecsp.cache.GetMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.redis.IgniteCacheRedisImpl;
import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.key.RetryBucketKey;
import org.eclipse.ecsp.utils.metrics.InternalCacheGuage;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * {@link CachedSortedMapStateStoreTest}.
 */
public class CachedSortedMapStateStoreTest {
    
    /** The key. */
    private RetryBucketKey key;
    
    /** The ignite event. */
    private IgniteEventImpl igniteEvent;

    /** The store. */
    @InjectMocks
    private CachedSortedMapStateStore<RetryBucketKey, IgniteEventImpl> store;

    /** The cache. */
    @Mock
    private IgniteCache cache;

    /** The bypass. */
    @Mock
    private CacheBypass bypass;

    /** The cache guage. */
    @Mock
    private InternalCacheGuage cacheGuage;

    /** The task id. */
    private String taskId = "test_id";

    /**
     * setup() to setup igniteEvent.
     */
    @Before
    public void setup() {
        key = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);
        MockitoAnnotations.initMocks(this);
        igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");
        store.setTaskId(taskId);
    }

    /**
     * Test set task id.
     */
    @Test
    public void testSetTaskId() {
        store.setTaskId(null);
        store.setTaskId(taskId);
        String testTaskId = (String) ReflectionTestUtils.getField(store, "taskId");
        Assert.assertEquals(taskId, testTaskId);
    }

    /**
     * Test sync with redis.
     */
    @Test
    public void testSyncWithRedis() {
        String prefix = DMAConstants.RETRY_BUCKET + "servicename:taskId:";
        String regex = prefix + "*";
        Map<String, IgniteEntity> map = new HashMap<String, IgniteEntity>();
        map.put("123", igniteEvent);
        IgniteCache igniteCache = Mockito.mock(IgniteCacheRedisImpl.class);
        Mockito.when(igniteCache.getKeyValuePairsForRegex(regex, Optional.of(false))).thenReturn(map);
        store.setCache(igniteCache);
        store.syncWithcache(regex, new RetryBucketKey());
        Assert.assertEquals(igniteEvent, store.get(key));

    }

    /**
     * Test put without mutation id.
     */
    @Test
    public void testPutWithoutMutationId() {
        store.put(key, igniteEvent, "dummy_cache");
        Assert.assertEquals(igniteEvent, store.get(key));
    }

    /**
     * Test put with mutation id.
     */
    @Test
    public void testPutWithMutationId() {
        OffsetMetadata meta = new OffsetMetadata(null, TestConstants.THREAD_SLEEP_TIME_1000);
        store.put(key, igniteEvent, Optional.of(meta), "dummy_cache");
        Assert.assertEquals(igniteEvent, store.get(key));
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test put if absent.
     */
    @Test
    public void testPutIfAbsent() {
        Object oldValue = store.putIfAbsent(key, igniteEvent, Optional.empty(), "dummy_cache");
        Assert.assertNull(oldValue);
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
        Assert.assertEquals(igniteEvent, store.get(key));
    }

    /**
     * Test delete without mutation id.
     */
    @Test
    public void testDeleteWithoutMutationId() {
        store.put(key, igniteEvent);
        store.delete(key, "dummy_cache");
        Assert.assertNull(store.get(key));
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test head map.
     */
    @Test
    public void testHeadMap() {
        RetryBucketKey key1 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);
        RetryBucketKey key2 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_223);
        RetryBucketKey key3 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_323);
        RetryBucketKey key4 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_423);
        RetryBucketKey key5 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_523);

        OffsetMetadata meta = new OffsetMetadata(null, TestConstants.THREAD_SLEEP_TIME_1000);
        store.put(key5, igniteEvent, Optional.of(meta), "dummy_cache");
        store.put(key2, igniteEvent, Optional.of(meta), "dummy_cache");
        store.put(key3, igniteEvent, Optional.of(meta), "dummy_cache");
        store.put(key4, igniteEvent, Optional.of(meta), "dummy_cache");
        store.put(key1, igniteEvent, Optional.of(meta), "dummy_cache");

        List<Long> expected = new LinkedList<Long>();
        expected.add(TestConstants.THREAD_SLEEP_TIME_123);
        expected.add(TestConstants.THREAD_SLEEP_TIME_223);
        expected.add(TestConstants.THREAD_SLEEP_TIME_323);
        RetryBucketKey toKey = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_400);
        KeyValueIterator<RetryBucketKey, IgniteEventImpl> itr = store.getHead(toKey);
        List<Long> actual = new LinkedList<Long>();
        while (itr.hasNext()) {
            actual.add(itr.next().key.getTimestamp());
        }
        Assert.assertEquals(expected, actual);

    }

    /**
     * Test get.
     */
    @Test
    public void testGet() {
        RetryBucketKey key1 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);
        RetryBucketKey key2 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);

        OffsetMetadata meta = new OffsetMetadata(null, TestConstants.THREAD_SLEEP_TIME_1000);
        store.put(key1, igniteEvent, Optional.of(meta), "dummy_cache");

        Assert.assertEquals(igniteEvent, store.get(key2));

    }

    /**
     * Test delete with mutation id.
     */
    @Test
    public void testDeleteWithMutationId() {
        OffsetMetadata meta = new OffsetMetadata(null, TestConstants.THREAD_SLEEP_TIME_1000);
        store.put(key, igniteEvent, Optional.of(meta), "dummy_cache");
        store.delete(key);
        Assert.assertNull(store.get(key));
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Testing equality,hashCode and other method to increase code coverage.
     */
    @Test
    public void testEqualityOfOffsetMetadata() {
        OffsetMetadata meta1 = new OffsetMetadata(null, TestConstants.THREAD_SLEEP_TIME_1000);
        OffsetMetadata meta2 = new OffsetMetadata(null, TestConstants.THREAD_SLEEP_TIME_1000);
        OffsetMetadata meta3 = new OffsetMetadata(new TopicPartition("testTopic1", TestConstants.TWELVE),
                TestConstants.THREAD_SLEEP_TIME_1000);
        assertNotEquals("Hashcode is not same", meta1.hashCode(), meta3.hashCode());
        assertEquals(meta1.hashCode(), meta2.hashCode());
        Assert.assertEquals(meta1, meta2);
        assertEquals(meta1.getPartition(), meta2.getPartition());
        assertEquals(meta1.getOffset(), meta2.getOffset());
        assertNotEquals(null, meta1);
        assertNotEquals(meta1, new Object());
        assertNotEquals(meta1, meta3);
        assertNotEquals(meta3, meta1);
        assertNotEquals(meta1, meta3);
        Assert.assertEquals(meta1, meta1);
        OffsetMetadata meta4 = new OffsetMetadata(new TopicPartition("testTopic2", TestConstants.THIRTEEN), 
                TestConstants.THREAD_SLEEP_TIME_100);
        OffsetMetadata meta5 = new OffsetMetadata(new TopicPartition("testTopic2", TestConstants.THIRTEEN), 
                TestConstants.THREAD_SLEEP_TIME_100);
        assertNotEquals(meta3, meta4);
        Assert.assertEquals(meta4, meta5);
    }

    /**
     * Test put to map.
     */
    @Test
    public void testPutToMap() {
        RetryBucketKey key1 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);
        String prefix = RetryBucketKey.getMapKey("service", "task");
        store.putToMap(prefix, key1, igniteEvent, Optional.empty(), "dummy_cache");
        Assert.assertEquals(igniteEvent, store.get(key1));
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test put to map if absent.
     */
    @Test
    public void testPutToMapIfAbsent() {
        RetryBucketKey key1 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);
        String prefix = RetryBucketKey.getMapKey("service", "task");
        store.putToMapIfAbsent(prefix, key1, igniteEvent, Optional.empty(), "dummy_cache");
        Assert.assertEquals(igniteEvent, store.get(key1));
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test delete from map.
     */
    @Test
    public void testDeleteFromMap() {
        RetryBucketKey key1 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);
        String prefix = RetryBucketKey.getMapKey("service", "task");
        store.deleteFromMap(prefix, key1, Optional.empty(), "dummy_cache");
        Mockito.verify(bypass, Mockito.times(1)).processEvents(Mockito.any(CacheEntity.class));
    }

    /**
     * Test sync with map cache.
     */
    @Test
    public void testSyncWithMapCache() {
        RetryBucketKey key1 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);
        String prefix = RetryBucketKey.getMapKey("service", "task");
        Map<String, IgniteEntity> pairs = new HashMap<String, IgniteEntity>();
        pairs.put(key1.convertToString(), igniteEvent);
        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(pairs);
        store.syncWithMapCache(prefix, key1, "dummy_cache");
        
        Mockito.verify(cache, Mockito.atMost(1)).getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class));
        ArgumentCaptor<GetMapOfEntitiesRequest> argument = ArgumentCaptor.forClass(GetMapOfEntitiesRequest.class);
        Mockito.verify(cache).getMapOfEntities(argument.capture());
        GetMapOfEntitiesRequest req = argument.getValue();
        Assert.assertEquals(req.getKey(), prefix);
        IgniteEventImpl actual = store.get(key1);
        Assert.assertNotNull(actual);
        Assert.assertEquals(igniteEvent.getEventId(), actual.getEventId());
    }

}