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

package org.eclipse.ecsp.stream.dma;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.CacheBypass;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.cache.GetMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.entities.dma.RetryRecordIds;
import org.eclipse.ecsp.stream.dma.dao.DMARetryBucketDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.key.RetryBucketKey;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.eclipse.ecsp.utils.metrics.InternalCacheGuage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;



/**
 * Test class for {@link DmaRetryBucketDaoCacheBackedInMemoryImpl}.
 */
public class DMARetryBucketDAOCacheBackedInMemoryImplTest {

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The task id. */
    private String taskId = "taskId";
    
    /** The map key. */
    private String mapKey;

    /** The sorted dao. */
    @InjectMocks
    private DMARetryBucketDAOCacheBackedInMemoryImpl sortedDao;

    /** The cache. */
    @Mock
    private IgniteCache cache;

    /** The bypass. */
    @Mock
    private CacheBypass bypass;

    /** The cache guage. */
    @Mock
    private InternalCacheGuage cacheGuage;

    /** The key 1. */
    private RetryBucketKey key1;
    
    /** The key 2. */
    private RetryBucketKey key2;
    
    /** The key 3. */
    private RetryBucketKey key3;
    
    /** The key 4. */
    private RetryBucketKey key4;

    /** The retry msg ids 1. */
    private RetryRecordIds retryMsgIds1;
    
    /** The retry msg ids 2. */
    private RetryRecordIds retryMsgIds2;
    
    /** The retry msg ids 3. */
    private RetryRecordIds retryMsgIds3;
    
    /** The retry msg ids 4. */
    private RetryRecordIds retryMsgIds4;
    
    /** The message ids set 1. */
    private ConcurrentHashSet<String> messageIdsSet1;
    
    /** The message ids set 2. */
    private ConcurrentHashSet<String> messageIdsSet2;
    
    /** The message ids set 3. */
    private ConcurrentHashSet<String> messageIdsSet3;
    
    /** The message ids set 4. */
    private ConcurrentHashSet<String> messageIdsSet4;

    /**
     * setup method is for setting up {@link RetryBucketKey} just after the class initialization.
     */
    @Before
    public void setup() {
        mapKey = RetryBucketKey.getMapKey(serviceName, taskId);
        key1 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_3000);
        key2 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        key3 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_5000);
        key4 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1500);

        MockitoAnnotations.initMocks(this);
        sortedDao.initialize(taskId);
        messageIdsSet1 = new ConcurrentHashSet<String>();
        messageIdsSet1.add("message123");
        messageIdsSet1.add("message456");
        retryMsgIds1 = new RetryRecordIds(Version.V1_0, messageIdsSet1);

        messageIdsSet2 = new ConcurrentHashSet<String>();
        messageIdsSet2.add("message223");
        messageIdsSet2.add("message256");
        retryMsgIds2 = new RetryRecordIds(Version.V1_0, messageIdsSet2);

        messageIdsSet3 = new ConcurrentHashSet<String>();
        messageIdsSet3.add("message323");
        messageIdsSet3.add("message356");
        retryMsgIds3 = new RetryRecordIds(Version.V1_0, messageIdsSet3);

        messageIdsSet4 = new ConcurrentHashSet<String>();
        messageIdsSet4.add("message423");
        messageIdsSet4.add("message456");
        retryMsgIds4 = new RetryRecordIds(Version.V1_0, messageIdsSet4);

        sortedDao.putToMap(mapKey, key1, retryMsgIds1, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        sortedDao.putToMap(mapKey, key2, retryMsgIds2, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        sortedDao.putToMap(mapKey, key3, retryMsgIds3, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        sortedDao.putToMap(mapKey, key4, retryMsgIds4, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);

    }

    /**
     * Cleanup.
     */
    @After
    public void cleanup() {
        sortedDao.close();
    }

    /**
     * Test update.
     */
    @Test
    public void testUpdate() {
        Assert.assertEquals(messageIdsSet1, sortedDao.get(key1).getRecordIds());
        sortedDao.update(key1.getMapKey(serviceName, taskId), key1, "message556");

        Set<String> expected = new HashSet<String>();
        expected.add("message123");
        expected.add("message456");
        expected.add("message556");

        Assert.assertEquals(expected, sortedDao.get(key1).getRecordIds());

        RetryBucketKey newKey = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_6000);
        Assert.assertNull(sortedDao.get(newKey));
        sortedDao.update(newKey.getMapKey(serviceName, taskId), newKey, "message556");

        expected = new HashSet<String>();
        expected.add("message556");

        Assert.assertEquals(expected, sortedDao.get(newKey).getRecordIds());

    }

    /**
     * Test put long set of string.
     */
    @Test
    public void testPutLongSetOfString() {
        RetryBucketKey newKey = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_6000);
        Assert.assertNull(sortedDao.get(newKey));
        ConcurrentHashSet<String> expected = new ConcurrentHashSet<String>();
        expected.add("message123");
        expected.add("message456");
        expected.add("message556");
        RetryRecordIds expectedMsgIds = new RetryRecordIds(Version.V1_0, expected);
        sortedDao.putToMap(mapKey, newKey, expectedMsgIds, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        Assert.assertEquals(expected, sortedDao.get(newKey).getRecordIds());
    }

    /**
     * Test delete key.
     */
    @Test
    public void testDeleteKey() {
        Assert.assertEquals(messageIdsSet1, sortedDao.get(key1).getRecordIds());
        sortedDao.deleteFromMap(mapKey, key1, Optional.empty(), InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        Assert.assertNull(sortedDao.get(key1));
    }

    /**
     * Test delete long string.
     */
    @Test
    public void testDeleteLongString() {
        String retryBucketMapKey = RetryBucketKey.getMapKey(serviceName, taskId);
        sortedDao.deleteMessageId(retryBucketMapKey, key1, "message124");
        // Cannot delte elemt that is not present
        Assert.assertEquals(retryMsgIds1, sortedDao.get(key1));

        sortedDao.deleteMessageId(retryBucketMapKey, key1, "message123");
        Set<String> expected = new HashSet<String>();
        expected.add("message456");
        // Element Deleted
        Assert.assertEquals(expected, sortedDao.get(key1).getRecordIds());
    }

    /**
     * Test get head map long boolean.
     */
    @Test
    public void testGetHeadMapLongBoolean() {

        // Including key3
        Set<RetryBucketKey> expectedKeySet = new HashSet<RetryBucketKey>();
        expectedKeySet.add(key2);
        expectedKeySet.add(key4);
        expectedKeySet.add(key1);
        expectedKeySet.add(key3);

        Set<RetryBucketKey> actualKeySet = new HashSet<RetryBucketKey>();

        KeyValueIterator<RetryBucketKey, RetryRecordIds> headMapItr1 = sortedDao.getHead(key3);
        while (headMapItr1.hasNext()) {
            KeyValue<RetryBucketKey, RetryRecordIds> keyValue = headMapItr1.next();
            RetryBucketKey timestamp = keyValue.key;
            actualKeySet.add(timestamp);
        }
        Assert.assertEquals(expectedKeySet, actualKeySet);

        expectedKeySet = new HashSet<RetryBucketKey>();
        expectedKeySet.add(key2);
        expectedKeySet.add(key4);
        expectedKeySet.add(key1);
        actualKeySet = new HashSet<RetryBucketKey>();
        KeyValueIterator<RetryBucketKey, RetryRecordIds> headMapItr2 = sortedDao
                .getHead((new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_4000)));
        while (headMapItr2.hasNext()) {
            KeyValue<RetryBucketKey, RetryRecordIds> keyValue = headMapItr2.next();
            RetryBucketKey timestamp = keyValue.key;
            actualKeySet.add(timestamp);
        }
        Assert.assertEquals(expectedKeySet, actualKeySet);

    }

    /**
     * Test get tail map long boolean.
     */
    @Test
    public void testGetTailMapLongBoolean() {
        Set<RetryBucketKey> expectedKeySet = new HashSet<RetryBucketKey>();
        expectedKeySet.add(key1);
        expectedKeySet.add(key3);
        Set<RetryBucketKey> actualKeySet = new HashSet<RetryBucketKey>();
        KeyValueIterator<RetryBucketKey, RetryRecordIds> headMapItr1 = sortedDao.getTail(key1);
        while (headMapItr1.hasNext()) {
            KeyValue<RetryBucketKey, RetryRecordIds> keyValue = headMapItr1.next();
            RetryBucketKey timestamp = keyValue.key;
            actualKeySet.add(timestamp);
        }
        Assert.assertEquals(expectedKeySet, actualKeySet);

    }

    /**
     * Test get sub map long boolean.
     */
    @Test
    public void testGetSubMapLongBoolean() {
        Set<RetryBucketKey> expectedKeySet = new HashSet<RetryBucketKey>();

        expectedKeySet = new HashSet<RetryBucketKey>();
        expectedKeySet.add(key4);
        expectedKeySet.add(key1);
        expectedKeySet.add(key3);
        Set<RetryBucketKey> actualKeySet = new HashSet<RetryBucketKey>();
        KeyValueIterator<RetryBucketKey, RetryRecordIds> subMapItr = sortedDao.range(key4, key3);
        while (subMapItr.hasNext()) {
            KeyValue<RetryBucketKey, RetryRecordIds> keyValue = subMapItr.next();
            RetryBucketKey timestamp = keyValue.key;
            actualKeySet.add(timestamp);
        }
        Assert.assertEquals(expectedKeySet, actualKeySet);
    }

    /**
     * Test get long.
     */
    @Test
    public void testGetLong() {
        Assert.assertEquals(retryMsgIds1, sortedDao.get(key1));
        Assert.assertEquals(retryMsgIds2, sortedDao.get(key2));
        Assert.assertEquals(retryMsgIds3, sortedDao.get(key3));
        Assert.assertEquals(retryMsgIds4, sortedDao.get(key4));
    }

    /**
     * Test sync with map cache.
     */
    @Test
    public void testSyncWithMapCache() {
        Map<String, IgniteEntity> map = new HashMap<String, IgniteEntity>();
        map.put("123", retryMsgIds1);
        map.put("223", retryMsgIds2);
        sortedDao.setServiceName(serviceName);
        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(map);
        sortedDao.initialize("taskId");
        Assert.assertEquals(retryMsgIds1, sortedDao.get(new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123)));
        Assert.assertEquals(retryMsgIds2, sortedDao.get(new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_223)));
    }

}