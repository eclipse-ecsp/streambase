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
import org.eclipse.ecsp.stream.dma.dao.ShoulderTapRetryBucketDAOCacheImpl;
import org.eclipse.ecsp.stream.dma.dao.key.ShoulderTapRetryBucketKey;
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
 * UT class {@link DMAShoulderTapRetryBucketDAOCacheBackedInMemoryImplTest}.
 */
public class DMAShoulderTapRetryBucketDAOCacheBackedInMemoryImplTest {

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The map key. */
    private String mapKey;

    /** The sorted dao. */
    @InjectMocks
    private ShoulderTapRetryBucketDAOCacheImpl sortedDao;

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
    private ShoulderTapRetryBucketKey key1;
    
    /** The key 2. */
    private ShoulderTapRetryBucketKey key2;
    
    /** The key 3. */
    private ShoulderTapRetryBucketKey key3;
    
    /** The key 4. */
    private ShoulderTapRetryBucketKey key4;

    /** The retry vehicle ids 1. */
    private RetryRecordIds retryVehicleIds1;
    
    /** The retry vehicle ids 2. */
    private RetryRecordIds retryVehicleIds2;
    
    /** The retry vehicle ids 3. */
    private RetryRecordIds retryVehicleIds3;
    
    /** The retry vehicle ids 4. */
    private RetryRecordIds retryVehicleIds4;
    
    /** The vehicle ids set 1. */
    private ConcurrentHashSet<String> vehicleIdsSet1;
    
    /** The vehicle ids set 2. */
    private ConcurrentHashSet<String> vehicleIdsSet2;
    
    /** The vehicle ids set 3. */
    private ConcurrentHashSet<String> vehicleIdsSet3;
    
    /** The vehicle ids set 4. */
    private ConcurrentHashSet<String> vehicleIdsSet4;

    /**
     * setup().
     */
    @Before
    public void setup() {
        key1 = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_3000);
        key2 = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        key3 = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_5000);
        key4 = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1500);

        MockitoAnnotations.initMocks(this);
        mapKey = ShoulderTapRetryBucketKey.getMapKey(serviceName, "taskId");
        sortedDao.initialize("taskId");
        vehicleIdsSet1 = new ConcurrentHashSet<String>();
        vehicleIdsSet1.add("message123");
        vehicleIdsSet1.add("message456");
        retryVehicleIds1 = new RetryRecordIds(Version.V1_0, vehicleIdsSet1);

        vehicleIdsSet2 = new ConcurrentHashSet<String>();
        vehicleIdsSet2.add("message223");
        vehicleIdsSet2.add("message256");
        retryVehicleIds2 = new RetryRecordIds(Version.V1_0, vehicleIdsSet2);

        vehicleIdsSet3 = new ConcurrentHashSet<String>();
        vehicleIdsSet3.add("message323");
        vehicleIdsSet3.add("message356");
        retryVehicleIds3 = new RetryRecordIds(Version.V1_0, vehicleIdsSet3);

        vehicleIdsSet4 = new ConcurrentHashSet<String>();
        vehicleIdsSet4.add("message423");
        vehicleIdsSet4.add("message456");
        retryVehicleIds4 = new RetryRecordIds(Version.V1_0, vehicleIdsSet4);

        sortedDao.putToMap(mapKey, key1, retryVehicleIds1,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        sortedDao.putToMap(mapKey, key2, retryVehicleIds2,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        sortedDao.putToMap(mapKey, key3, retryVehicleIds3,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        sortedDao.putToMap(mapKey, key4, retryVehicleIds4,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);

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
        Assert.assertEquals(vehicleIdsSet1, sortedDao.get(key1).getRecordIds());
        sortedDao.update(mapKey, key1, "message556");

        Set<String> expected = new HashSet<String>();
        expected.add("message123");
        expected.add("message456");
        expected.add("message556");

        Assert.assertEquals(expected, sortedDao.get(key1).getRecordIds());

        ShoulderTapRetryBucketKey newKey = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_6000);
        Assert.assertNull(sortedDao.get(newKey));
        sortedDao.update(mapKey, newKey, "message556");

        expected = new HashSet<String>();
        expected.add("message556");

        Assert.assertEquals(expected, sortedDao.get(newKey).getRecordIds());

    }

    /**
     * Test put long set of string.
     */
    @Test
    public void testPutLongSetOfString() {
        ShoulderTapRetryBucketKey newKey = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_6000);
        Assert.assertNull(sortedDao.get(newKey));
        ConcurrentHashSet<String> expected = new ConcurrentHashSet<String>();
        expected.add("message123");
        expected.add("message456");
        expected.add("message556");
        RetryRecordIds expectedMsgIds = new RetryRecordIds(Version.V1_0, expected);
        sortedDao.putToMap(mapKey, newKey, expectedMsgIds,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        Assert.assertEquals(expected, sortedDao.get(newKey).getRecordIds());
    }

    /**
     * Test delete key.
     */
    @Test
    public void testDeleteKey() {
        Assert.assertEquals(vehicleIdsSet1, sortedDao.get(key1).getRecordIds());
        sortedDao.deleteFromMap(mapKey, key1, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
        Assert.assertNull(sortedDao.get(key1));
    }

    /**
     * Test delete long string.
     */
    @Test
    public void testDeleteLongString() {
        sortedDao.deleteVehicleId(mapKey, key1, "message124");
        // Cannot delte elemt that is not present
        Assert.assertEquals(retryVehicleIds1, sortedDao.get(key1));

        sortedDao.deleteVehicleId(mapKey, key1, "message123");
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
        Set<ShoulderTapRetryBucketKey> expectedKeySet = new HashSet<ShoulderTapRetryBucketKey>();
        expectedKeySet.add(key2);
        expectedKeySet.add(key4);
        expectedKeySet.add(key1);
        expectedKeySet.add(key3);

        Set<ShoulderTapRetryBucketKey> actualKeySet = new HashSet<ShoulderTapRetryBucketKey>();
        KeyValueIterator<ShoulderTapRetryBucketKey, RetryRecordIds> headMapItr1 = sortedDao.getHead(key3);

        while (headMapItr1.hasNext()) {
            KeyValue<ShoulderTapRetryBucketKey, RetryRecordIds> keyValue = headMapItr1.next();
            ShoulderTapRetryBucketKey timestamp = keyValue.key;
            actualKeySet.add(timestamp);
        }
        Assert.assertEquals(expectedKeySet, actualKeySet);

        expectedKeySet = new HashSet<ShoulderTapRetryBucketKey>();
        expectedKeySet.add(key2);
        expectedKeySet.add(key4);
        expectedKeySet.add(key1);
        actualKeySet = new HashSet<ShoulderTapRetryBucketKey>();
        KeyValueIterator<ShoulderTapRetryBucketKey, RetryRecordIds> headMapItr2 = sortedDao
                .getHead((new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_4000)));
        while (headMapItr2.hasNext()) {
            KeyValue<ShoulderTapRetryBucketKey, RetryRecordIds> keyValue = headMapItr2.next();
            ShoulderTapRetryBucketKey timestamp = keyValue.key;
            actualKeySet.add(timestamp);
        }
        Assert.assertEquals(expectedKeySet, actualKeySet);

    }

    /**
     * Test get tail map long boolean.
     */
    @Test
    public void testGetTailMapLongBoolean() {
        Set<ShoulderTapRetryBucketKey> expectedKeySet = new HashSet<ShoulderTapRetryBucketKey>();

        expectedKeySet = new HashSet<ShoulderTapRetryBucketKey>();
        expectedKeySet.add(key1);
        expectedKeySet.add(key3);
        Set<ShoulderTapRetryBucketKey> actualKeySet = new HashSet<ShoulderTapRetryBucketKey>();
        KeyValueIterator<ShoulderTapRetryBucketKey, RetryRecordIds> headMapItr1 = sortedDao.getTail(key1);
        while (headMapItr1.hasNext()) {
            KeyValue<ShoulderTapRetryBucketKey, RetryRecordIds> keyValue = headMapItr1.next();
            ShoulderTapRetryBucketKey timestamp = keyValue.key;
            actualKeySet.add(timestamp);
        }
        Assert.assertEquals(expectedKeySet, actualKeySet);

    }

    /**
     * Test get sub map long boolean.
     */
    @Test
    public void testGetSubMapLongBoolean() {
        Set<ShoulderTapRetryBucketKey> expectedKeySet = new HashSet<ShoulderTapRetryBucketKey>();

        expectedKeySet = new HashSet<ShoulderTapRetryBucketKey>();
        expectedKeySet.add(key4);
        expectedKeySet.add(key1);
        expectedKeySet.add(key3);
        Set<ShoulderTapRetryBucketKey> actualKeySet = new HashSet<ShoulderTapRetryBucketKey>();
        KeyValueIterator<ShoulderTapRetryBucketKey, RetryRecordIds> subMapItr = sortedDao.range(key4, key3);
        while (subMapItr.hasNext()) {
            KeyValue<ShoulderTapRetryBucketKey, RetryRecordIds> keyValue = subMapItr.next();
            ShoulderTapRetryBucketKey timestamp = keyValue.key;
            actualKeySet.add(timestamp);
        }
        Assert.assertEquals(expectedKeySet, actualKeySet);
    }

    /**
     * Test get long.
     */
    @Test
    public void testGetLong() {
        Assert.assertEquals(retryVehicleIds1, sortedDao.get(key1));
        Assert.assertEquals(retryVehicleIds2, sortedDao.get(key2));
        Assert.assertEquals(retryVehicleIds3, sortedDao.get(key3));
        Assert.assertEquals(retryVehicleIds4, sortedDao.get(key4));
    }

    /**
     * Test sync with cache.
     */
    @Test
    public void testSyncWithCache() {
        Map<String, IgniteEntity> map = new HashMap<String, IgniteEntity>();
        map.put("123", retryVehicleIds1);
        map.put("223", retryVehicleIds2);
        sortedDao.setServiceName(serviceName);
        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(map);
        sortedDao.initialize("taskId");
        Assert.assertEquals(retryVehicleIds1, sortedDao.get(
                new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123)));
        Assert.assertEquals(retryVehicleIds2, sortedDao.get(
                new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_223)));
    }

}