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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.stores.CacheBypass;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.cache.GetMapOfEntitiesRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.RetryRecord;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.dao.DMARetryRecordDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.key.RetryRecordKey;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.eclipse.ecsp.utils.metrics.InternalCacheGuage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;



/**
 * DMARetryRecordDAOCacheBackedInMemoryImplTest UT class for {@link DMARetryRecordDAOCacheBackedInMemoryImpl}.
 */
public class DMARetryRecordDAOCacheBackedInMemoryImplTest {

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The map key. */
    private String mapKey;

    /** The dao. */
    @InjectMocks
    private DMARetryRecordDAOCacheBackedInMemoryImpl dao;

    /** The cache. */
    @Mock
    private IgniteCache cache;

    /** The bypass. */
    @Mock
    private CacheBypass bypass;

    /** The cache guage. */
    @Mock
    private InternalCacheGuage cacheGuage;

    /** The ignite key. */
    private IgniteStringKey igniteKey = new IgniteStringKey();
    
    /** The entity. */
    private DeviceMessage entity;
    
    /** The message id. */
    private String messageId = "message123";
    
    /** The task id. */
    private String taskId = "taskId";
    
    /** The message id key. */
    private RetryRecordKey messageIdKey = new RetryRecordKey(messageId, taskId);
    
    /** The message id 2. */
    private String messageId2 = "message456";
    
    /** The message id 2 key. */
    private RetryRecordKey messageId2Key = new RetryRecordKey(messageId2, taskId);

    /** The transformer. */
    private DeviceMessageIgniteEventTransformer transformer = new DeviceMessageIgniteEventTransformer();

    /**
     * setup().
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mapKey = RetryRecordKey.getMapKey(serviceName, taskId);
        dao.initialize(taskId);
        igniteKey.setKey("abc");
        IgniteEventImpl igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");

        entity = new DeviceMessage(transformer.toBlob(igniteEvent), Version.V1_0,
                igniteEvent, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
    }

    /**
     * Test put string DMA retry event.
     */
    @Test
    public void testPutStringDMARetryEvent() {
        RetryRecord retryEvent = new RetryRecord(igniteKey, entity, 0L);
        dao.putToMap(mapKey, messageIdKey, retryEvent,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        Assert.assertEquals(retryEvent, dao.get(messageIdKey));
    }

    /**
     * Test delete string.
     */
    @Test
    public void testDeleteString() {
        RetryRecord retryEvent = new RetryRecord(igniteKey, entity, 0L);
        dao.putToMap(mapKey, messageIdKey, retryEvent,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        dao.deleteFromMap(mapKey, messageIdKey, Optional.empty(), InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        Assert.assertNull(dao.get(messageIdKey));
    }

    /**
     * Test get.
     */
    @Test
    public void testGet() {
        RetryRecord retryEvent = new RetryRecord(igniteKey, entity, 0L);
        dao.putToMap(mapKey, messageIdKey, retryEvent,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        igniteKey.setKey("bcd");
        RetryRecord retryEvent2 = new RetryRecord(igniteKey, entity, 0L);
        dao.putToMap(mapKey, messageId2Key, retryEvent2,
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        Assert.assertEquals(retryEvent, dao.get(messageIdKey));
        Assert.assertEquals(retryEvent2, dao.get(messageId2Key));
    }

    /**
     * Test sync with cache.
     */
    @Test
    public void testSyncWithCache() {
        Map<String, IgniteEntity> map = new HashMap<String, IgniteEntity>();
        map.put("taskId:abc", entity);
        map.put("taskId:bcd", entity);
        dao.setServiceName(serviceName);
        Mockito.when(cache.getMapOfEntities(Mockito.any(GetMapOfEntitiesRequest.class))).thenReturn(map);
        dao.initialize(taskId);
        Object result = dao.get(new RetryRecordKey("abc", taskId));
        Assert.assertEquals(entity, result);
        Object result2 = dao.get(new RetryRecordKey("bcd", taskId));
        Assert.assertEquals(entity, result2);
    }

}