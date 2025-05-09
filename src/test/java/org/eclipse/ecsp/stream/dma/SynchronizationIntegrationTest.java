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

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.cache.GetEntityRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutEntityRequest;
import org.eclipse.ecsp.cache.PutMapOfEntitiesRequest;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.RetryRecord;
import org.eclipse.ecsp.entities.dma.RetryRecordIds;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.dao.DMARetryBucketDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.DMARetryRecordDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.stream.dma.dao.key.RetryBucketKey;
import org.eclipse.ecsp.stream.dma.dao.key.RetryRecordKey;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;



/**
 * This test class is to verify whether the in-memory state store can sync-up with redis.
 *
 * @author avadakkootko
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-handler-test.properties")
public class SynchronizationIntegrationTest extends KafkaStreamsApplicationTestBase {

    /** The cache. */
    @Autowired
    private IgniteCache cache;

    /** The transformer. */
    @Autowired
    private DeviceMessageIgniteEventTransformer transformer;

    /** The retry bucket dao. */
    @Autowired
    private DMARetryBucketDAOCacheBackedInMemoryImpl retryBucketDao;

    /** The retry record dao. */
    @Autowired
    private DMARetryRecordDAOCacheBackedInMemoryImpl retryRecordDao;

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /** The source topic. */
    private String sourceTopic = "testTopic";
    
    /** The task id. */
    private String taskId = "taskId";

    /**
     * Test put get entity.
     */
    @Test
    public void testPutGetEntity() {
        IgniteStringKey igniteKey = new IgniteStringKey();
        IgniteEventImpl igniteEvent = new IgniteEventImpl();
        igniteKey.setKey("abc");
        igniteEvent.setEventId("test");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(igniteEvent),
                Version.V1_0, igniteEvent, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        RetryRecord entityPut = new RetryRecord(igniteKey, entity, 0L);

        PutEntityRequest<RetryRecord> req = new PutEntityRequest<RetryRecord>();
        req.withKey("hello").withValue(entityPut).withNamespaceEnabled(false);

        cache.putEntity(req);
        RetryRecord entityRead = cache.getEntity(new GetEntityRequest().withKey("hello").withNamespaceEnabled(false));
        Assert.assertEquals(entityPut.getDeviceMessage().getDeviceMessageHeader().toString(),
                entityRead.getDeviceMessage().getDeviceMessageHeader().toString());
        Assert.assertEquals(entityPut.getLastRetryTimestamp(), entityRead.getLastRetryTimestamp());
        Assert.assertEquals(entityPut.getIgniteKey(), entityRead.getIgniteKey());
    }

    /**
     * Test retry record sync with cache integration.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testRetryRecordSyncWithCacheIntegration() throws InterruptedException {

        ConcurrentHashSet<String> messageIdsSet1 = new ConcurrentHashSet<String>();
        messageIdsSet1.add("message123");
        messageIdsSet1.add("message456");

        ConcurrentHashSet<String> messageIdsSet2 = new ConcurrentHashSet<String>();
        messageIdsSet2.add("message223");
        messageIdsSet2.add("message256");

        ConcurrentHashSet<String> messageIdsSet3 = new ConcurrentHashSet<String>();
        messageIdsSet3.add("message323");
        messageIdsSet3.add("message356");
        RetryRecordIds retryMsgIds3 = new RetryRecordIds(Version.V1_0, messageIdsSet3);

        retryBucketDao.setServiceName(serviceName);
        retryBucketDao.initialize("taskId");

        RetryBucketKey key123 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_123);
        RetryBucketKey key223 = new RetryBucketKey(TestConstants.TWO_TWO_THREE);
        RetryBucketKey key323 = new RetryBucketKey(TestConstants.THREE_TWO_THREE);

        String prefix = RetryBucketKey.getMapKey(serviceName, "taskId");
        RetryRecordIds retryMsgIds1 = new RetryRecordIds(Version.V1_0, messageIdsSet1);
        retryBucketDao.putToMap(prefix, key123, retryMsgIds1, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        RetryRecordIds retryMsgIds2 = new RetryRecordIds(Version.V1_0, messageIdsSet2);
        retryBucketDao.putToMap(prefix, key223, retryMsgIds2, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        retryBucketDao.putToMap(prefix, key323, retryMsgIds3, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        Assert.assertNotNull(retryBucketDao.get(key323));
        retryBucketDao.deleteFromMap(prefix, key323, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
        retryBucketDao.close();
        Assert.assertNull(retryBucketDao.get(key123));
        retryBucketDao.initialize("taskId");
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_1000);
        Assert.assertEquals(retryMsgIds1.toString(), retryBucketDao.get(key123).toString());
        Assert.assertEquals(retryMsgIds2.toString(), retryBucketDao.get(key223).toString());
        Assert.assertNull(retryBucketDao.get(key323));
    }

    /**
     * Test sync with cache integration.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testSyncWithCacheIntegration() throws InterruptedException {
        IgniteStringKey igniteKey = new IgniteStringKey();
        IgniteEventImpl igniteEvent = new IgniteEventImpl();
        igniteKey.setKey("abc");
        igniteEvent.setEventId("test");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(igniteEvent), Version.V1_0,
                igniteEvent, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        RetryRecord retryEvent1 = new RetryRecord(igniteKey, entity, TestConstants.THREAD_SLEEP_TIME_2000);
        RetryRecord retryEvent2 = new RetryRecord(igniteKey, entity, TestConstants.THREAD_SLEEP_TIME_1000);
        RetryRecord retryEvent3 = new RetryRecord(igniteKey, entity, TestConstants.THREAD_SLEEP_TIME_3000);
        RetryRecordKey abc = new RetryRecordKey("abc", taskId);
        RetryRecordKey bcd = new RetryRecordKey("bcd", taskId);
        RetryRecordKey efg = new RetryRecordKey("efg", taskId);
        retryRecordDao.setServiceName(serviceName);
        retryRecordDao.initialize(taskId);

        String prefix = RetryRecordKey.getMapKey(serviceName, taskId);
        retryRecordDao.putToMap(prefix, abc, retryEvent1, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        retryRecordDao.putToMap(prefix, bcd, retryEvent2, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        retryRecordDao.putToMap(prefix, efg, retryEvent3, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        Assert.assertNotNull(retryRecordDao.get(efg));
        retryRecordDao.deleteFromMap(prefix, efg, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        retryRecordDao.close();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_1000);
        Assert.assertNull(retryRecordDao.get(abc));
        retryRecordDao.initialize(taskId);

        Assert.assertEquals(retryEvent1.getIgniteKey().getKey(),
                retryRecordDao
                        .get(abc)
                        .getIgniteKey().getKey());
        Assert.assertEquals(retryEvent2.getIgniteKey().getKey(),
                retryRecordDao
                        .get(bcd)
                        .getIgniteKey().getKey());

        Assert.assertEquals(retryEvent1.getDeviceMessage().getDeviceMessageHeader().toString(),
                retryRecordDao
                        .get(abc)
                        .getDeviceMessage().getDeviceMessageHeader().toString());
        Assert.assertEquals(retryEvent2.getDeviceMessage().getDeviceMessageHeader().toString(),
                retryRecordDao
                        .get(bcd)
                        .getDeviceMessage().getDeviceMessageHeader().toString());

        Assert.assertNull(retryRecordDao
                .get(efg));
    }

}