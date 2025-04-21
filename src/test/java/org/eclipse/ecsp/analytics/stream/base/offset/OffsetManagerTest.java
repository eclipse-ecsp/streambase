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

import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.offset.KafkaStreamsOffsetManagementDAOMongoImpl;
import org.eclipse.ecsp.analytics.stream.base.offset.KafkaStreamsTopicOffset;
import org.eclipse.ecsp.analytics.stream.base.offset.OffsetManager;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;



/**
 * Test Class for {@link OffsetManager}.
 */
public class OffsetManagerTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The offset manager. */
    @InjectMocks
    private OffsetManager offsetManager = new OffsetManager();

    /** The topic offset dao. */
    @Mock
    private KafkaStreamsOffsetManagementDAOMongoImpl topicOffsetDao;
    
    /** The topic. */
    private String topic = "topic";
    
    /** The partition. */
    private int partition = 1;
    
    /** The offset. */
    private long offset = 1000L;

    /**
     * Sets the up.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test update processed offset if flag not enabled.
     */
    @Test
    public void testUpdateProcessedOffsetIfFlagNotEnabled() {
        offsetManager.setOffsetPersistenceEnabled(false);
        ConcurrentHashMap<String, KafkaStreamsTopicOffset> persistOffsetMap =
                (ConcurrentHashMap<String, KafkaStreamsTopicOffset>) Mockito
                .mock(ConcurrentHashMap.class);
        offsetManager.setPersistOffsetMap(persistOffsetMap);
        offsetManager.updateProcessedOffset("topic", partition, offset);
        Mockito.verify(persistOffsetMap, Mockito.times(0)).get(Mockito.anyString());
    }

    /**
     * Test update processed offset.
     */
    @Test
    public void testUpdateProcessedOffset() {
        offsetManager.setOffsetPersistenceEnabled(true);
        ConcurrentHashMap<String, KafkaStreamsTopicOffset> persistOffsetMap =
                (ConcurrentHashMap<String, KafkaStreamsTopicOffset>) Mockito
                .mock(ConcurrentHashMap.class);
        offsetManager.setPersistOffsetMap(persistOffsetMap);

        String key = offsetManager.getKey(topic, partition);

        KafkaStreamsTopicOffset topicOffset = new KafkaStreamsTopicOffset(topic, partition, offset);
        Mockito.when(persistOffsetMap.get(key))
                .thenReturn(topicOffset);
        offsetManager.updateProcessedOffset(topic, partition, offset);

        Mockito.verify(persistOffsetMap, Mockito.times(1)).get(key);
    }

    /**
     * Test is skip offset if flag not enabled.
     */
    @Test
    public void testIsSkipOffsetIfFlagNotEnabled() {
        offsetManager.setOffsetPersistenceEnabled(false);
        ConcurrentHashMap<String, KafkaStreamsTopicOffset> persistOffsetMap =
                (ConcurrentHashMap<String, KafkaStreamsTopicOffset>) Mockito
                .mock(ConcurrentHashMap.class);
        ConcurrentHashMap<String, KafkaStreamsTopicOffset> refrenceMap =
                (ConcurrentHashMap<String, KafkaStreamsTopicOffset>) Mockito
                .mock(ConcurrentHashMap.class);
        offsetManager.setPersistOffsetMap(persistOffsetMap);
        offsetManager.setRefrenceMap(refrenceMap);
        Assert.assertFalse(offsetManager.doSkipOffset("topic", 1, TestConstants.THREAD_SLEEP_TIME_1000));
        Mockito.verify(persistOffsetMap, Mockito.times(0)).get(Mockito.anyString());
        Mockito.verify(refrenceMap, Mockito.times(0)).get(Mockito.anyString());
    }

    /**
     * Test is skip offset.
     */
    @Test
    public void testIsSkipOffset() {
        offsetManager.setOffsetPersistenceEnabled(true);
        ConcurrentHashMap<String, KafkaStreamsTopicOffset> persistOffsetMap =
                (ConcurrentHashMap<String, KafkaStreamsTopicOffset>) Mockito
                .mock(ConcurrentHashMap.class);
        ConcurrentHashMap<String, KafkaStreamsTopicOffset> refrenceMap =
                (ConcurrentHashMap<String, KafkaStreamsTopicOffset>) Mockito
                .mock(ConcurrentHashMap.class);
        offsetManager.setPersistOffsetMap(persistOffsetMap);
        offsetManager.setRefrenceMap(refrenceMap);

        // Case 1 : When currentmap and refmap are null
        Assert.assertFalse(offsetManager.doSkipOffset("topic", 1, TestConstants.THREAD_SLEEP_TIME_1000));

        String key = offsetManager.getKey("topic", 1);
        Mockito.verify(persistOffsetMap, Mockito.times(1)).get(key);
        Mockito.verify(refrenceMap, Mockito.times(1)).get(key);

        // Case 2 : When currentmap and refmap not null
        KafkaStreamsTopicOffset topicOffset = new KafkaStreamsTopicOffset("topic",
                1, TestConstants.THREAD_SLEEP_TIME_900);
        Mockito.when(persistOffsetMap.get(key))
                .thenReturn(topicOffset);
        Mockito.when(refrenceMap.get(key))
                .thenReturn(topicOffset);
        // skip smaller offset than processed
        Assert.assertTrue(offsetManager.doSkipOffset("topic", 1, TestConstants.THREAD_SLEEP_TIME_900));
        // Do not skip larger offset than processed
        Assert.assertFalse(offsetManager.doSkipOffset("topic", 1, TestConstants.THREAD_SLEEP_TIME_1100));

        // Case 3 : When currentmap is null ; and refmap not null
        Mockito.when(persistOffsetMap.get(key))
                .thenReturn(null);
        Mockito.when(refrenceMap.get(key))
                .thenReturn(topicOffset);
        // skip smaller offset than processed
        Assert.assertTrue(offsetManager.doSkipOffset("topic", 1, TestConstants.THREAD_SLEEP_TIME_900));
        // Do not skip larger offset than processed
        Assert.assertFalse(offsetManager.doSkipOffset("topic", 1, TestConstants.THREAD_SLEEP_TIME_1100));
    }

    /**
     * Test initialize refrence map.
     */
    @Test
    public void testInitializeRefrenceMap() {
        ConcurrentHashMap<String, KafkaStreamsTopicOffset> refrenceMap =
                (ConcurrentHashMap<String, KafkaStreamsTopicOffset>) Mockito
                .mock(ConcurrentHashMap.class);
        offsetManager.setRefrenceMap(refrenceMap);

        List<KafkaStreamsTopicOffset> topicOffsetList = new ArrayList<KafkaStreamsTopicOffset>();
        KafkaStreamsTopicOffset topicOffset1 = new KafkaStreamsTopicOffset(topic, partition, offset);
        KafkaStreamsTopicOffset topicOffset2 = new KafkaStreamsTopicOffset(topic, Constants.FIVE, offset);
        topicOffsetList.add(topicOffset1);
        topicOffsetList.add(topicOffset2);

        Mockito.when(topicOffsetDao.findAll())
                .thenReturn(topicOffsetList);
        offsetManager.initializeRefrenceMap();
        Mockito.verify(refrenceMap, Mockito.times(Constants.TWO)).put(Mockito.anyString(), Mockito.any());
    }

    /**
     * Test get key.
     */
    @Test
    public void testGetKey() {
        String topic = "topic";
        int partition = Constants.THREAD_SLEEP_TIME_10;
        String key = topic + ":" + partition;
        Assert.assertEquals(key, offsetManager.getKey(topic, partition));
    }

    /**
     * Test persist offset.
     */
    @Test
    public void testPersistOffset() {
        ConcurrentHashMap<String, KafkaStreamsTopicOffset> persistOffsetMap =
                (ConcurrentHashMap<String, KafkaStreamsTopicOffset>) Mockito
                .mock(ConcurrentHashMap.class);
        KafkaStreamsTopicOffset topicOffset1 = new KafkaStreamsTopicOffset(topic, partition, offset);
        offsetManager.setPersistOffsetMap(persistOffsetMap);

        // Case 1 check when map is null
        offsetManager.persistOffset();
        Mockito.verify(topicOffsetDao, Mockito.times(0)).save(topicOffset1);
        KafkaStreamsTopicOffset topicOffset2 = new KafkaStreamsTopicOffset(topic, Constants.FIVE, offset);
        Mockito.verify(topicOffsetDao, Mockito.times(0)).save(topicOffset2);

        // Case 2 check when map is not null
        List<KafkaStreamsTopicOffset> list = new ArrayList<KafkaStreamsTopicOffset>();
        list.add(topicOffset1);
        list.add(topicOffset2);

        Mockito.when(persistOffsetMap.values())
                .thenReturn(list);

        offsetManager.persistOffset();
        Mockito.verify(topicOffsetDao, Mockito.times(1)).save(topicOffset1);
        Mockito.verify(topicOffsetDao, Mockito.times(1)).save(topicOffset2);
    }

}
