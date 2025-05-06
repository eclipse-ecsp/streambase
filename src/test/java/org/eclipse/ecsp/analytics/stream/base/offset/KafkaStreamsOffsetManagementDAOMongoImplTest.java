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

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.offset.KafkaStreamsOffsetManagementDAOMongoImpl;
import org.eclipse.ecsp.analytics.stream.base.offset.KafkaStreamsTopicOffset;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;



/**
 * class KafkaStreamsOffsetManagementDAOMongoImplTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@TestPropertySource("/offsetmanager-test.properties")
public class KafkaStreamsOffsetManagementDAOMongoImplTest extends KafkaStreamsApplicationTestBase {

    /** The kafka topic. */
    private String kafkaTopic = "kafkaTopic";
    
    /** The partition. */
    private int partition = 1;
    
    /** The offset. */
    private long offset = 1000L;
    
    /** The topic offset. */
    private KafkaStreamsTopicOffset topicOffset;

    /** The offset dao. */
    @Autowired
    private KafkaStreamsOffsetManagementDAOMongoImpl offsetDao;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        topicOffset = new KafkaStreamsTopicOffset(kafkaTopic, partition, offset);
    }

    /**
     * Test get overriding collection name.
     */
    @Test
    public void testGetOverridingCollectionName() {
        Assert.assertEquals("kafkastreamsoffsetecallspservicetest_hi", offsetDao.getOverridingCollectionName());
    }

    /**
     * Test find all.
     */
    @Test
    public void testFindAll() {
        KafkaStreamsTopicOffset topicOffset1 = new KafkaStreamsTopicOffset(kafkaTopic, partition, offset);
        offsetDao.save(topicOffset1);
        KafkaStreamsTopicOffset topicOffset2 = new KafkaStreamsTopicOffset(kafkaTopic, Constants.FIVE, offset);
        offsetDao.save(topicOffset2);
        KafkaStreamsTopicOffset topicOffset3 = new KafkaStreamsTopicOffset("topic2",
                Constants.TWO, TestConstants.THREAD_SLEEP_TIME_2000);
        offsetDao.save(topicOffset3);
        KafkaStreamsTopicOffset topicOffset4 = new KafkaStreamsTopicOffset("topic2",
                Constants.THREE, TestConstants.THREAD_SLEEP_TIME_2000);
        offsetDao.save(topicOffset4);

        List<KafkaStreamsTopicOffset> offsetList = offsetDao.findAll();
        Assert.assertEquals(Constants.FOUR, offsetList.size());
    }

    /**
     * Test save.
     */
    @Test
    public void testSave() {
        offsetDao.save(topicOffset);
        Assert.assertEquals(topicOffset, offsetDao.findById(topicOffset.getId()));

        long offsetTMP = TestConstants.THREAD_SLEEP_TIME_2000;
        topicOffset.setOffset(offsetTMP);
        offsetDao.save(topicOffset);
        Assert.assertEquals(topicOffset, offsetDao.findById(topicOffset.getId()));
        Assert.assertEquals(topicOffset.getOffset(), offsetTMP);
    }

}
