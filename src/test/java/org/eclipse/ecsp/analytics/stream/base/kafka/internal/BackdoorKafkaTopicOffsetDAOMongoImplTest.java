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

package org.eclipse.ecsp.analytics.stream.base.kafka.internal;

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaTopicOffset;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaTopicOffsetDAOMongoImpl;
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
 * BackdoorKafkaTopicOffsetDAOMongoImplTest immplements {@link KafkaStreamsApplicationTestBase}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@TestPropertySource("/backdoor-dao-test.properties")
public class BackdoorKafkaTopicOffsetDAOMongoImplTest extends KafkaStreamsApplicationTestBase {
    
    /** The kafka topic. */
    private String kafkaTopic = "kafkaTopic";
    
    /** The service. */
    private String service = "service";
    
    /** The partition. */
    private int partition = 1;
    
    /** The offset. */
    private long offset = 1000L;
    
    /** The topic offset. */
    private BackdoorKafkaTopicOffset topicOffset;

    /** The backdoor dao. */
    @Autowired
    private BackdoorKafkaTopicOffsetDAOMongoImpl backdoorDao;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        topicOffset = new BackdoorKafkaTopicOffset(kafkaTopic, partition, offset);
    }

    /**
     * Test get backdoor kafka topic offset list.
     */
    @Test
    public void testGetBackdoorKafkaTopicOffsetList() {
        backdoorDao.save(topicOffset);
        List<BackdoorKafkaTopicOffset> list = backdoorDao.getTopicOffsetList(kafkaTopic);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(topicOffset, list.get(0));
    }

    /**
     * Test save.
     */
    @Test
    public void testSave() {
        backdoorDao.save(topicOffset);
        Assert.assertEquals(topicOffset, backdoorDao.findById(topicOffset.getId()));

        long offsetTmp = TestConstants.THREAD_SLEEP_TIME_2000;
        topicOffset.setOffset(offsetTmp);
        backdoorDao.save(topicOffset);
        Assert.assertEquals(topicOffset, backdoorDao.findById(topicOffset.getId()));
        Assert.assertEquals(topicOffset.getOffset(), offsetTmp);
    }

}
