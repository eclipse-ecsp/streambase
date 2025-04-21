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

import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaTopicOffset;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;


/**
 * BackdoorKafkaTopicOffsetTest is a Test class for BackdoorKafkaTopicOffset class.
 */
public class BackdoorKafkaTopicOffsetTest {
    
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

    /**
     * Setup.
     */
    @Before
    public void setup() {
        topicOffset = new BackdoorKafkaTopicOffset(kafkaTopic, partition, offset);
    }

    /**
     * Test backdoor kafka topic offset.
     */
    @Test
    public void testBackdoorKafkaTopicOffset() {
        String id = new StringBuilder().append(kafkaTopic).append(":")
                .append(partition).toString();
        Assert.assertEquals(id, topicOffset.getId());
        Assert.assertEquals(kafkaTopic, topicOffset.getKafkaTopic());
        Assert.assertEquals(partition, topicOffset.getPartition());
        Assert.assertEquals(offset, topicOffset.getOffset());
    }

    /**
     * Test backdoor kafka topic offset copy constructor.
     */
    @Test
    public void testBackdoorKafkaTopicOffsetCopyConstructor() {
        ConcurrentHashMap<Integer, BackdoorKafkaTopicOffset> persistOffsetMap = new ConcurrentHashMap<>();
        Integer key = 1;
        BackdoorKafkaTopicOffset newTopicOffset = new BackdoorKafkaTopicOffset(kafkaTopic, partition, offset);
        persistOffsetMap.put(key, newTopicOffset);
        BackdoorKafkaTopicOffset newTopicOffsetCopy = new BackdoorKafkaTopicOffset(newTopicOffset);
        newTopicOffset.setOffset(TestConstants.THREAD_SLEEP_TIME_2000);

        Assert.assertNotEquals(persistOffsetMap.get(key).getOffset(), newTopicOffsetCopy.getOffset());
        Assert.assertNotEquals(newTopicOffset.getOffset(), newTopicOffsetCopy.getOffset());

        Assert.assertEquals(persistOffsetMap.get(key).getOffset(), newTopicOffset.getOffset());
        BackdoorKafkaTopicOffset newTopicOffsetDuplicate = newTopicOffset;
        Assert.assertEquals(newTopicOffsetDuplicate.getOffset(), newTopicOffset.getOffset());
    }

    /**
     * Test set id.
     */
    @Test
    public void testSetId() {
        topicOffset.setId("id");
        Assert.assertEquals("id", topicOffset.getId());
    }

    /**
     * Test set kafka topic.
     */
    @Test
    public void testSetKafkaTopic() {
        topicOffset.setKafkaTopic("newTopic");
        Assert.assertEquals("newTopic", topicOffset.getKafkaTopic());
    }

    /**
     * Test set partition.
     */
    @Test
    public void testSetPartition() {
        topicOffset.setPartition(TestConstants.THREAD_SLEEP_TIME_50);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_50, topicOffset.getPartition());
    }

    /**
     * Test set offset.
     */
    @Test
    public void testSetOffset() {
        topicOffset.setOffset(TestConstants.THREAD_SLEEP_TIME_5000);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_5000, topicOffset.getOffset());
    }

    /**
     * Test to string.
     */
    @Test
    public void testToString() {
        String id = new StringBuilder().append(kafkaTopic).append(":")
                .append(partition).toString();
        String toStr = "BackdoorKafkaTopicOffset [getId()=" + id
                + ", getKafkaTopic()=" + kafkaTopic + ", getPartition()="
                + partition + ", getOffset()=" + offset + "]";
        Assert.assertEquals(toStr, topicOffset.toString());
    }

    /**
     * Test equals.
     */
    @Test
    public void testEquals() {
        BackdoorKafkaTopicOffset newTopicOffset = new BackdoorKafkaTopicOffset(kafkaTopic, partition, offset);
        Assert.assertEquals(newTopicOffset, topicOffset);
        newTopicOffset = new BackdoorKafkaTopicOffset(kafkaTopic, partition, TestConstants.THREAD_SLEEP_TIME_5000);
        Assert.assertNotEquals(newTopicOffset, topicOffset);
    }

    /**
     * Test hash code.
     */
    @Test
    public void testHashCode() {
        BackdoorKafkaTopicOffset newTopicOffset = new BackdoorKafkaTopicOffset(kafkaTopic, partition, offset);
        Assert.assertEquals(newTopicOffset.hashCode(), topicOffset.hashCode());
        newTopicOffset = new BackdoorKafkaTopicOffset(kafkaTopic, partition, TestConstants.THREAD_SLEEP_TIME_5000);
        Assert.assertNotEquals(newTopicOffset.hashCode(), topicOffset.hashCode());
    }

}
