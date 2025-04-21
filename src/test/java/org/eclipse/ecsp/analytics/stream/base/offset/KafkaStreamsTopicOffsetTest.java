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

import org.eclipse.ecsp.analytics.stream.base.offset.KafkaStreamsTopicOffset;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * test class KafkaStreamsTopicOffsetTest.
 */
public class KafkaStreamsTopicOffsetTest {
    
    /** The kafka topic. */
    private String kafkaTopic = "kafkaTopic";

    /** The partition. */
    private int partition = 1;
    
    /** The offset. */
    private long offset = 1000L;
    
    /** The topic offset. */
    private KafkaStreamsTopicOffset topicOffset;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        topicOffset = new KafkaStreamsTopicOffset(kafkaTopic, partition, offset);
    }

    /**
     * Test kafka streams topic offset.
     */
    @Test
    public void testKafkaStreamsTopicOffset() {
        String id = new StringBuilder().append(kafkaTopic).append(":")
                .append(partition).toString();
        Assert.assertEquals(id, topicOffset.getId());
        Assert.assertEquals(kafkaTopic, topicOffset.getKafkaTopic());
        Assert.assertEquals(partition, topicOffset.getPartition());
        Assert.assertEquals(offset, topicOffset.getOffset());
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
        topicOffset.setPartition(Constants.FIFTY);
        Assert.assertEquals(Constants.FIFTY, topicOffset.getPartition());
    }

    /**
     * Test set offset.
     */
    @Test
    public void testSetOffset() {
        topicOffset.setOffset(Constants.THREAD_SLEEP_TIME_5000);
        Assert.assertEquals(Constants.THREAD_SLEEP_TIME_5000, topicOffset.getOffset());
    }

    /**
     * Test to string.
     */
    @Test
    public void testToString() {
        String id = new StringBuilder().append(kafkaTopic).append(":")
                .append(partition).toString();
        String toStr = "KafkaStreamsTopicOffset [getId()=" + id
                + ", getKafkaTopic()=" + kafkaTopic + ", getPartition()="
                + partition + ", getOffset()=" + offset + "]";
        Assert.assertEquals(toStr, topicOffset.toString());
    }

    /**
     * Test equals.
     */
    @Test
    public void testEquals() {
        KafkaStreamsTopicOffset newTopicOffset = new KafkaStreamsTopicOffset(kafkaTopic, partition, offset);
        Assert.assertEquals(newTopicOffset, topicOffset);
        newTopicOffset = new KafkaStreamsTopicOffset(kafkaTopic, partition, Constants.THREAD_SLEEP_TIME_5000);
        Assert.assertNotEquals(newTopicOffset, topicOffset);
    }

    /**
     * Test hash code.
     */
    @Test
    public void testHashCode() {
        KafkaStreamsTopicOffset newTopicOffset = new KafkaStreamsTopicOffset(kafkaTopic, partition, offset);
        Assert.assertEquals(newTopicOffset.hashCode(), topicOffset.hashCode());
        newTopicOffset = new KafkaStreamsTopicOffset(kafkaTopic, partition, Constants.THREAD_SLEEP_TIME_5000);
        Assert.assertNotEquals(newTopicOffset.hashCode(), topicOffset.hashCode());
    }

}
