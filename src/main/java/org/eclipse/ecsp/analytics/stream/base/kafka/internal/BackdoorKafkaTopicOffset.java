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

import dev.morphia.annotations.Entity;
import org.eclipse.ecsp.analytics.stream.base.offset.TopicOffset;

/**
 * BackdoorKafkaTopicOffset extends {@link TopicOffset}.
 */
@Entity
public class BackdoorKafkaTopicOffset extends TopicOffset {

    /**
     * Instantiates a new backdoor kafka topic offset.
     */
    public BackdoorKafkaTopicOffset() {
        super();
    }

    /**
     * Instantiates a new backdoor kafka topic offset.
     *
     * @param kafkaTopic the kafka topic
     * @param partition the partition
     * @param offset the offset
     */
    public BackdoorKafkaTopicOffset(String kafkaTopic, int partition, long offset) {
        super(kafkaTopic, partition, offset);
    }

    /**
     * Instantiates a new backdoor kafka topic offset.
     *
     * @param topicOffset the topic offset
     */
    public BackdoorKafkaTopicOffset(BackdoorKafkaTopicOffset topicOffset) {
        this(topicOffset.getKafkaTopic(), topicOffset.getPartition(), topicOffset.getOffset());
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "BackdoorKafkaTopicOffset [getId()=" + getId() + ", getKafkaTopic()=" + getKafkaTopic()
                + ", getPartition()=" + getPartition() + ", getOffset()=" + getOffset() + "]";
    }

}
