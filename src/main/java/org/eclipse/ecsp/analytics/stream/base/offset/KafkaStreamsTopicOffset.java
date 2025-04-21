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

import dev.morphia.annotations.Entity;

/**
 * class KafkaStreamsTopicOffset extends TopicOffset.
 */
@Entity()
public class KafkaStreamsTopicOffset extends TopicOffset {
    
    /**
     * Instantiates a new kafka streams topic offset.
     */
    public KafkaStreamsTopicOffset() {
        super();
    }

    /**
     * Instantiates a new kafka streams topic offset.
     *
     * @param kafkaTopic the kafka topic
     * @param partition the partition
     * @param offset the offset
     */
    public KafkaStreamsTopicOffset(String kafkaTopic, int partition, long offset) {
        super(kafkaTopic, partition, offset);
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "KafkaStreamsTopicOffset [getId()=" + getId() + ", getKafkaTopic()="
                + getKafkaTopic() + ", getPartition()=" + getPartition()
                + ", getOffset()=" + getOffset() + "]";
    }

}
