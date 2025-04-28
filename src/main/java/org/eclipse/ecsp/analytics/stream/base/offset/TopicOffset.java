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
import dev.morphia.annotations.Id;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.entities.AbstractIgniteEntity;

/**
 * TopicOffset serves as the base data structure for topic to partition to offset mapping.
 * This is used by BackdoorOffsetManager and
 * KafksStreamsOffset manager to store offsets per topic per partition.
 *
 * @author avadakkootko
 */
@Entity
public class TopicOffset extends AbstractIgniteEntity {

    /** The Constant COLON. */
    private static final String COLON = ":";
    
    /** The id. */
    @Id
    private String id;
    
    /** The kafka topic. */
    private String kafkaTopic;
    
    /** The partition. */
    private int partition;
    
    /** The offset. */
    private long offset;

    /**
     * Instantiates a new topic offset.
     */
    public TopicOffset() {

    }

    /**
     * Constructor to generate TopicOffset.
     *
     * @param kafkaTopic topic name
     * @param partition partition name
     * @param offset offset value
     **/
    public TopicOffset(String kafkaTopic, int partition, long offset) {
        super();
        this.id = new StringBuilder().append(kafkaTopic).append(COLON)
                .append(partition).toString();
        this.kafkaTopic = kafkaTopic;
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the id.
     *
     * @param id the new id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the kafka topic.
     *
     * @return the kafka topic
     */
    public String getKafkaTopic() {
        return kafkaTopic;
    }

    /**
     * Sets the kafka topic.
     *
     * @param kafkaTopic the new kafka topic
     */
    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    /**
     * Gets the partition.
     *
     * @return the partition
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Sets the partition.
     *
     * @param partition the new partition
     */
    public void setPartition(int partition) {
        this.partition = partition;
    }

    /**
     * Gets the offset.
     *
     * @return the offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Sets the offset.
     *
     * @param offset the new offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "TopicOffset [id=" + id + ", kafkaTopic=" + kafkaTopic
                + ", partition=" + partition + ", offset=" + offset + "]";
    }

    /**
     * Hash code.
     *
     * @return the int
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((kafkaTopic == null) ? 0 : kafkaTopic.hashCode());
        result = prime * result + (int) (offset ^ (offset >>> Constants.OFFSET_VALUE));
        result = prime * result + partition;
        return result;
    }

    /**
     * Equals.
     *
     * @param obj the obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TopicOffset other = (TopicOffset) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (kafkaTopic == null) {
            if (other.kafkaTopic != null) {
                return false;
            }
        } else if (!kafkaTopic.equals(other.kafkaTopic)) {
            return false;
        }
        if (offset != other.offset) {
            return false;
        }
        if (partition != other.partition) {
            return false;
        }
        return partition == other.partition;
    }

}
