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

import org.apache.kafka.common.TopicPartition;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;

/**
 * class OffsetMetadata implements MutationId.
 */
public class OffsetMetadata implements MutationId {

    /** The partition. */
    private TopicPartition partition;
    
    /** The offset. */
    private long offset;

    /**
     * OffsetMetadata().
     *
     * @param partition partition
     * @param offset offset
     */
    public OffsetMetadata(TopicPartition partition, long offset) {
        super();
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * Gets the partition.
     *
     * @return the partition
     */
    public TopicPartition getPartition() {
        return partition;
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
     * Hash code.
     *
     * @return the int
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (offset ^ (offset >>> Constants.OFFSET_VALUE));
        result = prime * result + ((partition == null) ? 0 : partition.hashCode());
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
        OffsetMetadata other = (OffsetMetadata) obj;
        if (offset != other.offset) {
            return false;
        }
        if (partition == null) {
            if (other.partition != null) {
                return false;
            }
        } else if (!partition.equals(other.partition)) {
            return false;
        }
        return true;
    }

}
