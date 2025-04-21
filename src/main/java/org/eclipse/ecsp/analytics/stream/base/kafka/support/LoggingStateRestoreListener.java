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

package org.eclipse.ecsp.analytics.stream.base.kafka.support;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

/**
 * Logs the progress of state store restoration.
 *
 * @author ssasidharan
 */
public class LoggingStateRestoreListener implements StateRestoreListener {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(LoggingStateRestoreListener.class);

    /**
     * On restore start.
     *
     * @param topicPartition the topic partition
     * @param storeName the store name
     * @param startingOffset the starting offset
     * @param endingOffset the ending offset
     */
    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName,
            long startingOffset, long endingOffset) {
        logger.info("State store restoration for store named {} with topic {} and partition {} has started. "
                 + "Total records: {}", storeName, topicPartition.topic(), topicPartition.partition(), 
                 (endingOffset - startingOffset));
    }

    /**
     * On batch restored.
     *
     * @param topicPartition the topic partition
     * @param storeName the store name
     * @param batchEndOffset the batch end offset
     * @param numRestored the num restored
     */
    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName,
            long batchEndOffset, long numRestored) {
        logger.info("State store restoration progress update for store named {} with topic {} and partition {}. "
                + "Restored {}", storeName, topicPartition.topic(), topicPartition.partition(), numRestored);
    }

    /**
     * On restore end.
     *
     * @param topicPartition the topic partition
     * @param storeName the store name
     * @param totalRestored the total restored
     */
    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        logger.info("State store restoration for store named {} with topic {} and partition {} has completed. "
                + "Total records: {}", storeName, topicPartition.topic(), topicPartition.partition(), totalRestored);
    }
}
