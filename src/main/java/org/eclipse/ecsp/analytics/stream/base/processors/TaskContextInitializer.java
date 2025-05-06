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

package org.eclipse.ecsp.analytics.stream.base.processors;

import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.offset.OffsetManager;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.threadlocal.TaskContextHandler;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Properties;

/**
 * TaskContextInitializer is the first processor in the Processor chaining. The
 *
 * @author avadakkootko
 */
public class TaskContextInitializer implements StreamProcessor<byte[], byte[], byte[], byte[]> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(TaskContextInitializer.class);

    /** The spc. */
    private StreamProcessingContext<byte[], byte[]> spc;
    
    /** The handler. */
    private TaskContextHandler handler;

    /** The task id. */
    private String taskId;

    /** The offset manager. */
    @Autowired
    private OffsetManager offsetManager;

    /**
     * Inits the.
     *
     * @param spc the spc
     */
    @Override
    public void init(StreamProcessingContext<byte[], byte[]> spc) {
        this.spc = spc;
        handler = TaskContextHandler.getTaskContextHandler();
        taskId = spc.getTaskID();
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return "task-context-initializer";
    }

    /**
     * Process.
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<byte[], byte[]> kafkaRecord) {

        if (null == kafkaRecord) {
            logger.error("Input record to TaskContextInitializer cannot be null");
            return;
        }

        byte[] key = kafkaRecord.key();
        byte[] value = kafkaRecord.value();
        long currentOffset = spc.offset();
        String topic = spc.streamName();
        int partition = spc.partition();
        if (!offsetManager.doSkipOffset(topic, partition, currentOffset)) {
            if (key == null || value == null) {
                logger.error("Input Key {} or value {} cannot be null in TaskContextInitializer", key, value);
                return;
            }
            handler.resetTaskContextValue(taskId);
            logger.debug("Resetting handler for taskId {} ", taskId);
            spc.forward(kafkaRecord);
            offsetManager.updateProcessedOffset(topic, partition, currentOffset);
        }
    }

    /**
     * Punctuate.
     *
     * @param timestamp the timestamp
     */
    @Override
    public void punctuate(long timestamp) {
        //method
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        logger.debug("Attempting to shutdown offsetmanager executer");
        offsetManager.shutdown();
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        // no additional configuration to be applied
    }

    /**
     * Creates the state store.
     *
     * @return the harman persistent KV store
     */
    @SuppressWarnings("rawtypes")
    @Override
    public HarmanPersistentKVStore createStateStore() {
        return null;
    }

}
