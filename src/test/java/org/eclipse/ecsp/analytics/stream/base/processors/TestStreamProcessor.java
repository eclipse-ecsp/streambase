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
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Properties;



/**
 * class {@link TestStreamProcessor} implements {@link IgniteEventStreamProcessor}.
 */
@Component
public class TestStreamProcessor implements IgniteEventStreamProcessor {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(TestStreamProcessor.class);
    
    /** The ctxt. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctxt;

    /** The source topics. */
    @Value("${source.topic.name}")
    private String[] sourceTopics;

    /** The sink topics. */
    @Value("${sink.topic.name}")
    private String[] sinkTopics;

    /**
     * Inits the.
     *
     * @param spc the spc
     */
    @Override
    public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        this.ctxt = spc;

    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return "test-stream-processor";
    }

    /**
     * Sources.
     *
     * @return the string[]
     */
    @Override
    public String[] sources() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("source topics {}", Arrays.toString(sourceTopics));
        }
        return sourceTopics;
    }

    /**
     * Sinks.
     *
     * @return the string[]
     */
    @Override
    public String[] sinks() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sink topics {}", Arrays.toString(sinkTopics));
        }
        return sinkTopics;
    }

    /**
     * Process.
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {

        IgniteKey<?> key = kafkaRecord.key();
        IgniteEvent value = kafkaRecord.value();

        if (null == key) {
            LOGGER.error("Key is null, no further processing.");
        }

        if (null == value) {
            LOGGER.error("Value is null, no further processing.");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Key={},Value={}", key, value);
        }

        // do processing and then forward the event.
        //Give the topic name. In this streaam processsor we have only one topic
        this.ctxt.forward(kafkaRecord, sinkTopics[0]);

    }

    /**
     * Punctuate.
     *
     * @param timestamp the timestamp
     */
    @Override
    public void punctuate(long timestamp) {
        // Nothing to do.
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        // Nothing to do.
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        // Nothing to do.
    }

    /**
     * Inits the config.
     *
     * @param props the props
     */
    @Override
    public void initConfig(Properties props) {
        String sourceTopicNames = (String) props.get("source.topic.name");
        sourceTopics = sourceTopicNames.split(",");
        LOGGER.info("source topic list {}", sourceTopicNames);

        String sinkTopicNames = (String) props.get("sink.topic.name");
        sinkTopics = sinkTopicNames.split(",");
        LOGGER.info("sink topic list {}", sinkTopicNames);

    }

    /**
     * Creates the state store.
     *
     * @return the harman persistent KV store
     */
    @Override
    public HarmanPersistentKVStore createStateStore() {
        return null;
    }

}
