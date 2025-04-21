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

package org.eclipse.ecsp.analytics.stream.base;

import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;

/**
 * Contract for stream based processing.
 */
public interface StreamProcessingContext<K, V> {

    /**
     * The current topic being read from.
     *
     * @return String
     */
    public String streamName();

    /**
     * The partition for the current message being processed.
     *
     * @return int
     */
    public int partition();

    /**
     * The offset for the current message being processed.
     *
     * @return long
     */
    public long offset();

    /**
     * Enforce a checkpoint of the processing. Once this is called messages till the current
     * offset will never be available again unless replayed.
     */
    public void checkpoint();

    @SuppressWarnings("rawtypes")
    public KeyValueStore getStateStore(String name);

    /**
     * Forward to the next processor/sink in the chain.
     *
     * @param kafkaRecord kafkaRecord
     */
    public void forward(Record<K, V> kafkaRecord);

    /**
     * Forward to the named processor/sink in the chain.
     *
     * @param kafkaRecord kafkaRecord
     * @param name name
     */
    public void forward(Record<K, V> kafkaRecord, String name);



    /**
     * Return the taskID per stream thread. It would be generally topicGroupID_partitionID.
     * Useful when like to know the which task is processing which partition
     *
     * @return String
     */
    public String getTaskID();

    /**
     * Return the MetricRegistry.
     */
    public MetricRegistry getMetricRegistry();

    /**
     * Forwards to a sink stream directly without going through the rest of the framework.
     *
     * @param key key
     * @param value value
     * @param topic topic
     */
    public default void forwardDirectly(@SuppressWarnings("rawtypes") IgniteKey key, IgniteEvent value, String topic) {
    }

    /**
     * Forwards to a sink stream directly without going through the rest of the framework.
     *
     * @param key key
     * @param value value
     * @param topic topic
     */
    public void forwardDirectly(String key, String value, String topic);

    /**
     * schedule().
     *
     * @param interval interval
     * @param punctuationType punctuationType
     * @param punctuator punctuator
     */
    public void schedule(long interval, PunctuationType punctuationType, Punctuator punctuator);

}
