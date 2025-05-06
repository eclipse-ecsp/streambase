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

import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.dao.GenericDAO;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;

import java.util.Properties;

/**
 * Contract for all event processors.
 *
 * @author ssasidharan
 */
public interface StreamProcessor<KIn, VIn, KOut, VOut> {

    /**
     * Initialize the processor
     * Use spc.schedule() to schedule punctuations.
     *
     * @param spc spc
     */
    void init(StreamProcessingContext<KOut, VOut> spc);

    /**
     * Name by which this processor is known to other processors.
     *
     * @return String
     */
    String name();

    /**
     * Process an event.
     *
     * @param kafkaRecord kafkaRecord
     */
    void process(Record<KIn, VIn> kafkaRecord);

    /**
     * Perform actions on a schedule that is dictated by arrival of events.
     *
     * @param timestamp timestamp
     */
    void punctuate(long timestamp);

    /**
     * Perform actions on a schedule dictated by wall clock time.
     * One tick is roughly one second. Note that implementations of this method
     * should use the StreamsProcessorContext.forwardDirectly() instead of the forward() methods.
     */
    default void punctuateWc(long ticks) {
    }

    /**
     * Cleanup. Note that outputting data at this stage is not possible
     */
    void close();

    /**
     * Optional provision to define source topics this processor can handle. Typical implementations should ignore this.
     *
     * @return String[]
     */
    default String[] sources() {
        return new String[0];
    }

    /**
     * Perform any restructuring based on new configuration.
     *
     * @param props props
     */
    void configChanged(Properties props);

    @SuppressWarnings("rawtypes")
    HarmanPersistentKVStore createStateStore();

    /**
     * Defines sink topics this processor writes to. Typically the final processor in the chain will define this.
     *
     * @return String[]
     */
    default String[] sinks() {
        return new String[0];
    }

    /**
     * The initial config to use. This is the same as the config used from the command
     * line to launch the streams application. Note that
     * this method is called before any other methods are called on your
     * stream processor and also note that your stream processor is not
     * yet registered with the streams framework when this is called.
     *
     * @param props props
     */
    default void initConfig(Properties props) {
    }

    /**
     * Called only when event arrived in config topic or master data topic.
     * Respective implementation needs to update respective changes in
     * its state-store.
     * Since we already had readAndPopulateSharedData(GenericDAO dao) which is
     * reading master/config data from the data source. But this
     * method (i.e. updateSharedData) is also required because we want changed/new
     * property or master data needs to be read as and when
     * needed.
     *
     */
    default void updateSharedData(Object key, Object value, String streamName) {
    }
    
    /**
     * Setter for external data source.
     *
     * @param dao The DAO for the data source.
     */
    default void setExternalSharedDataSource(GenericDAO dao) {

    }

}
