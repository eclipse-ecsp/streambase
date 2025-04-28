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
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessorFilter;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessagingHandlerChain;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Properties;

/**
 * DMA or DeviceMessagingAgent stream processor is responsible for pushing the data to the MQTT topic.
 * Event which has to be sent to MQTT topic has to implement DeviceRoutableInterface
 */
@Service
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DeviceMessagingAgentPostProcessor implements IgniteEventStreamProcessor, StreamProcessorFilter {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceMessagingAgentPostProcessor.class);

    /** The ctxt. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctxt;

    /** The dma handler. */
    @Autowired
    private DeviceMessagingHandlerChain dmaHandler;

    /** The device status back door kafka consumer. */
    @Autowired
    private DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;

    /**
     * Inits the.
     *
     * @param spc the spc
     */
    @Override
    public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        this.ctxt = spc;
        long currentTime = System.currentTimeMillis();
        String taskId = spc.getTaskID();
        dmaHandler.constructChain(taskId, spc);
        long endTime = System.currentTimeMillis();
        logger.info("Time taken to Initialize DeviceMessagingAgentPostProcessor for taskId {} is {} seconds", taskId,
                (endTime - currentTime) / Constants.THREAD_SLEEP_TIME_1000);
    }

    /**
     * Inits the config.
     *
     * @param props the props
     */
    @Override
    public void initConfig(Properties props) {
        //overridden method
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return "DeviceMessagingAgent";
    }

    /**
     * Method will be called when data (key-value) is available.
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
        dmaHandler.handle(kafkaRecord.key(), kafkaRecord.value());
        this.ctxt.forward(kafkaRecord);

    }

    /**
     * Punctuate.
     *
     * @param timestamp the timestamp
     */
    @Override
    public void punctuate(long timestamp) {
        //
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        logger.info("Closing DeviceMessagingAgentPostProcessor and shutting down DMA backdoor consumer");
        deviceStatusBackDoorKafkaConsumer.shutdown(ctxt);
        logger.info("Closing device message handlers");
        dmaHandler.close();
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        //
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

    /**
     * returns if current stream processor is enabled or not.
     *
     * @param props props
     * @return boolean
     */
    @Override
    public boolean includeInProcessorChain(Properties props) {
        return Boolean.parseBoolean(props.getProperty(PropertyNames.DMA_ENABLED));
    }
}
