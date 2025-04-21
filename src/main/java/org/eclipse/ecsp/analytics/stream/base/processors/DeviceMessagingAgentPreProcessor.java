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

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessorFilter;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidKeyOrValueException;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.ObjectUtils;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMARetryRecordDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.key.RetryRecordKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

/**
 * DeviceMessagingAgentPreProcessor is responsible to removing events
 * from retry map when an acknowledgement is received.
 *
 * @author avadakkootko
 */
@Service
public class DeviceMessagingAgentPreProcessor implements IgniteEventStreamProcessor, StreamProcessorFilter {

    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceMessagingAgentPreProcessor.class);

    /** The retry event dao. */
    @Autowired
    private DMARetryRecordDAOCacheBackedInMemoryImpl retryEventDao;
    
    /** The map key. */
    private String mapKey;
    
    /** The task id. */
    private String taskId;

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /**
     * Inits the.
     */
    @PostConstruct
    public void init() {
        ObjectUtils.requireNonEmpty(serviceName, "Service Name cannot be empty in DMA PreProcessor");
    }

    /**
     * Inits the.
     *
     * @param spc the spc
     */
    @Override
    public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        this.spc = spc;
        this.taskId = spc.getTaskID();
        this.mapKey = RetryRecordKey.getMapKey(serviceName, this.taskId);
        logger.info("Initialized DeviceMessagingAgentPreProcessor stream processor and mapKey is {}", mapKey);
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return "DeviceMessagingAgentPreProcessor";
    }

    /**
     * Check if correlationId is present in retry map. If yes delete it.
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
        if (null == kafkaRecord) {
            logger.error("Input record to DeviceMessagingAgentPreProcessor cannot be null");
            throw new IllegalArgumentException("Input record to DeviceMessagingAgentPreProcessor cannot be null");
        }
        IgniteEvent value = kafkaRecord.value();
        IgniteKey<?> key = kafkaRecord.key();
        if (key == null) {
            throw new InvalidKeyOrValueException("IgniteKey cannot be null in input record");
        }
        logger.info(value, "DeviceMessagingAgentPreProcessor processing event with key {} ", key);
        String correlationId = value.getCorrelationId();

        if (StringUtils.isNotEmpty(correlationId)) {
            String igniteKey = (String) key.getKey();
            String retryRecordKeyPart = RetryRecordKey.createVehiclePart(igniteKey, correlationId);
            removeEventFromCache(retryRecordKeyPart);
        }
        this.spc.forward(new Record<>(key, value, System.currentTimeMillis()));
    }

    /**
     * Removes the event from cache.
     *
     * @param retryRecordKey the retry record key
     */
    private void removeEventFromCache(String retryRecordKey) {
        RetryRecordKey retryEventKey = constructKey(retryRecordKey);
        retryEventDao.deleteFromMap(mapKey, retryEventKey, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        logger.debug("Deleted retry event with key {} from in-memory map", retryEventKey.convertToString());
    }

    /**
     * Construct key.
     *
     * @param retryRecordKey the retry record key
     * @return the retry record key
     */
    private RetryRecordKey constructKey(String retryRecordKey) {
        return new RetryRecordKey(retryRecordKey, this.taskId);
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
        //
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        //overridden method
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
     * Sets the map key.
     *
     * @param mapKey the new map key
     */
    protected void setMapKey(String mapKey) {
        this.mapKey = mapKey;
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