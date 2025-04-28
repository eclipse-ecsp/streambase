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

package org.eclipse.ecsp.stream.dma.handler;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaDispatcher;
import org.eclipse.ecsp.analytics.stream.base.utils.MqttDispatcher;
import org.eclipse.ecsp.analytics.stream.base.utils.ObjectUtils;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageDispatchers;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Map;


/**
 * <p>
 * DispatchHandler is responsible for the following.
 *
 * -> Check if TTL has exceeded.
 *
 * -> If TTL has not exceeded Add Header (messageId/correlationId)
 *
 * -> Transform IgniteEvent and IgniteKey to byte[]
 *
 * -> Dispatch to MQTT.
 *
 * </p>
 *
 * @author avadakkootko
 */
@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DispatchHandler implements DeviceMessageHandler {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DispatchHandler.class);
    
    /** The mqtt dispatcher. */
    @Autowired
    private MqttDispatcher mqttDispatcher;

    // RDNG: 170507, RTC: 433337. 
    /**
     * DMA should have the capability to dispatch data to other plaforms/brokers along with HiveMQ.
     * Check {@link #handle(IgniteKey, DeviceMessage) for more}
     */
    @Autowired
    private KafkaDispatcher kafkaDispatcher;
    
    /** The broker to ecu types mapping. */
    private Map<String, Map<String, String>> brokerToEcuTypesMapping = null;        
    
    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;
    
    /**
     * <P>
     * ECU type from each IgniteEvent / DeviceMessage will be extracted and its
     * presence will be checked in the map against each broker.
     * If it's present and is mapped to a topic, only then the DeviceMessage will
     * be dispatched to the topic of that respective
     * broker/platform.
     *
     * Else, dispatch to MQTT.(The default behavior)
     *
     * </P>
     *
     * @param key IgniteKey
     * @param value DeviceMessage
     */
    @Override
    public void handle(IgniteKey<?> key, DeviceMessage value) {
        if (brokerToEcuTypesMapping != null && brokerToEcuTypesMapping.size() > 0) {
            for (Map.Entry<String, Map<String, String>> entry : brokerToEcuTypesMapping.entrySet()) {
                String broker = entry.getKey();
                Map<String, String> ecuTypeToTopicMapping = entry.getValue();
                if (DeviceMessageDispatchers.KAFKA.equals(broker)) {
                    String ecuType = value.getEvent().getEcuType().toLowerCase();
                    if (ecuTypeToTopicMapping.containsKey(ecuType) 
                            && StringUtils.isNotEmpty(ecuTypeToTopicMapping.get(ecuType))) {
                        logger.info("Forwarding the DeviceMessage with key: {} and value: {} to "
                                + "KafkaDispatcher", key, value);
                        kafkaDispatcher.dispatch(key, value);
                    } else {
                        logger.info("No topic found for the ecuType: {} for broker: {} The DeviceMessage "
                                + "for this ecuType will be dispatched to MQTT by default", ecuType, broker);
                        mqttDispatcher.dispatch(key, value);
                    }
                } else {
                    logger.error("Unknown dispatcher. Won't dispatch the message.");
                }
            }
        } else {
            logger.info("Forwarding the DeviceMessage with key: {} and value: {} to MqttDispatcher for "
                    + "dispatch", key, value);
            mqttDispatcher.dispatch(key, value);
        }
    }

    /**
     * Validate.
     */
    @PostConstruct
    public void validate() {
        ObjectUtils.requireNonNull(mqttDispatcher, "Uninitialized MQTT dispatcher.");
        ObjectUtils.requireNonNull(kafkaDispatcher, "Uninitialized Kafka dispatcher.");
    }

    /**
     * Sets the next handler.
     *
     * @param handler the new next handler
     */
    @Override
    public void setNextHandler(DeviceMessageHandler handler) {
        mqttDispatcher.setNextHandler(handler);
        kafkaDispatcher.setNextHandler(handler);
    }

    /**
     * setup().
     *
     * @param taskId taskId
     * @param brokerToEcuTypesMapping brokerToEcuTypesMapping
     */
    public void setup(String taskId, Map<String, Map<String, String>> brokerToEcuTypesMapping) {
        mqttDispatcher.setup(taskId);
        this.brokerToEcuTypesMapping = brokerToEcuTypesMapping;
        kafkaDispatcher.setup(brokerToEcuTypesMapping, this.spc);
    }

    /**
     * Sets the stream processing context.
     *
     * @param ctx the ctx
     */
    @Override
    public void setStreamProcessingContext(StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctx) {
        this.spc = ctx;
        mqttDispatcher.setStreamProcessingContext(ctx);
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        mqttDispatcher.close();
    }

}
