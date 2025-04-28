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

package org.eclipse.ecsp.analytics.stream.base.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.ecsp.analytics.stream.base.KafkaProducerInstance;
import org.eclipse.ecsp.analytics.stream.base.KafkaSslConfig;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageDispatchers;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageHandler;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageUtils;
import org.eclipse.ecsp.transform.IgniteKeyTransformer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * RDNG: 170507, RTC: 433337. DMA should have the capability to dispatch data to Kafka.
 * This dispatcher class will dispatch the DeviceMessage to the provided kafka topic for an ecuType.
 *
 */

@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class KafkaDispatcher implements Dispatcher<IgniteKey<?>, DeviceMessage> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaDispatcher.class);
    
    /** The dma post dispatch handler. */
    private DeviceMessageHandler dmaPostDispatchHandler;
    
    /** The broker to ecu types mapping. */
    private Map<String, Map<String, String>> brokerToEcuTypesMapping = null;
    
    /** The spc. */
    private StreamProcessingContext<?, ?> spc;
    
    /** The kafka producer. */
    //Kafka producer used to publish to kafka.
    private KafkaProducer<byte[], byte[]> kafkaProducer = null;
    
    /** The kafka bootstrap servers. */
    @Value("${" + PropertyNames.BOOTSTRAP_SERVERS + ":}")
    private String kafkaBootstrapServers;
    
    /** The max request size. */
    @Value("${" + PropertyNames.KAFKA_MAX_REQUEST_SIZE + ":}")
    private String maxRequestSize;
    
    /** The acks config. */
    @Value("${" + PropertyNames.KAFKA_ACKS_CONFIG + ":}")
    private String acksConfig;
    
    /** The retries config. */
    @Value("${" + PropertyNames.KAFKA_RETRIES_CONFIG + ":}")
    private String retriesConfig;
    
    /** The batch size config. */
    @Value("${" + PropertyNames.KAFKA_BATCH_SIZE_CONFIG + ":}")
    private String batchSizeConfig;
    
    /** The linger ms config. */
    @Value("${" + PropertyNames.KAFKA_LINGER_MS_CONFIG + ":}")
    private String lingerMsConfig;
    
    /** The buffer memory config. */
    @Value("${" + PropertyNames.KAFKA_BUFFER_MEMORY_CONFIG + ":}")
    private String bufferMemoryConfig;
    
    /** The request timeout ms config. */
    @Value("${" + PropertyNames.KAFKA_REQUEST_TIMEOUT_MS_CONFIG + ":}")
    private String requestTimeoutMsConfig;
    
    /** The delivery timeout ms config. */
    @Value("${" + PropertyNames.KAFKA_DELIVERY_TIMEOUT_MS_CONFIG + ":}")
    private String deliveryTimeoutMsConfig;
    
    /** The compression type config. */
    @Value("${" + PropertyNames.KAFKA_COMPRESSION_TYPE_CONFIG + ":}")
    private String compressionTypeConfig;
    
    /** The client id. */
    @Value("${" + PropertyNames.CLIENT_ID + ":}")
    private String clientId;
    
    /** The key transformer. */
    @Autowired
    private IgniteKeyTransformer<String> keyTransformer;
    
    /** The device message utils. */
    @Autowired
    private DeviceMessageUtils deviceMessageUtils;
    
    /** The kafka ssl config. */
    @Autowired
    private KafkaSslConfig kafkaSslConfig;

    /**
     * Inits the.
     */
    //If the broker name is present in our brokerToEcuTypesMapping, only then initialize the KafkaProducer.
    private void init() {
        if (brokerToEcuTypesMapping != null && brokerToEcuTypesMapping.containsKey(DeviceMessageDispatchers.KAFKA)) {
            logger.info("Initializing KafkaProducer for DMA KafkaDispatcher...");
            ObjectUtils.requireNonEmpty(kafkaBootstrapServers, "Kafka bootstrap servers config cannot be empty.");

            Properties kafkaConfig = new Properties();
            kafkaConfig.put(PropertyNames.BOOTSTRAP_SERVERS, kafkaBootstrapServers);
            kafkaConfig.put(PropertyNames.KAFKA_MAX_REQUEST_SIZE, maxRequestSize);
            kafkaConfig.put(PropertyNames.KAFKA_ACKS_CONFIG, acksConfig);
            kafkaConfig.put(PropertyNames.KAFKA_BATCH_SIZE_CONFIG, batchSizeConfig);
            kafkaConfig.put(PropertyNames.KAFKA_RETRIES_CONFIG, retriesConfig);
            kafkaConfig.put(PropertyNames.KAFKA_LINGER_MS_CONFIG, lingerMsConfig);
            kafkaConfig.put(PropertyNames.KAFKA_BUFFER_MEMORY_CONFIG, bufferMemoryConfig);
            kafkaConfig.put(PropertyNames.KAFKA_REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMsConfig);
            kafkaConfig.put(PropertyNames.KAFKA_DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMsConfig);
            kafkaConfig.put(PropertyNames.KAFKA_COMPRESSION_TYPE_CONFIG, compressionTypeConfig);
            if (StringUtils.isNotBlank(clientId)) {
                kafkaConfig.put(PropertyNames.CLIENT_ID, clientId);
            }
            kafkaSslConfig.setSslPropsIfEnabled(kafkaConfig);
            kafkaProducer = KafkaProducerInstance.getProducerInstance(kafkaConfig);
            logger.debug("Initialized Kafka Producer for DMA KafkaDispatcher.");
        }
    }
    
    /**
     * setup().
     *
     * @param brokerToEcuTypesMapping brokerToEcuTypesMapping
     * @param spc spc
    */
    
    public void setup(Map<String, Map<String, String>> brokerToEcuTypesMapping, StreamProcessingContext<?, ?> spc) {
        this.brokerToEcuTypesMapping = brokerToEcuTypesMapping;
        this.spc = spc;
        init();
    }
    
    /**
     * Dispatch the key,value to the kafka topic and if some exception occurs while dispatch,
     * publish the failed device message event to the feedback topic.
     * This method does not throw any exception as we don't want to stop the application from 
     * serving other VINs / Requests.
     *
     * @param key the key
     * @param value the value
     */
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void dispatch(IgniteKey key, DeviceMessage value) {
        if (key == null) {
            logger.error("Key is NULL. Not dispatching the data to Kafka");
            return;
        }
        if (value == null) {
            logger.error("Value is NULL. Not dispatching the data to kafka.");
            return;
        }
        logger.debug("Recevied key: {} and value {} in KafkaDispatcher to dispatch.", key.toString(), value);
        
        /*
         * brokerToEcuTypesMapping.get(DeviceMessageDispatchers.KAFKA) will get us the name of the broker/platform
         * which again contains a map of ecuType-topic mapping against it.
         * Hence the another call to get method will get us the name of the Kafka topic against this ecuType.
         */
        String kafkaTopic = brokerToEcuTypesMapping.get(DeviceMessageDispatchers.KAFKA)
                .get(value.getEvent().getEcuType().toLowerCase());
        byte[] keyBytes = keyTransformer.toBlob(key);
        byte[] message = value.getMessage();
        Map<String, String> kafkaHeaders = value.getEvent().getKafkaHeaders();
        List<Header> kafkaHeadersList = new ArrayList<>();
        logger.debug("Retreived kafka headers from IgniteEvent with requestID : {} , headers : {}",
                value.getDeviceMessageHeader().getRequestId(), kafkaHeaders);
        if (kafkaHeaders != null && !kafkaHeaders.isEmpty()) {
            kafkaHeaders.forEach((k, v) -> {
                if (!StringUtils.isEmpty(v)) {
                    kafkaHeadersList.add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
                }
            });
        }
        
        //Send this data to the kafka topic and if any exception occurs, publish the failed DeviceMessage event 
        //to the feedback topic.
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, null, keyBytes, message, kafkaHeadersList), 
                (metadata, exception) -> {
                    if (exception != null) {
                    logger.error("Exception when pushing message directly to stream: ", exception);
                    DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
                    data.setFailedIgniteEvent(value.getEvent());
                    data.setErrorCode(DeviceMessageErrorCode.KAFKA_DISPATCH_FAILED);
                    data.setDeviceStatusInactive(true);
                    deviceMessageUtils.postFailureEvent(data, key, spc, value.getFeedBackTopic());
                    }
                });
        logger.info("Successfully published the message: {} with key: {}, and headers : {}, on kafka topic: {}",
                value, key.toString(), kafkaHeadersList, kafkaTopic);
        dmaPostDispatchHandler.handle(key, value);   
    }

    /**
     * Sets the next handler.
     *
     * @param dmaPostDispatchHandler the new next handler
     */
    public void setNextHandler(DeviceMessageHandler dmaPostDispatchHandler) {
        this.dmaPostDispatchHandler = dmaPostDispatchHandler;
    }
}
