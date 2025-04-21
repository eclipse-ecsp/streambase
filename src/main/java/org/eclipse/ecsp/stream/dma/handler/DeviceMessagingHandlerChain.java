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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidKeyOrValueException;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidSourceTopicException;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.config.DMAConfigResolver;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.transform.Transformer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * <p>
 *     DeviceMessagingGateway is the entry point or gateway for processing messages
 * to be sent to Device.
 *
 * Its responsibility includes process chaining among the different Handlers
 * which enriches the DeviceMessage IgniteEvent.
 * </p>
 *
 * @author avadakkootko
 */
@Lazy
@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DeviceMessagingHandlerChain {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceMessagingHandlerChain.class);

    /** The device message validator. */
    @Autowired
    private DeviceMessageValidator deviceMessageValidator;

    /** The header updater. */
    @Autowired
    private DeviceHeaderUpdater headerUpdater;

    /** The conn status handler. */
    @Autowired
    private DeviceConnectionStatusHandler connStatusHandler;

    /** The retry handler. */
    @Autowired
    private RetryHandler retryHandler;

    /** The dispatch handler. */
    @Autowired
    private DispatchHandler dispatchHandler;

    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /** The device messaging event transformer. */
    @Value("${" + PropertyNames.DEVICE_MESSAGING_EVENT_TRANSFORMER + "}")
    private String deviceMessagingEventTransformer;

    /** The device message feedback topic. */
    @Value("${" + PropertyNames.DEVICE_MESSAGE_FEEDBACK_TOPIC + ":#{null}}")
    private String deviceMessageFeedbackTopic;

    /** The dma config resolver impl class. */
    @Value("${" + PropertyNames.DMA_CONFIG_RESOLVER_CLASS + ":" + DMAConstants.DEFAULT_DMA_CONFIG_RESOLVER + "}")
    private String dmaConfigResolverImplClass;
    
    /** The dma post dispatch handler class. */
    @Value("${" + PropertyNames.DMA_POST_DISPATCH_HANDLER_CLASS + ":"
            + DMAConstants.DMA_DEFAULT_POST_DISPATCH_HANDLER_CLASS + "}")
    private String dmaPostDispatchHandlerClass;
    
    /**
     * <P>
     * DMA should have the capability to dispatch the DeviceMessage for various ecuTypes to various brokers 
     * if configured so. 
     * By default DMA dispatches to MQTT broker but if below property is configured correctly, it will be able to 
     * dispatch to other brokers as well for respective ecuTypes.
     * 
     * The @Value annotation will give us a list, at every index of which,
     * we will have broker to ecuType-topicName mapping.
     * For eg: at 0 index: kafka:ecuType1#kafkaTopic1,ecuType2#kafkaTopic2
     * at 1 index: rabbitMQ:ecuType3#kafkaTopic3,ecuType4#kafkaTopic4
     * 
     * In above example kakfa and rabbitMQ are the names
     * of the brokers where DMA has to dispatch data.
     * ecuType1,ecuType2 are the names of the ecuTypes for which data
     * will be dispatched to kafkaTopic1 and kafkaTopic2 of the
     * respective broker.
     * Same goes for rabbitMQ.
     * </P>
     */
    @Value("#{'${" + PropertyNames.DMA_DISPATCHER_ECU_TYPES + "}'.split(';')}")
    private List<String> ecuTypes;
    
    /** The broker to ecu types mapping. */
    private Map<String, Map<String, String>> brokerToEcuTypesMapping = null;
    
    /** The ecu types list. */
    private List<String> ecuTypesList = null;

    /** The dm event transformer. */
    private Transformer dmEventTransformer;

    /** The config resolver. */
    private DMAConfigResolver configResolver;
    
    /** The dma post dispatch handler. */
    private DeviceMessageHandler dmaPostDispatchHandler;

    /**
     * handle().
     *
     * @param key key
     * @param value value
     */
    public void handle(IgniteKey<?> key, IgniteEvent value) {
        logger.info("DeviceMessagingGateway processing event with requestId {} and messageId {} ", value.getRequestId(),
                value.getMessageId());

        IgniteEventImpl event = (IgniteEventImpl) value;
        if (value.isDeviceRoutable()) {
            deviceMessageValidator.handle(key, getDeviceRoutableEntity(event));
        }
    }

    /**
     * Gets the device routable entity.
     *
     * @param value the value
     * @return the device routable entity
     */
    private DeviceMessage getDeviceRoutableEntity(IgniteEventImpl value) {
        byte[] payLoad = dmEventTransformer.toBlob(value);
        DeviceMessage deviceRoutableEntity;
        if (StringUtils.isEmpty(spc.streamName())) {
            throw new InvalidSourceTopicException(
                    "SourceTopic unavailable in Stream Processing Context for IgniteEvent " + value.toString()
                            + ". Cannot Proceed Forward");
        }
        long retryIntervalAtEventLevel = configResolver.getRetryInterval(value);

        if (retryIntervalAtEventLevel <= 0) {
            throw new IllegalArgumentException(
                    "Received retry interval from DMAConfigResolver as "
                            + retryIntervalAtEventLevel + ", should be greater than zero ");
        }

        if (StringUtils.isEmpty(deviceMessageFeedbackTopic)) {
            deviceRoutableEntity = new DeviceMessage(payLoad, Version.V1_0,
                    value, spc.streamName(), retryIntervalAtEventLevel);
        } else {
            deviceRoutableEntity = new DeviceMessage(payLoad, Version.V1_0,
                    value, deviceMessageFeedbackTopic, retryIntervalAtEventLevel);
        }
        /*
         * Since we have brokerToEcuTypesMapping here in this class, hence to avoid processing in
         * DeviceConnectionStatusHandler, which route to take any DeviceMessage from, one field introduced in
         * DeviceMessage which will contain the information whether this DeviceMessage has to be published to
         * some other broker than HiveMQ.
         */
        if (brokerToEcuTypesMapping != null && !brokerToEcuTypesMapping.isEmpty()) {
            String ecuType = value.getEcuType().toLowerCase();
            for (Map.Entry<String, Map<String, String>> entry : brokerToEcuTypesMapping.entrySet()) {
                Map<String, String> map = entry.getValue();
                if (map.containsKey(ecuType) && !StringUtils.isEmpty(map.get(ecuType))) {
                    deviceRoutableEntity.isOtherBrokerConfigured(true);
                }
            }
        }
        return deviceRoutableEntity;
    }

    /**
     * Gets the instance by class name.
     *
     * @param canonicalClassName the canonical class name
     * @return the instance by class name
     */
    private Object getInstanceByClassName(String canonicalClassName) {
        Object instance = null;
        Class<?> classObject = null;
        try {
            classObject = getClass().getClassLoader().loadClass(canonicalClassName);
            instance = ctx.getBean(classObject);
            logger.info("Class {} loaded from spring application context", classObject.getName());
        } catch (Exception ex) {
            try {
                if (classObject == null) {
                    throw new IllegalArgumentException("Could not load the class " + canonicalClassName);
                }
                logger.info("Class {} could not be loaded as spring bean. Attempting to create new instance.", 
                        canonicalClassName);
                instance = classObject.getDeclaredConstructor().newInstance();
            } catch (Exception exception) {
                String msg = String.format("Class %s could not be loaded. Not found on classpath.%n", 
                        canonicalClassName);
                logger.error(msg + ExceptionUtils.getStackTrace(exception));
                throw new IllegalArgumentException(msg);
            }
        }
        return instance;
    }


    /**
     * Construct process chain.
     *
     * @param taskId the task id
     * @param spc the spc
     */
    public void constructChain(String taskId, StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        if (spc == null) {
            throw new InvalidKeyOrValueException("Stream Processing Context is null. Cannot Proceed Forward");
        }
        this.spc = spc;
        retryHandler.setStreamProcessingContext(spc);
        connStatusHandler.setStreamProcessingContext(spc);
        dispatchHandler.setStreamProcessingContext(spc);
        dmaPostDispatchHandler.setStreamProcessingContext(spc);
        connStatusHandler.setup(taskId, ecuTypesList);
        retryHandler.setup(taskId);
        dispatchHandler.setup(taskId, brokerToEcuTypesMapping);
        retryHandler.setConnStatusHandler(connStatusHandler);

        deviceMessageValidator.setNextHandler(connStatusHandler);
        connStatusHandler.setNextHandler(headerUpdater);
        headerUpdater.setNextHandler(retryHandler);
        retryHandler.setNextHandler(dispatchHandler);
        dispatchHandler.setNextHandler(dmaPostDispatchHandler);
        logger.info("Chained : DeviceMessageValidator -> DeviceIdUpdator -> ConnectionStatusHandler -> "
                + "RetryHandler -> DispatchHandler -> PostDispatchHandler");
    }

    /**
     * setUp().
     */
    @PostConstruct
    public void setUp() {
        if (StringUtils.isEmpty(deviceMessagingEventTransformer)) {
            throw new IllegalArgumentException(PropertyNames
                    .DEVICE_MESSAGING_EVENT_TRANSFORMER + " unavailable in property file");
        }
        dmEventTransformer = (Transformer) getInstanceByClassName(deviceMessagingEventTransformer);
        logger.debug("Device Messaging Event Transformer initialized is {}", deviceMessagingEventTransformer);
        
        if (dmaConfigResolverImplClass == null) {
            dmaConfigResolverImplClass = DMAConstants.DEFAULT_DMA_CONFIG_RESOLVER;
        }
        configResolver = (DMAConfigResolver) getInstanceByClassName(dmaConfigResolverImplClass);
        dmaPostDispatchHandler = (DeviceMessageHandler) getInstanceByClassName(dmaPostDispatchHandlerClass);
        populateMap();
    }
    
    /**
     * To understand the below parsing, refer to the comment over {@link #ecuTypes} property.
     * This map will be passed to the DispatchHandler where the decision as to which dispatcher to hand the 
     * DeviceMessage to, will be taken. 
     */
    private void populateMap() {
        if (ecuTypes != null && !ecuTypes.isEmpty()) {
            brokerToEcuTypesMapping = new HashMap<>();
            for (String pair : ecuTypes) {
                String[] keysValues = pair.split(":");
                String brokerName = keysValues[0].toLowerCase();
                if (keysValues.length == 1) {
                    logger.info("Only broker name: {} found against dma.dispatcher.ecu.types. "
                            + "List of comma separated ecuType to Kafka topic mapping not found. "
                            + "By default messages will be dispatched to HiveMQ");
                    continue;
                }
                String[] mappings = keysValues[1].split(Constants.COMMA);
                Map<String, String> ecuTypeToTopicMappings = new HashMap<>();
                for (String mapping : mappings) {
                    createEcuTypesList(ecuTypeToTopicMappings, mapping);
                }
                brokerToEcuTypesMapping.put(brokerName, ecuTypeToTopicMappings);
            }
            logger.info("BrokerToECUTypesMappings constructed as: {}", brokerToEcuTypesMapping.toString());
        }
    }

    /**
     * Creates the ecu types list.
     *
     * @param ecuTypeToTopicMappings the ecu type to topic mappings
     * @param mapping the mapping
     */
    private void createEcuTypesList(Map<String, String> ecuTypeToTopicMappings, String mapping) {
        String[] str = mapping.split(Constants.ECU_TYPE_BROKER_TOPIC_DELIMETER);
        String ecuType = str[0].toLowerCase();
        if (str.length == 1) {
            logger.info("No kafka topic name found against ecuType: {} By default messages for this ecuType "
                    + "will be dispatched to HiveMQ", ecuType);
            return;
        }
        String kafkaTopic = str[1];
        ecuTypeToTopicMappings.put(ecuType, kafkaTopic);
        if (ecuTypesList == null) {
            ecuTypesList = new ArrayList<>();
        }
        //marking this ecuType as a valid ecuType for which DeviceMessages has to be taken through
        //new route in DeviceConnectionStatusHandler.
        ecuTypesList.add(ecuType);
    }

    /**
     * close: to close the opened resources.
     */
    public void close() {
        deviceMessageValidator.close();
        connStatusHandler.close();
        headerUpdater.close();
        retryHandler.close();
        dispatchHandler.close();
        dmaPostDispatchHandler.close();
    }
}
