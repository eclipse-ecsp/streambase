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

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidTargetIDException;
import org.eclipse.ecsp.analytics.stream.base.exception.MqttTopicException;
import org.eclipse.ecsp.analytics.stream.base.platform.MqttTopicNameGenerator;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * The default logic of creating the MQTT Topic name is in this class.
 */
@Component
@Scope("prototype")
@ConditionalOnExpression("${" + PropertyNames.DMA_ENABLED + ":true} "
        + "and '${" + PropertyNames.MQTT_TOPIC_GENERATOR_SERVICE_IMPL_CLASS_NAME + "}'"
                + ".equalsIgnoreCase('" + PropertyNames.DEFAULT_TOPIC_NAME_GENERATOR_IMPL + "')"
)
public class DefaultMqttTopicNameGeneratorImpl implements MqttTopicNameGenerator {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DefaultMqttTopicNameGeneratorImpl.class);

    /** The topic separator. */
    // mqtt topic separator
    @Value("${" + PropertyNames.MQTT_TOPIC_SEPARATOR + ":" + Constants.FORWARD_SLASH + "}")
    protected String topicSeparator;

    /** The to device. */
    // mqtt topic separator
    @Value("${" + PropertyNames.MQTT_TOPIC_TO_DEVICE_INFIX + ":" + Constants.TO_DEVICE + "}")
    protected String toDevice;

    /** The topic name prefix. */
    @Value("${" + PropertyNames.MQTT_SERVICE_TOPIC_NAME_PREFIX + ":}")
    protected String topicNamePrefix;
    
    /** The service topic name. */
    @Value("${" + PropertyNames.MQTT_SERVICE_TOPIC_NAME + "}")
    protected String serviceTopicName;

    /** The sub services. */
    /*
     * RTC 355420. DMA should have the functionality to track device connection status at sub-service level,
     * if configured any.
     */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    protected String subServices;

    /**
     * Gets the topic name prefix.
     *
     * @return the topic name prefix
     */
    public String getTopicNamePrefix() {
        return topicNamePrefix;
    }

    /**
     * Sets the topic name prefix.
     *
     * @param topicNamePrefix the new topic name prefix
     */
    public void setTopicNamePrefix(String topicNamePrefix) {
        this.topicNamePrefix = topicNamePrefix;
    }
    
    /**
     * Verify and dump mqtt properties.
     */
    @PostConstruct
    private void verifyAndDumpMqttProperties() {
        ObjectUtils.requireNonEmpty(serviceTopicName, "Service topic name shouldn't be null or empty.");
    }
    
    /**
     * Gets the mqtt topic name.
     *
     * @param key the key
     * @param header the header
     * @param eventId the event id
     * @return the mqtt topic name
     */
    @Override
    public Optional<String> getMqttTopicName(IgniteKey key, DeviceMessageHeader header, String eventId) {

        /*
         * DataPlatform 101-HCP-12088, All the events going to Mqtt are
         * white listed and Mqtt Topic is vehicleId/serviceName/eventId
         *
         * Later Mqtt topic will be targetDeviceId+eventID. Targeted deviceID
         * could be either TELEMATICS or HU. Both TELEMATICS and HU can be present in the
         * vehicle, and thats why we would like to know through targetDeviceID
         * who is the recipient of this event.
         *
         * s
         */

        String topicName = null;
        if (header.isGlobalTopicNameProvided()) {
            topicName = header.getDevMsgGlobalTopic();
            logger.debug("Global mqtt topic name {} is set for for key:{} ", topicName, key.toString());
        } else {
            StringBuilder builder = new StringBuilder();
            String deviceId = header.getTargetDeviceId();
            if (StringUtils.isEmpty(deviceId)) {
                throw new InvalidTargetIDException("Target device not present. Cannot dispatch to MQTT");
            }

            String actualTopicPrefix = header.getDevMsgTopicPrefix();
            if (!StringUtils.isEmpty(actualTopicPrefix)) {
                buildActualTopicPrefix(builder, actualTopicPrefix);
            } else {
                builder.append(topicNamePrefix);
                logger.debug("Mqtt topic prefix from property file is {}", topicNamePrefix);
            }

            // topicNamePrefix will already have forward slash at the
            // end.mqtt.service.topic.name.prefix=haa/harman/dev/
            // IN case topicNamePrefix is empty then the topic name will be
            // vehicleId/serviceName/eventId (Note the absense of forward slash
            // at the begining
            // toDevice should have prefix with topic separator in
            // mqtt.topic.to.device.infix property, for example /2d

            String actualTopicSuffix = header.getDevMsgTopicSuffix();

            if (StringUtils.isNotEmpty(subServices)) {
                builder.append(deviceId).append(topicSeparator);
                actualTopicSuffix = actualTopicSuffix.toUpperCase();
            } else {
                builder.append(deviceId).append(toDevice).append(topicSeparator);
            }

            if (!StringUtils.isEmpty(actualTopicSuffix)) {
                topicName = buildActualTopicSuffix(builder, actualTopicSuffix);
            } else {
                topicName = builder.append(serviceTopicName).toString();
            }
        }

        logger.info("Mqtt Topic Name for key : {} , eventID : {} with device message headers : {} is : {}", 
                key.toString(), eventId, header.toString(), topicName);

        return Optional.ofNullable(topicName);

    }

    /**
     * Builds the actual topic suffix.
     *
     * @param builder the builder
     * @param actualTopicSuffix the actual topic suffix
     * @return the string
     */
    @NotNull
    protected static String buildActualTopicSuffix(StringBuilder builder, String actualTopicSuffix) {
        String topicName;
        if (actualTopicSuffix.startsWith("/")) {
            actualTopicSuffix = actualTopicSuffix.replaceFirst("/", "");
        }
        if (StringUtils.isEmpty(actualTopicSuffix)) {
            throw new MqttTopicException("Mqtt topic suffix set is empty");
        }
        topicName = builder.append(actualTopicSuffix).toString();
        return topicName;
    }

    /**
     * Builds the actual topic prefix.
     *
     * @param builder the builder
     * @param actualTopicPrefix the actual topic prefix
     */
    protected static void buildActualTopicPrefix(StringBuilder builder, String actualTopicPrefix) {
        if (actualTopicPrefix.startsWith("/")) {
            actualTopicPrefix = actualTopicPrefix.replaceFirst("/", "");
        }
        if (StringUtils.isEmpty(actualTopicPrefix)) {
            throw new MqttTopicException("Mqtt topic prefix set is empty");
        }
        builder.append(actualTopicPrefix);
        logger.debug("Mqtt topic prefix from event is {}", actualTopicPrefix);
    }
    
}
