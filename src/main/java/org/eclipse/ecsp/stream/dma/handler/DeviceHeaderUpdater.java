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
import org.eclipse.ecsp.analytics.stream.base.exception.HeaderUpdateException;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.GlobalMessageIdGenerator;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


/**
 * class DeviceHeaderUpdater implements DeviceMessageHandler.
 */
@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DeviceHeaderUpdater implements DeviceMessageHandler {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceHeaderUpdater.class);

    /** The next handler. */
    private DeviceMessageHandler nextHandler;

    /** The msg id generator. */
    @Autowired
    private GlobalMessageIdGenerator msgIdGenerator;

    /** The event header updation. */
    @Value("${" + PropertyNames.DMA_EVENT_HEADER_UPDATION_TYPE + ":}")
    private String eventHeaderUpdation;

    /**
     * Handle.
     *
     * @param key the key
     * @param entity the entity
     */
    @Override
    public void handle(IgniteKey<?> key, DeviceMessage entity) {
        switch (eventHeaderUpdation) {
            case DMAConstants.MESSAGEID:
                entity = addMessageIdIfNotPresent(entity);
                break;
            case DMAConstants.MESSAGEID_AND_CORRELATIONID:
                entity = addMessageIdAndCorrelationIdIfNotPresent(entity);
                break;
            default:
                throw new HeaderUpdateException("Unknown method for eventId updation");
        }
        nextHandler.handle(key, entity);
    }

    /**
     * Sets the next handler.
     *
     * @param handler the new next handler
     */
    @Override
    public void setNextHandler(DeviceMessageHandler handler) {
        nextHandler = handler;

    }

    /**
     * Add messageId if no messageId is set.
     *
     * @param entity the entity
     * @return the device message
     */
    public DeviceMessage addMessageIdIfNotPresent(DeviceMessage entity) {
        DeviceMessageHeader header = entity.getDeviceMessageHeader();
        String currentMsgId = header.getMessageId();
        logger.debug("DMA header updation : Current MessageId {}", currentMsgId);
        if (StringUtils.isEmpty(currentMsgId)) {
            String msgIdGenerated = msgIdGenerator.generateUniqueMsgId(header.getVehicleId());
            header.withMessageId(msgIdGenerated);
            logger.debug("New MessageId {} added to header by DMA", msgIdGenerated);
        }
        return entity;
    }

    /**
     * Set correlation Id as messageId if messageId is present.
     * Update messageId to a new value before dispatching to device.
     *
     * @param entity DeviceMessage
     * @return DeviceMessage
     */
    public DeviceMessage addMessageIdAndCorrelationIdIfNotPresent(DeviceMessage entity) {
        DeviceMessageHeader header = entity.getDeviceMessageHeader();
        String currentMsgId = header.getMessageId();
        logger.debug("DMA header updation : Current MessageId {}", currentMsgId);

        // Update correlationId
        if (StringUtils.isNotEmpty(currentMsgId)) {
            header.withCorrelationId(currentMsgId);
            logger.debug("CorrelationId updated with MessageId {} by DMA", header.getCorrelationId());

        }

        // Update messageId
        String msgIdGenerated = msgIdGenerator.generateUniqueMsgId(header.getVehicleId());
        if (StringUtils.isNotEmpty(msgIdGenerated)) {
            header.withMessageId(msgIdGenerated);
            logger.debug("New MessageId {} added to header by DMA", msgIdGenerated);
        } else {
            logger.error("Generated MessageId is null of Empty");
            throw new HeaderUpdateException("Generated MessageId is null of Empty");
        }
        logger.info("DMA updated event for requestId {} with messageId {} ", 
                header.getRequestId(), header.getMessageId());
        return entity;
    }

    /**
     * validate().
     */
    @PostConstruct
    public void validate() {
        if (StringUtils.isEmpty(eventHeaderUpdation)) {
            throw new HeaderUpdateException("Event header updation type cannot be null or Empty");
        }
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        // Nothing to do as of now
    }

}
