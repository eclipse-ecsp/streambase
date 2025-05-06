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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


/**
 *DefaultPostDispatchHandler is responsible for handling the messages which have 
 *been successfully published to MQTT via DispatchHandler.
 *
 * @author karora
 *
 */
@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DefaultPostDispatchHandler implements DeviceMessageHandler {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DefaultPostDispatchHandler.class);

    /**
     * Handle.
     *
     * @param key the key
     * @param value the value
     */
    @Override
    public void handle(IgniteKey<?> key, DeviceMessage value) {
        logger.debug("DefaultPostDispatchHandler invoked. Message published successfully to MQTT with key : {} and "
                + "vehicleID : {}", key, value.getDeviceMessageHeader().getVehicleId());
    }

    /**
     * Sets the next handler.
     *
     * @param handler the new next handler
     */
    @Override
    public void setNextHandler(DeviceMessageHandler handler) {
        //Overridden method
    }

    
    /**
     * Close.
     */
    @Override
    public void close() {
        //Overridden method
    }
}
