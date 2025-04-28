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

import org.apache.kafka.streams.KafkaStreams.State;
import org.eclipse.ecsp.analytics.stream.base.KafkaStateAgentListener;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


/**
 * {@link DMABackdoorKafkaConsumer} implements {@link KafkaStateAgentListener}.
 */
@Component
public class DMABackdoorKafkaConsumer implements KafkaStateAgentListener {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DMABackdoorKafkaConsumer.class);

    /** The device status back door kafka consumer. */
    @Autowired
    private DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;

    /**
     * dma.enabled flag is linked with devicestatuskafkaconsumer as both are components of dma.
     */
    @Value("${" + PropertyNames.DMA_ENABLED + ":true}")
    private boolean isDmaEnabled;

    /**
     * On change.
     *
     * @param newState the new state
     * @param oldState the old state
     */
    @Override
    public void onChange(State newState, State oldState) {
        if (isDmaEnabled) {
            logger.info("Attempting to Start DMABackDoorKafkaConsumer...");
            deviceStatusBackDoorKafkaConsumer.startDMABackDoorConsumer();
        }
    }
}
