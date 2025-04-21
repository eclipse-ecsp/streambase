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

package org.eclipse.ecsp.stream.dma.presencemanager;

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.GlobalMessageIdGenerator;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.FetchConnectionStatusEventData;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


/**
 * DeviceFetchConnectionStatusProducer is responsible to pushing event to 
 * fetch connection status kafka topic.

 * @author karora
 */
@Component
@Scope("prototype")
public class DeviceFetchConnectionStatusProducer {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceFetchConnectionStatusProducer.class);

    /** The fetch connection status topic. */
    @Value("${" + PropertyNames.FETCH_CONNECTION_STATUS_TOPIC_NAME + ":}")
    private String fetchConnectionStatusTopic;
    
    /** The global message id generator. */
    @Autowired
    private GlobalMessageIdGenerator globalMessageIdGenerator;
    
    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /**
     * Push event to "Fetch Connection Status" kafka topic.
     *
     * @param key The IgniteKey
     * @param header The DeviceMessageHeader
     * @param ctx The StreamProcessingContext
     */
    public void pushEventToFetchConnStatus(IgniteKey<?> key, DeviceMessageHeader header, 
            StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctx) {
        setContext(ctx);
        IgniteEventImpl fetchConnStatusEvent = createEvent(header);
        logger.info("Sending event to topic : {} to fetch connection status : {}", 
                fetchConnectionStatusTopic, fetchConnStatusEvent);
        spc.forwardDirectly(key, fetchConnStatusEvent, fetchConnectionStatusTopic);
    }
    
    /**
     * Creates the event.
     *
     * @param msgHeader the msg header
     * @return the ignite event impl
     */
    private IgniteEventImpl createEvent(DeviceMessageHeader msgHeader) {
        FetchConnectionStatusEventData fetchConnEventData = new FetchConnectionStatusEventData();
        fetchConnEventData.setVehicleId(msgHeader.getVehicleId());
        fetchConnEventData.setPlatformId(msgHeader.getPlatformId());
        IgniteEventImpl fetchConnStatusEvent = new IgniteEventImpl();
        fetchConnStatusEvent.setEventId(EventID.FETCH_CONN_STATUS);
        fetchConnStatusEvent.setTimestamp(System.currentTimeMillis());
        fetchConnStatusEvent.setVersion(Version.V1_0);
        fetchConnStatusEvent.setEventData(fetchConnEventData);
        fetchConnStatusEvent.setTimezone(msgHeader.getTimezone());
        fetchConnStatusEvent.setMessageId(globalMessageIdGenerator.generateUniqueMsgId(msgHeader.getVehicleId()));
        
        return fetchConnStatusEvent;
    }

    /**
     * Sets the context.
     *
     * @param ctx the ctx
     */
    private void setContext(StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctx) {
        spc = ctx;
    }
}
