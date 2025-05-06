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
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.GlobalMessageIdGenerator;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


/**
 * Util class for {@link org.eclipse.ecsp.entities.dma.DeviceMessage}.
 */
@Component
public class DeviceMessageUtils {

    /** The msg id generator. */
    @Autowired
    private GlobalMessageIdGenerator msgIdGenerator;
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceMessageUtils.class);
    
    /**
     * postFailureEvent().
     *
     * @param data data
     * @param key key
     * @param spc spc
     * @param feedBackTopic feedBackTopic
     */

    public void postFailureEvent(DeviceMessageFailureEventDataV1_0 data, IgniteKey<?> key, 
            StreamProcessingContext<?, ?> spc, String feedBackTopic) {
        String requestId = data.getFailedIgniteEvent().getRequestId();
        IgniteEventImpl failureEvent = new IgniteEventImpl();
        failureEvent.setEventId(EventID.DEVICEMESSAGEFAILURE);
        failureEvent.setTimestamp(System.currentTimeMillis());
        failureEvent.setRequestId(requestId);
        failureEvent.setBizTransactionId(data.getFailedIgniteEvent().getBizTransactionId());
        failureEvent.setTimezone(data.getFailedIgniteEvent().getTimezone());
        failureEvent.setMessageId(msgIdGenerator.generateUniqueMsgId(data.getFailedIgniteEvent().getVehicleId()));
        failureEvent.setVersion(Version.V1_0);
        failureEvent.setEventData(data);
        spc.forwardDirectly(key, failureEvent, feedBackTopic);
        String payloadMsgId = data.getFailedIgniteEvent().getMessageId();
        logger.debug("{} feedback forwarded to topic {} for key {} with FailedIgniteEvent messageId {} ,requestId {} "
                + "and FeebBackEvent messageId {} ", data.toString(), feedBackTopic, key, payloadMsgId, 
                requestId, failureEvent.getMessageId());
    }
}
