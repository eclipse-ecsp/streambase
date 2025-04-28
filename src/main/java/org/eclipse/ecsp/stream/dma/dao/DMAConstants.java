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

package org.eclipse.ecsp.stream.dma.dao;


/**
 * Constants class.
 */
public class DMAConstants {

    /**
     * Instantiates a new DMA constants.
     */
    private DMAConstants() {
        throw new UnsupportedOperationException("Objects for utility classes can not be created");
    }
    
    /** The Constant ACTIVE. */
    public static final String ACTIVE = "ACTIVE";
    
    /** The Constant INACTIVE. */
    public static final String INACTIVE = "INACTIVE";
    
    /** The Constant VEHICLE_DEVICE_MAPPING. */
    public static final String VEHICLE_DEVICE_MAPPING = "VEHICLE_DEVICE_MAPPING:";
    
    /** The Constant RETRY_MESSAGEID. */
    public static final String RETRY_MESSAGEID = "RETRY_MESSAGEID";
    
    /** The Constant RETRY_BUCKET. */
    public static final String RETRY_BUCKET = "RETRY_BUCKET";
    
    /** The Constant DEVICE_STATUS_TOPIC_PREFIX. */
    public static final String DEVICE_STATUS_TOPIC_PREFIX = "device-status-";
    
    /** The Constant DMA. */
    public static final String DMA = "DMA";
    
    /** The Constant CONSUMER_GROUP. */
    public static final String CONSUMER_GROUP = "consumer-group";
    
    /** The Constant HYPHEN. */
    public static final String HYPHEN = "-";
    
    /** The Constant MESSAGEID. */
    public static final String MESSAGEID = "messageId";
    
    /** The Constant MESSAGEID_AND_CORRELATIONID. */
    public static final String MESSAGEID_AND_CORRELATIONID = "messageIdAndCorrelationId";
    
    /** The Constant COLON. */
    public static final String COLON = ":";
    
    /** The Constant SEMI_COLON. */
    public static final String SEMI_COLON = ";";
    
    /** The Constant FORWARD_SLASH. */
    public static final String FORWARD_SLASH = "/";
    
    /** The Constant BIZ_TRANSACTION_ID. */
    public static final String BIZ_TRANSACTION_ID = "bizTransactionId";

    /** The Constant DMA_SHOULDER_TAP_ENABLED_MESSAGE_PRIORITY. */
    public static final short DMA_SHOULDER_TAP_ENABLED_MESSAGE_PRIORITY = 10;
    
    /** The Constant SHOULDER_TAP_RETRY_BUCKET. */
    public static final String SHOULDER_TAP_RETRY_BUCKET = "SHOULDER_TAP_RETRY_BUCKET";
    
    /** The Constant SHOULDER_TAP_RETRY_VEHICLEID. */
    public static final String SHOULDER_TAP_RETRY_VEHICLEID = "SHOULDER_TAP_RETRY_VEHICLEID";
    
    /** The Constant TOP_RECORD_NUMBER. */
    public static final Integer TOP_RECORD_NUMBER = 1;
    
    /** The Constant DM_NEXT_TTL_EXPIRATION_TIMER_KEY. */
    public static final String DM_NEXT_TTL_EXPIRATION_TIMER_KEY = "nextTtlExpirationTimer";
    
    /** The Constant TTL_EXPRIATION_TIME_FIELD. */
    public static final String TTL_EXPRIATION_TIME_FIELD = "ttlExpirationTime";
    
    /** The Constant IS_TTL_NOTIF_PROCESSED_FIELD. */
    public static final String IS_TTL_NOTIF_PROCESSED_FIELD = "isTtlNotifProcessed";

    /** The Constant DEFAULT_DMA_CONFIG_RESOLVER. */
    public static final String DEFAULT_DMA_CONFIG_RESOLVER =
            "org.eclipse.ecsp.stream.dma.config.DefaultDMAConfigResolver";
    
    /** The Constant DMA_DEFAULT_POST_DISPATCH_HANDLER_CLASS. */
    public static final String DMA_DEFAULT_POST_DISPATCH_HANDLER_CLASS =
            "org.eclipse.ecsp.stream.dma.handler.DefaultPostDispatchHandler";
}
