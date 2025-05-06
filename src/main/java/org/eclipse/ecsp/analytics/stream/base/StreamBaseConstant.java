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

package org.eclipse.ecsp.analytics.stream.base;

/**
 * Constants class for StreamBase.
 */
public class StreamBaseConstant {

    /** The Constant ASCENDING. */
    public static final String ASCENDING = "ascending";
    
    /** The Constant DESCENDING. */
    public static final String DESCENDING = "descending";
    
    /** The Constant DLQ_TOPIC_POSFIX. */
    public static final String DLQ_TOPIC_POSFIX = "-dlq";
    
    /** The Constant MSG_SEQ_PREFIX. */
    public static final String MSG_SEQ_PREFIX = "msg-seq-";
    
    /** The Constant MSG_SEQ_LAST_PROCESSED_EVENT_TS. */
    public static final String MSG_SEQ_LAST_PROCESSED_EVENT_TS = "MSG_SEQ_LAST_PROCESSED_EVENT_TS_";
    
    /** The Constant MSG_SEQ_FLUSH_EVENT_DATA. */
    public static final String MSG_SEQ_FLUSH_EVENT_DATA = "FlushEvent data";
    
    /** The Constant MSG_FILTER. */
    public static final String MSG_FILTER = "MESSAGE_FILTER_";
    
    /** The Constant UNDERSCORE. */
    public static final String UNDERSCORE = "_";

    /**
     * Private constructor to not allow instantiation of a constants class.
     */
    private StreamBaseConstant() {
    }

    /** The Constant DMA_ONE_WAY_TLS_AUTH_MECHANISM. */
    public static final String DMA_ONE_WAY_TLS_AUTH_MECHANISM = "one-way-tls";
}
