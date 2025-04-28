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

/**
 * class {@link InternalCacheConstants}: constants file.
 */
public class InternalCacheConstants {

    /**
     * Instantiates a new internal cache constants.
     */
    private InternalCacheConstants() {

    }

    /** The Constant CACHE_TYPE_RETRY_RECORD. */
    public static final String CACHE_TYPE_RETRY_RECORD = "retry_record_cache";
    
    /** The Constant CACHE_TYPE_RETRY_BUCKET. */
    public static final String CACHE_TYPE_RETRY_BUCKET = "retry_bucket_cache";
    
    /** The Constant CACHE_TYPE_SHOULDER_TAP_RETRY_RECORD. */
    public static final String CACHE_TYPE_SHOULDER_TAP_RETRY_RECORD = "shoulder_tap_retry_record_cache";
    
    /** The Constant CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET. */
    public static final String CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET = "shoulder_tap_retry_bucket_cache";
    
    /** The Constant CACHE_TYPE_DEVICE_CONN_STATUS_CACHE. */
    public static final String CACHE_TYPE_DEVICE_CONN_STATUS_CACHE = "device_conn_status_cache";
}
