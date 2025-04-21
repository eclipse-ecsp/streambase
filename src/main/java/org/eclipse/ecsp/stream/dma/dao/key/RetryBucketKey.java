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

package org.eclipse.ecsp.stream.dma.dao.key;

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;


/**
 * It is used to denote the timestamp or bucket consisting of messageIds which will be retried.
 *
 * @author avadakkootko
 */
public class RetryBucketKey extends AbstractRetryBucketKey<RetryBucketKey> {

    /**
     * Instantiates a new retry bucket key.
     */
    public RetryBucketKey() {
        super();
    }

    /**
     * Instantiates a new retry bucket key.
     *
     * @param timestamp the timestamp
     */
    public RetryBucketKey(long timestamp) {
        super(timestamp);
    }

    /**
     * getMapKey().
     *
     * @param serviceName serviceName
     * @param taskId taskId
     * @return String
     */
    public static String getMapKey(String serviceName, String taskId) {
        StringBuilder regexBuilder = new StringBuilder().append(DMAConstants.RETRY_BUCKET).append(DMAConstants.COLON)
                .append(serviceName).append(DMAConstants.COLON).append(taskId);
        return regexBuilder.toString();
    }

    /**
     * Convert from.
     *
     * @param key the key
     * @return the retry bucket key
     */
    @Override
    public RetryBucketKey convertFrom(String key) {
        return new RetryBucketKey(Long.parseLong(key));
    }

    /**
     * Compare to.
     *
     * @param obj the obj
     * @return the int
     */
    @Override
    public int compareTo(Object obj) {
        RetryBucketKey comparableObj = (RetryBucketKey) obj;
        long x = this.getTimestamp();
        long y = comparableObj.getTimestamp();
        if (x < y) {
            return Constants.NEGATIVE_ONE;
        } else {
            return (x == y) ? 0 : 1;
        }
    }
}