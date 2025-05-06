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
 * This class will be used as a key to retry shoulder tap while waking up a Vehicle.
 *
 * @author avadakkootko
 */
public class ShoulderTapRetryBucketKey extends AbstractRetryBucketKey<ShoulderTapRetryBucketKey> {

    /**
     * Instantiates a new shoulder tap retry bucket key.
     */
    public ShoulderTapRetryBucketKey() {
        super();
    }

    /**
     * Instantiates a new shoulder tap retry bucket key.
     *
     * @param timestamp the timestamp
     */
    public ShoulderTapRetryBucketKey(long timestamp) {
        super(timestamp);
    }

    /**
     *  getMapKey(): to create the key using StringBuilder.
     *
     * @param serviceName serviceName
     * @param taskId taskId
     * @return String
     */
    public static String getMapKey(String serviceName, String taskId) {
        StringBuilder regexBuilder = new StringBuilder()
                .append(DMAConstants.SHOULDER_TAP_RETRY_BUCKET).append(DMAConstants.COLON)
                .append(serviceName).append(DMAConstants.COLON).append(taskId);
        return regexBuilder.toString();
    }

    /**
     * Convert from.
     *
     * @param key the key
     * @return the shoulder tap retry bucket key
     */
    @Override
    public ShoulderTapRetryBucketKey convertFrom(String key) {
        return new ShoulderTapRetryBucketKey(Long.parseLong(key));
    }

    /**
     * Compare to.
     *
     * @param obj the obj
     * @return the int
     */
    @Override
    public int compareTo(Object obj) {
        ShoulderTapRetryBucketKey comparableObj = (ShoulderTapRetryBucketKey) obj;
        long x = this.getTimestamp();
        long y = comparableObj.getTimestamp();
        if (x < y) {
            return Constants.INT_MINUS_ONE;
        } else {
            return (x == y) ? 0 : 1;
        }
    }

}
