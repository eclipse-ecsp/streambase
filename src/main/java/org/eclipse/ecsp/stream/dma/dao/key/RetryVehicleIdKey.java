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

import org.eclipse.ecsp.analytics.stream.base.stores.CacheKeyConverter;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;


/**
 * This key is used to identify the record that has to be retried.
 *
 * @author avadakkootko
 */
public class RetryVehicleIdKey implements CacheKeyConverter<RetryVehicleIdKey> {

    /** The key. */
    private String key;

    /**
     * Instantiates a new retry vehicle id key.
     */
    public RetryVehicleIdKey() {
    }

    /**
     * Instantiates a new retry vehicle id key.
     *
     * @param key the key
     */
    public RetryVehicleIdKey(String key) {
        this.key = key;
    }

    /**
     * getMapKey(): to fetch the key.
     *
     * @param serviceName serviceName
     * @param taskId taskId
     * @return String
     */
    public static String getMapKey(String serviceName, String taskId) {
        StringBuilder regexBuilder = new StringBuilder()
                .append(DMAConstants.SHOULDER_TAP_RETRY_VEHICLEID).append(DMAConstants.COLON)
                .append(serviceName).append(DMAConstants.COLON).append(taskId);
        return regexBuilder.toString();
    }

    /**
     * Gets the key.
     *
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets the key.
     *
     * @param key the new key
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Convert from.
     *
     * @param key the key
     * @return the retry vehicle id key
     */
    @Override
    public RetryVehicleIdKey convertFrom(String key) {
        return new RetryVehicleIdKey(key);
    }

    /**
     * Convert to string.
     *
     * @return the string
     */
    @Override
    public String convertToString() {
        return key;
    }

    /**
     * Hash code.
     *
     * @return the int
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    /**
     * Equals.
     *
     * @param obj the obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RetryVehicleIdKey other = (RetryVehicleIdKey) obj;
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        return true;
    }

}