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
public class RetryRecordKey implements CacheKeyConverter<RetryRecordKey> {

    /** The key. */
    private String key;
    
    /** The task id. */
    private String taskId;

    /**
     * Instantiates a new retry record key.
     */
    public RetryRecordKey() {
    }

    /**
     * Instantiates a new retry record key.
     *
     * @param key the key
     * @param taskId the task id
     */
    public RetryRecordKey(String key, String taskId) {
        this.key = key;
        this.taskId = taskId;
    }

    /**
     * Creates the vehicle part.
     *
     * @param vehicleId the vehicle id
     * @param messageId the message id
     * @return the string
     */
    public static String createVehiclePart(String vehicleId, String messageId) {
        return (vehicleId + DMAConstants.SEMI_COLON + messageId);
    }

    /**
     * getMapKey().
     *
     * @param serviceName serviceName
     * @param taskId taskId
     * @return String
     */
    public static String getMapKey(String serviceName, String taskId) {
        StringBuilder regexBuilder = new StringBuilder().append(DMAConstants.RETRY_MESSAGEID).append(DMAConstants.COLON)
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
     * Gets the task ID.
     *
     * @return the task ID
     */
    public String getTaskID() {
        return taskId;
    }

    /**
     * Sets the task id.
     *
     * @param taskId the new task id
     */
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    /**
     * Convert from.
     *
     * @param key the key
     * @return the retry record key
     */
    @Override
    public RetryRecordKey convertFrom(String key) {
        String[] keySplit = key.split(DMAConstants.COLON);
        return new RetryRecordKey(keySplit[1], keySplit[0]);
    }

    /**
     * Convert to string.
     *
     * @return the string
     */
    @Override
    public String convertToString() {
        return taskId + DMAConstants.COLON + key;
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
        result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
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
        RetryRecordKey other = (RetryRecordKey) obj;
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        if (taskId == null) {
            if (other.taskId != null) {
                return false;
            }
        } else if (!taskId.equals(other.taskId)) {
            return false;
        }
        return true;
    }

}