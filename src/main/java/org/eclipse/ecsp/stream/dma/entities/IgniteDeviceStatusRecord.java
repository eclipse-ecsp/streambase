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

package org.eclipse.ecsp.stream.dma.entities;

import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;

/**
 * IgniteDeviceStatusRecord is a POJO class that holds an Ignite key and an Ignite event.
 * This object is forwarded to the service when a device connection status changes to ACTIVE/INACTIVE.
 *
 * @author pijantkar
 */
public class IgniteDeviceStatusRecord {

    /**
     * The Ignite event associated with the device status.
     */
    private IgniteEvent event;

    /**
     * The Ignite key associated with the device status.
     */
    private IgniteKey<?> key;

    /**
     * Retrieves the Ignite event associated with the device status.
     *
     * @return The Ignite event.
     */
    public IgniteEvent getEvent() {
        return event;
    }

    /**
     * Sets the Ignite event associated with the device status.
     *
     * @param event The Ignite event to set.
     */
    public void setEvent(IgniteEvent event) {
        this.event = event;
    }

    /**
     * Retrieves the Ignite key associated with the device status.
     *
     * @return The Ignite key.
     */
    public IgniteKey<?> getKey() {
        return key;
    }

    /**
     * Sets the Ignite key associated with the device status.
     *
     * @param key The Ignite key to set.
     */
    public void setKey(IgniteKey<?> key) {
        this.key = key;
    }

    /**
     * Converts the IgniteDeviceStatusRecord object to a string representation.
     *
     * @return A string representation of the IgniteDeviceStatusRecord object.
     */
    @Override
    public String toString() {
        return "IgniteDeviceStatusRecord{"
                + "event=" + event
                + ", key=" + key
                + "}";
    }
}