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

package org.eclipse.ecsp.stream.dma.config;


/**
 * Service needs to implement this use case to provide EventConfig for
 * specific eventId. Required for DMA to know whether to enable the use case for
 * this eventId or not.
 *
 * @author hbadshah
 */
public interface EventConfigProvider {
    
    /** The default event config. */
    public final EventConfig DEFAULT_EVENT_CONFIG = new DefaultEventConfig();

    /**
     * Gets the event config.
     *
     * @param eventId the event id
     * @return the event config
     */
    /*
     * @returns EventConfig
     */
    public EventConfig getEventConfig(String eventId);

    /**
     * Gets the default event config.
     *
     * @param eventId the event id
     * @return the default event config
     */
    default EventConfig getDefaultEventConfig(String eventId) {
        return DEFAULT_EVENT_CONFIG;
    }
}
