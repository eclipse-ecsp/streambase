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

import org.eclipse.ecsp.entities.IgniteEventImpl;

import java.util.Optional;


/**
 * class RetryTestEvent extends IgniteEventImpl.
 */
public class RetryFallbackTtlTestEvent extends IgniteEventImpl {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new retry test event.
     */
    public RetryFallbackTtlTestEvent() {
        // Default Constructor
    }

    /**
     * Gets the event id.
     *
     * @return the event id
     */
    @Override
    public String getEventId() {
        return "test_Speed";
    }

    /**
     * Gets the target device id.
     *
     * @return the target device id
     */
    @Override
    public Optional<String> getTargetDeviceId() {
        return Optional.of("Device12345");
    }

}