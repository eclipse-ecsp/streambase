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

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;


/**
 *  interface DeviceMessageHandler.
 */
public interface DeviceMessageHandler {

    /**
     * Handle.
     *
     * @param key the key
     * @param value the value
     */
    public void handle(IgniteKey<?> key, DeviceMessage value);

    /**
     * Sets the next handler.
     *
     * @param handler the new next handler
     */
    public void setNextHandler(DeviceMessageHandler handler);

    /**
     * Sets the stream processing context.
     *
     * @param ctx the ctx
     */
    default void setStreamProcessingContext(StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctx) {
        // empty default implementation
    }

    /**
     * Close.
     */
    public void close();

}
