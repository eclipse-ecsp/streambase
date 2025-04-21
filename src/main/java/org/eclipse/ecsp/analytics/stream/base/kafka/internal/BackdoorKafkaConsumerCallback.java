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

package org.eclipse.ecsp.analytics.stream.base.kafka.internal;

import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;

import java.util.Optional;

/**
 * Stream Processors that needs to access records from kafka back door consumer,
 * should implement this interface. It acts a call back
 * mechanism.
 *
 * @author avadakkootko
 */
public interface BackdoorKafkaConsumerCallback {

    /**
     * Process.
     *
     * @param key the key
     * @param value the value
     * @param meta the meta
     */
    public void process(IgniteKey<?> key, IgniteEvent value, OffsetMetadata meta);

    /**
     * Gets the committable offset.
     *
     * @return the committable offset
     */
    public Optional<OffsetMetadata> getCommittableOffset();

    /**
     * Close.
     */
    public default void close() {
        // Provide use case specific implementation.
    }
}
