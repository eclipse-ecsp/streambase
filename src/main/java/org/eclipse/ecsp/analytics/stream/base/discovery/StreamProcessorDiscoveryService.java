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

package org.eclipse.ecsp.analytics.stream.base.discovery;

import org.eclipse.ecsp.analytics.stream.base.StreamProcessor;
import java.util.List;
import java.util.Properties;

/**
 * Abstraction for processor discovery.
 *
 * @author ssasidharan
 * @param <KIn> the type parameter for incoming key.
 * @param <VIn> the type parameter for incoming value.
 * @param <KOut> the type parameter for outgoing key.
 * @param <VOut> the type parameter for outgoing value.
 */
public interface StreamProcessorDiscoveryService<KIn, VIn, KOut, VOut> {
    
    /**
     * Discover processors.
     *
     * @return the list
     */
    List<StreamProcessor<KIn, VIn, KOut, VOut>> discoverProcessors();

    /**
     * Sets the properties.
     *
     * @param props the new properties
     */
    default void setProperties(Properties props) {

    }

}
