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

package org.eclipse.ecsp.analytics.stream.base;

import java.util.Properties;

/**
 * Launches stream processing specific to the stream
 * processing system being used (for ex Kafka Streams or Samza).
 *
 * @author ssasidharan
 */
public interface LauncherProvider {

    /**
     * Launches the stream processing system with the provided properties.
     *
     * @param props the properties to configure the stream processing system
     */
    void launch(Properties props);

    /**
     * Terminates the stream processing system and releases resources.
     */
    void terminate();

    /**
     * Terminates the streams with a timeout for unit test cases.
     */
    void terminateStreamsWithTimeout();
}
