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

package org.eclipse.ecsp.analytics.stream.base.parser;

import java.util.Map;

/**
 *         An interface for services to implement when they want the connection
 *         status of the devices to be retrieved through a third party
 *         API. DMA will hit the API and get the response JSON.
 *         Services then have to provide an implementation of below interface to
 *         extract the connection status information from the response and return it to DMA.
 *         Parsing of this response has been avoided in DMA as response may vary
 *         from service to service, hence to avoid any specific kind
 *         of parsing specific to any particular service.
 *         If a service wants DMA to get connection status data through API,
 *         and yet it did not provide implementation of this interface,
 *         then by default null will be returned and no connection status data for
 *         devices will be stored in the in-memory of DMA.
 *
 *
 * @author hbadshah
 */
public interface DeviceConnectionStatusParser {
    
    /**
     * Gets the connection status.
     *
     * @param responseData the response data
     * @return the connection status
     */
    public default String getConnectionStatus(Map<String, Object> responseData) {
        return null;
    }
}
