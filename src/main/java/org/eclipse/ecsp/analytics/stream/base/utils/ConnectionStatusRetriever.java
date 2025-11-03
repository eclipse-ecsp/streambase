/*
 *
 *
 * ******************************************************************************
 *
 * Copyright (c) 2023-24 Harman International
 *
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 *
 * distributed under the License is distributed on an "AS IS" BASIS,
 *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and
 *
 * limitations under the License.
 *
 *
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * *******************************************************************************
 *
 *
 */

package org.eclipse.ecsp.analytics.stream.base.utils;

import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;

import java.util.Optional;

/**
 * CR-4570 DMA should expose an interface for services to retrieve connection status from an API.
 * Services can implement this interface and plug-in its implementation configuration to provide
 * their own logic to call the API and fetch device connection status.
 *
 * @author HBadshah
 */
public interface ConnectionStatusRetriever {

    /**
     * Gets the connection status data from an API.
     * 
     * @param requestId The request ID for tracking purposes.
     * @param vehicleId The vehicle ID associated with the device.
     * @param deviceId The device ID for which the connection status is to be retrieved.
     * @param subService An optional sub-service identifier.
     * @return The connection status data for the specified device.
     */
    public VehicleIdDeviceIdStatus getConnectionStatusData(String requestId, String vehicleId,
            String deviceId, Optional<String> subService);
}
