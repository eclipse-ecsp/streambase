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

package org.eclipse.ecsp.stream.dma.shouldertap;

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;

import java.util.Map;


/**
 * {@link ShoulderTapInvokerVehicleNotificationImpl} implements {@link DeviceShoulderTapInvoker}.
 *
 * @author KJalawadi
 */
class ShoulderTapInvokerVehicleNotificationImpl implements DeviceShoulderTapInvoker {

    /**
     * Send wake up message.
     *
     * @param requestId the request id
     * @param vehicleId the vehicle id
     * @param extraParameters the extra parameters
     * @param spc the spc
     * @return true, if successful
     */
    @Override
    public boolean sendWakeUpMessage(String requestId, String vehicleId, 
        Map<String, Object> extraParameters, StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        return false;
    }
}
