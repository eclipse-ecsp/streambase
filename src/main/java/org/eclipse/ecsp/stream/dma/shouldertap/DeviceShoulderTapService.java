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
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Map;


/**
 * DeviceShoulderTapService handles device shoulder tap wake up request.
 * This interacts with DeviceShoulderTapRetryHandler to
 * register/deregister device for retries.
 *
 * @author KJalawadi
 */

@Component
@Scope("prototype")
public class DeviceShoulderTapService {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceShoulderTapService.class);

    /** The device shoulder tap retry handler. */
    @Autowired
    private DeviceShoulderTapRetryHandler deviceShoulderTapRetryHandler;

    /** The shoulder tap enabled. */
    @Value("${dma.shoulder.tap.enabled:false}")
    private boolean shoulderTapEnabled;

    /**
     * When device is inactive and shoulder tap enabled we need to wake up a device.
     *
     * @param requestId requestId
     * @param vehicleId vehicleId
     * @param serviceName serviceName
     * @param igniteKey igniteKey
     * @param igniteEvent igniteEvent
     * @param extraParameters extraParameters
     * @return boolean
     */
    public boolean wakeUpDevice(String requestId, String vehicleId, String serviceName, IgniteKey<?> igniteKey,
            DeviceMessage igniteEvent, Map<String, Object> extraParameters) {
        if (shoulderTapEnabled) {
            boolean registered = registerWithDeviceShoulderTapRetryHandler(igniteKey, igniteEvent, extraParameters);
            logger.debug("Registered device with DeviceShoulderTapRetryHandler: requestId={} vehicleId={} "
                    + "serviceName={} registered={}", requestId, vehicleId, serviceName, registered);
            return registered;
        } else {
            logger.trace("Shoulder tap has been disabled for this service. Unable to wakeup device");
            return false;
        }

    }

    /**
     * When device is active we need to stop sending shoulder tap messages.
     *
     * @param vehicleId vehicleId
     * @param deviceId deviceId
     * @param serviceName serviceName
     */
    public void executeOnDeviceActiveStatus(String vehicleId, String deviceId, String serviceName) {
        boolean deregistered = false;
        if (shoulderTapEnabled) {
            deregistered = deviceShoulderTapRetryHandler.deregisterDevice(vehicleId);
            logger.debug("Deregistered device with DeviceShoulderTapRetryHandler: deviceId={} serviceName={} "
                    + "deregistered={}", deviceId, serviceName, deregistered);
        } else {
            logger.trace("Shoulder tap has been disabled for this service. No action performed");
        }
    }

    /**
     * Register with device shoulder tap retry handler.
     *
     * @param key the key
     * @param deviceMessage the device message
     * @param extraParameters the extra parameters
     * @return true, if successful
     */
    private boolean registerWithDeviceShoulderTapRetryHandler(IgniteKey<?> key, DeviceMessage deviceMessage,
            Map<String, Object> extraParameters) {
        boolean registered = false;
        registered = deviceShoulderTapRetryHandler.registerDevice(key, deviceMessage, extraParameters);
        return registered;
    }

    /**
     * Sets the stream processing context.
     *
     * @param spc the spc
     */
    public void setStreamProcessingContext(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        deviceShoulderTapRetryHandler.setStreamProcessingContext(spc);
    }

    /**
     * setup().
     *
     * @param taskId taskId
     */
    public void setup(String taskId) {
        if (shoulderTapEnabled) {
            deviceShoulderTapRetryHandler.setup(taskId);
        } else {
            logger.warn("Shoulder tap has been disabled for this service. Not setting up ShoulderTapRetryHandler!!!");
        }
    }

    /**
     * Sets the shoulder tap enabled.
     *
     * @param shoulderTapEnabled the new shoulder tap enabled
     */
    protected void setShoulderTapEnabled(boolean shoulderTapEnabled) {
        this.shoulderTapEnabled = shoulderTapEnabled;
    }

}
