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

package org.eclipse.ecsp.stream.dma.dao.key;

import org.eclipse.ecsp.analytics.stream.base.stores.CacheKeyConverter;
import org.eclipse.ecsp.entities.dma.AbstractDeviceStatusKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;


/**
 * DeviceStatusKey extends AbstractDeviceStatusKey implements CacheKeyConverter.
 */
public class DeviceStatusKey extends AbstractDeviceStatusKey implements CacheKeyConverter<DeviceStatusKey> {

    /**
     * Instantiates a new device status key.
     */
    public DeviceStatusKey() {
    }

    /**
     * Instantiates a new device status key.
     *
     * @param key the key
     */
    public DeviceStatusKey(String key) {
        super(key);
    }

    /**
     * getMapKey().
     *
     * @param serviceName serviceName
     * @return String serviceName
     */
    public static String getMapKey(String serviceName) {
        StringBuilder regexBuilder = new StringBuilder()
                .append(DMAConstants.VEHICLE_DEVICE_MAPPING).append(serviceName);
        return regexBuilder.toString();
    }

    /**
     * Convert from.
     *
     * @param key the key
     * @return the device status key
     */
    @Override
    public DeviceStatusKey convertFrom(String key) {
        return new DeviceStatusKey(key);
    }
}