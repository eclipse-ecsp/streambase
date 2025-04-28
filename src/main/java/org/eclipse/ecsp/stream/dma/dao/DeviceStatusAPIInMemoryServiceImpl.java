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

package org.eclipse.ecsp.stream.dma.dao;

import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0.ConnectionStatus;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A DAO layer for in-memory connection status cache.
 * Check {@link DeviceStatusDaoInMemoryCache} for details on the in-memory HashMap data structure.
 *
 * @author hbadshah
 */
@Service
public class DeviceStatusAPIInMemoryServiceImpl implements DeviceStatusAPIInMemoryService {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceStatusAPIInMemoryServiceImpl.class);
    
    /** The device status dao. */
    @Autowired
    private DeviceStatusDaoInMemoryCache deviceStatusDao;

    /**
     * Gets the.
     *
     * @param vehicleId the vehicle id
     * @return the vehicle id device id status
     */
    @Override
    public VehicleIdDeviceIdStatus get(String vehicleId) {
        DeviceStatusKey key = new DeviceStatusKey(vehicleId);
        //Get mapping for this vehicleId from in-memory cache.
        VehicleIdDeviceIdStatus mapping = deviceStatusDao.get(key);
        if (mapping != null) {
            logger.debug("Mapping {} found for vehicleId {}", mapping.toString(), vehicleId);
            return mapping;
        }
        logger.debug("No mapping found for vehicleId: {} in in-memory cache.", vehicleId);
        return null;
    }

    /**
     * In this method, both put and update operations have been handled.
     * If mapping found, update the connection status of the device in the
     * mapping and put the mapping in in-memory. Else, create and add a
     * new mapping for this vehicleId and put it in in-memory.
     *
     * @param vehicleId vehicleId
     * @param deviceId the device id
     * @param connectionStatus the connection status
     */
    @Override
    public void update(String vehicleId, String deviceId, String connectionStatus) {
        DeviceStatusKey key = new DeviceStatusKey(vehicleId);
        //Get mapping for this vehicleId from in-memory cache.
        VehicleIdDeviceIdStatus mapping = deviceStatusDao.get(key);
        if (mapping != null) {
            mapping.addDeviceId(deviceId, connectionStatus);
            //put the mapping in in-memory cache for this vehicleId
            deviceStatusDao.put(key, mapping, InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        } else {
            ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
            map.put(deviceId, ConnectionStatus.valueOf(connectionStatus));
            deviceStatusDao.putIfAbsent(key, new VehicleIdDeviceIdStatus(Version.V1_0, map), Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        }
        logger.debug("Updated connection status for vehicleId {} and deviceId "
                + "{} as {} in in-memory.", vehicleId, deviceId, connectionStatus);
    }

    /**
     * In case a requirement comes for DMA to hit the API forcefully for some scenario,
     * then to handle such a case, implementation can be
     * provided in this method.
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     */
    @Override
    public void forceGet(String vehicleId, String deviceId) {
        // method to force get status
    }
}
