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

package org.eclipse.ecsp.stream.dma.dao;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidServiceNameException;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * DeviceStatusServiceImpl interacts with the DAO layer. Whenever querying for device status the
 * input deviceId should be of the format
 * DEVICE_STATUS_{@code <}SERVICE{@code >}_{@code <}deviceID{@code >}
 *
 * @author avadakkootko
 */
@Component("deviceStatusServiceImpl")
public class DeviceStatusServiceImpl implements DeviceStatusService<ConcurrentHashSet<String>> {

    /**
     * Logger instance for logging messages.
     */
    private static IgniteLogger logger =
            IgniteLoggerFactory.getLogger(DeviceStatusServiceImpl.class);

    /**
     * Map containing sub-service names to their corresponding Redis parent keys.
     */
    private Map<String, String> subServiceToParentKeyMapping = new HashMap<>();

    /**
     * Redis parent key for device status data when no sub-services are present.
     */
    private String mapParentKey = null;

    /**
     * DAO implementation for accessing device status data.
     */
    @Qualifier("deviceStatusDaoImpl")
    @Autowired
    private DeviceConnStatusDao<VehicleIdDeviceIdMapping> deviceStatusDao;

    /**
     * Utility class for device status operations.
     */
    @Autowired
    private DeviceStatusUtil deviceStatusUtil;

    /**
     * Name of the service.
     */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /**
     * Comma-separated list of sub-services.
     */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    private String subServices;

    /**
     * Initializes the service by validating the service name and setting up sub-service mappings.
     */
    @PostConstruct
    public void initKey() {
        deviceStatusUtil.validateServiceName(serviceName);
        subServiceToParentKeyMapping = deviceStatusUtil.getSubServiceToParentKeyMapping();
        mapParentKey = deviceStatusUtil.getMapParentKeyWithServiceName(serviceName);
    }

    /**
     * Retrieves device IDs for a given vehicle ID from the in-memory cache or Redis.
     *
     * @param key The key representing the vehicle ID.
     * @param subService Optional sub-service identifier.
     * @return A set of device IDs associated with the vehicle ID.
     */
    @Override
    public ConcurrentHashSet<String> get(String key, Optional<String> subService) {
        DeviceStatusKey deviceStatusKey = null;
        String redisMapKey = mapParentKey;
        if (subService.isPresent()) {
            String keyWithSubService = key + DMAConstants.SEMI_COLON + subService.get();
            deviceStatusKey = new DeviceStatusKey(keyWithSubService);
            redisMapKey = StringUtils.isEmpty(redisMapKey)
                    ? subServiceToParentKeyMapping.get(subService.get())
                    : redisMapKey;
        } else {
            deviceStatusKey = new DeviceStatusKey(key);
        }
        ConcurrentHashSet<String> deviceIds = null;
        VehicleIdDeviceIdMapping mapping = deviceStatusDao.get(deviceStatusKey);
        if (mapping != null) {
            logger.debug("Received VehicleIdDeviceIdMapping from in-memory cache as {}",
                    mapping.toString());
            deviceIds = mapping.getDeviceIds();
            if (deviceIds == null || deviceIds.isEmpty()) {
                logger.warn(
                        "DeviceId not present in VehicleIdDeviceIdMapping hence forcing it to query from redis- "
                                + "mapParentKey {} , deviceStatusKey-key {} ,deviceStatusKey {}",
                        mapParentKey, deviceStatusKey.getKey(), deviceStatusKey);
                deviceIds = forceGet(redisMapKey, new DeviceStatusKey(key));
                updateInMemoryMap(deviceStatusKey, deviceIds, mapping);
            }
        } else {
            // Force to read from redis if vehicle Inactive
            deviceIds = forceGet(redisMapKey, new DeviceStatusKey(key));
            updateInMemoryMap(deviceStatusKey, deviceIds, new VehicleIdDeviceIdMapping());
        }
        logger.debug("DeviceId for VehicleId key {} is {}", key, deviceIds);
        return deviceIds;
    }

    /**
     * Update in memory map.
     *
     * @param deviceStatusKey The key representing the vehicle ID.
     * @param deviceIds The set of device IDs to update.
     * @param mapping The mapping object to update.
     */
    private void updateInMemoryMap(DeviceStatusKey deviceStatusKey,
            ConcurrentHashSet<String> deviceIds, VehicleIdDeviceIdMapping mapping) {
        if (deviceIds != null) {
            mapping.setDeviceIds(deviceIds);
            // Put the data in in-memory map
            deviceStatusDao.put(deviceStatusKey, mapping, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        }
    }

    /**
     * Put.
     *
     * @param key the key
     * @param deviceIds the device ids
     * @param mutationId the mutation id
     * @param subService the sub service
     */
    @Override
    public void put(String key, ConcurrentHashSet<String> deviceIds,
            Optional<MutationId> mutationId, Optional<String> subService) {
        DeviceStatusKey deviceStatusKey = null;
        String redisMapKey = mapParentKey;
        if (subService.isPresent()) {
            String keyWithSubService = key + DMAConstants.SEMI_COLON + subService.get();
            deviceStatusKey = new DeviceStatusKey(keyWithSubService);
            redisMapKey = StringUtils.isEmpty(redisMapKey)
                    ? subServiceToParentKeyMapping.get(subService.get())
                    : redisMapKey;
        } else {
            deviceStatusKey = new DeviceStatusKey(key);
        }
        deviceStatusDao.putIfAbsent(deviceStatusKey,
                new VehicleIdDeviceIdMapping(Version.V1_0, deviceIds), Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        VehicleIdDeviceIdMapping mapping = deviceStatusDao.get(deviceStatusKey);
        mapping.setDeviceIds(deviceIds);
        deviceStatusDao.putToMap(redisMapKey, deviceStatusKey, mapping, mutationId,
                InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        logger.info("Key {}, Value {} updated in cache", key, mapping.toString());
    }

    /**
     * Delete operation can be performed at key level or for a granular level of deviceId by passing
     * and optional argument deviceId.
     *
     * @param key the key
     * @param deviceId the device id
     * @param mutationId the mutation id
     * @param subService the sub service
     */
    @Override
    public void delete(String key, String deviceId, Optional<MutationId> mutationId,
            Optional<String> subService) {
        String vehicleIdDeviceIdStatusParentKey = mapParentKey;
        if (subService.isPresent()) {
            key = key + DMAConstants.SEMI_COLON + subService.get();
            vehicleIdDeviceIdStatusParentKey = subServiceToParentKeyMapping.get(subService.get());
        }
        DeviceStatusKey deviceStatusKey = new DeviceStatusKey(key);
        logger.debug("In delete Mapping for key {}", deviceStatusKey.convertToString());
        VehicleIdDeviceIdMapping mapping = deviceStatusDao.get(deviceStatusKey);
        if (mapping == null) {
            logger.warn("No VehicleIdDeviceIdMapping instance found to delete for key {}", key);
            return;
        }
        logger.debug(
                "Attempting to delete Device Status in cache for key {}, deviceId {}, with mapping {}",
                key, deviceId, mapping.toString());
        if (mapping.deleteDeviceId(deviceId)) {
            logger.info("DeviceID {} deleted for key {}, from mapping instance {}", deviceId, key,
                    mapping.toString());
            if (mapping.getDeviceIds().isEmpty()) {
                deleteKey(key, mutationId);
            } else {
                deviceStatusDao.putToMap(vehicleIdDeviceIdStatusParentKey, deviceStatusKey,
                        mapping, mutationId,
                        InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
            }
        }
    }

    /**
     * Get the latest TopicPartition and offset value of Kafka Consumer from Redis.
     *
     * @param serviceName the service name
     * @return the offset metadata
     */
    @Override
    public Optional<OffsetMetadata> getOffsetMetadata(String serviceName) {
        return deviceStatusDao.getOffsetMetadata(serviceName);
    }

    /**
     * Updates the connection status of a specific device for a given vehicle ID.
     *
     * @param vehicleId The vehicle ID.
     * @param targetDeviceId The target device ID.
     * @param connectionStatus The new connection status of the device.
     */
    @Override
    public void update(String vehicleId, String targetDeviceId, String connectionStatus, Optional<String> subService) {
        DeviceStatusKey key = new DeviceStatusKey(vehicleId);
        // Get mapping for this vehicleId from in-memory cache.
        VehicleIdDeviceIdMapping mapping = deviceStatusDao.get(key);
        if (mapping != null) {
            mapping.addDeviceId(targetDeviceId);
            // put the mapping in in-memory cache for this vehicleId
            deviceStatusDao.put(key, mapping, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        } else {
            ConcurrentHashSet<String> map = new ConcurrentHashSet<>();
            map.add(targetDeviceId);
            deviceStatusDao.putIfAbsent(key, new VehicleIdDeviceIdMapping(Version.V1_0, map),
                    Optional.empty(), InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        }
        logger.debug("Updated in memory for vehicleId {} and deviceId {} .", vehicleId,
                targetDeviceId, connectionStatus);
    }

    /**
     * Deletes all device IDs for a given vehicle ID from the in-memory cache or Redis.
     *
     * @param vehicleId The vehicle ID.
     * @param mutationId Optional mutation identifier.
     */
    @Override
    public void deleteKey(String vehicleId, Optional<MutationId> mutationId) {
        DeviceStatusKey deviceStatusKey = new DeviceStatusKey(vehicleId);
        logger.debug("Attempting to Delete Device Status in cache for key {}", vehicleId);
        if (!subServiceToParentKeyMapping.isEmpty()) {
            String[] arr = vehicleId.split(":");
            String subService = arr[arr.length - 1];
            deviceStatusDao.deleteFromMap(subServiceToParentKeyMapping.get(subService),
                    deviceStatusKey, mutationId,
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        } else {
            deviceStatusDao.deleteFromMap(mapParentKey, deviceStatusKey, mutationId,
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        }
    }

    /**
     * Retrieves device IDs directly from Redis, bypassing the in-memory cache.
     *
     * @param subServiceOpt Optional sub-service identifier.
     * @param key The key representing the vehicle ID.
     * @return A set of device IDs associated with the vehicle ID.
     */
    @Override
    public ConcurrentHashSet<String> forceGet(String key, Optional<String> subServiceOpt) {
        if (subServiceOpt.isPresent()) {
            String subService = subServiceOpt.get();
            if (StringUtils.isEmpty(subServiceToParentKeyMapping.get(subService))) {
                logger.error(
                        "No vehicleDeviceID mapping key found for subservice {} in "
                                + "subServiceToParentKeyMapping : {}",
                        subService, subServiceToParentKeyMapping);
                return new ConcurrentHashSet<>();
            }
            String vehicleIdDeviceIdStatusParentKey = subServiceToParentKeyMapping.get(subService);
            return forceGet(vehicleIdDeviceIdStatusParentKey, new DeviceStatusKey(key));
        } else {
            return forceGet(mapParentKey, new DeviceStatusKey(key));
        }
    }

    /**
     * Retrieves device IDs directly from Redis for a specific map key and entry key.
     *
     * @param mapKey The Redis map key.
     * @param mapEntryKey The Redis map entry key.
     * @return A set of device IDs associated with the map entry key.
     */
    private ConcurrentHashSet<String> forceGet(String mapKey, DeviceStatusKey mapEntryKey) {
        ConcurrentHashSet<String> deviceIds = null;
        VehicleIdDeviceIdMapping vehicleIdDeviceIdMapping =
                deviceStatusDao.forceGet(mapKey, mapEntryKey);
        if (vehicleIdDeviceIdMapping != null) {
            deviceIds = vehicleIdDeviceIdMapping.getDeviceIds();
        }
        logger.debug("Force get for mapParentKey {}, key {} returned deviceIds {}", mapKey,
                mapEntryKey.convertToString(), deviceIds);
        return deviceIds;
    }

    /**
     * Sets the sub-services for the service.
     *
     * @param subServices The comma-separated list of sub-services.
     */
    public void setSubServices(String subServices) {
        this.subServices = subServices;
    }
}
