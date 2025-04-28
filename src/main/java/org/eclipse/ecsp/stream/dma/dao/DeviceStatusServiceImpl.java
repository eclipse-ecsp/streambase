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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * DeviceStatusServiceImpl interacts with the DAO layer.
 * Whenever querying for device status the input deviceId should be of the
 * format DEVICE_STATUS_{@code <}SERVICE{@code >}_{@code <}deviceID{@code >}
 *
 * @author avadakkootko
 */
@Service
public class DeviceStatusServiceImpl implements DeviceStatusService {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceStatusServiceImpl.class);
    
    /** The sub service to parent key mapping. */
    /*
     * Below map will contain sub-service's name to its corresponding redis parent key's name.
     * As part of RTC 355420.
     */
    private Map<String, String> subServiceToParentKeyMapping = new HashMap<>();
    
    /** The map parent key. */
    /*
     * In case of no sub-services, there will be just one redis parent key for device status data/map in redis.
     * Below variable will hold that parent key's name.
     * As part of RTC 355420.
     */
    private String mapParentKey = null;
    
    /** The device status dao. */
    @Autowired
    private DeviceConnStatusDAO deviceStatusDao;



    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /** The sub services. */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    private String subServices;

    /**
     * Accepts vehicleId as an argument and returns the deviceId.
     * Key should be of the format VEHICLE_DEVICE_MAPPING_service.name_vehicleId.
     * If there is no value present for above format of the key, then it implies
     * status is INACTIVE.
     * Optional subService: RTC 355420. If there exist sub-services
     * under one service, then read operation for device status should be
     * performed at sub-service level rather than just on service level.
     *
     * @param key the key
     * @param subService the sub service
     * @return the concurrent hash set
     */
    @Override
    public ConcurrentHashSet<String> get(String key, Optional<String> subService) {

        DeviceStatusKey deviceStatusKey = null;
        String redisMapKey = mapParentKey;
        if (subService.isPresent()) {
            String keyWithSubService = key + DMAConstants.SEMI_COLON + subService.get();
            deviceStatusKey = new DeviceStatusKey(keyWithSubService);
            redisMapKey = StringUtils.isEmpty(redisMapKey) ? subServiceToParentKeyMapping.get(subService.get()) 
                    : redisMapKey;
        } else {
            deviceStatusKey = new DeviceStatusKey(key);
        }
        ConcurrentHashSet<String> deviceIds = null;
        VehicleIdDeviceIdMapping mapping = deviceStatusDao.get(deviceStatusKey);
        if (mapping != null) {
            logger.debug("Received VehicleIdDeviceIdMapping from in-memory cache as {}", mapping.toString());
            deviceIds = mapping.getDeviceIds();
            if (deviceIds == null || deviceIds.isEmpty()) {
                logger.warn("DeviceId not present in VehicleIdDeviceIdMapping hence forcing it to query from redis- "
                        + "mapParentKey {} , deviceStatusKey-key {} ,deviceStatusKey {}", mapParentKey, 
                        deviceStatusKey.getKey(), deviceStatusKey);
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
     * @param deviceStatusKey the device status key
     * @param deviceIds the device ids
     * @param mapping the mapping
     */
    private void updateInMemoryMap(DeviceStatusKey deviceStatusKey, ConcurrentHashSet<String> deviceIds, 
            VehicleIdDeviceIdMapping mapping) {
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
            redisMapKey = StringUtils.isEmpty(redisMapKey) ? subServiceToParentKeyMapping.get(subService.get()) 
                    : redisMapKey;
        } else {
            deviceStatusKey = new DeviceStatusKey(key);
        }   
        deviceStatusDao.putIfAbsent(deviceStatusKey, new VehicleIdDeviceIdMapping(Version.V1_0, deviceIds), 
                Optional.empty(), InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        VehicleIdDeviceIdMapping mapping = deviceStatusDao.get(deviceStatusKey);
        mapping.setDeviceIds(deviceIds);
        deviceStatusDao.putToMap(redisMapKey, deviceStatusKey, mapping, mutationId,
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        logger.info("Key {}, Value {} updated in cache", key, mapping.toString());
    }

    /**
     * Delete operation can be performed at key level or for a granular level of
     * deviceId by passing and optional argument deviceId.
     *
     * @param key the key
     * @param deviceId the device id
     * @param mutationId the mutation id
     * @param subService the sub service
     */
    @Override
    public void delete(String key, String deviceId, Optional<MutationId> mutationId, Optional<String> subService) {
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
        logger.debug("Attempting to delete Device Status in cache for key {}, deviceId {}, with mapping {}", 
                key, deviceId, mapping.toString());
        if (mapping.deleteDeviceId(deviceId)) {
            logger.info("DeviceID {} deleted for key {}, from mapping instance {}", deviceId, key, mapping.toString());
            if (mapping.getDeviceIds().isEmpty()) {
                deleteKey(key, mutationId);
            } else {
                deviceStatusDao.putToMap(vehicleIdDeviceIdStatusParentKey, deviceStatusKey, mapping, mutationId,
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
     * Delete key.
     *
     * @param vehicleId the vehicle id
     * @param mutationId the mutation id
     */
    @Override
    public void deleteKey(String vehicleId, Optional<MutationId> mutationId) {
        DeviceStatusKey deviceStatusKey = new DeviceStatusKey(vehicleId);
        logger.debug("Attempting to Delete Device Status in cache for key {}", vehicleId);
        if (subServiceToParentKeyMapping.size() > 0) {
            String[] arr = vehicleId.split(":");
            String subService = arr[arr.length - 1];
            deviceStatusDao.deleteFromMap(subServiceToParentKeyMapping.get(subService), deviceStatusKey,
                    mutationId, InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        } else {
            deviceStatusDao.deleteFromMap(mapParentKey, deviceStatusKey, mutationId,
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        }
    }

    /**
     * initKey().
     */
    @PostConstruct
    public void initKey() {
        if (StringUtils.isEmpty(serviceName)) {
            throw new InvalidServiceNameException("Service name cannot be empty for DeviceStatusService");
        }
        if (StringUtils.isNotEmpty(subServices)) {
            List<String> subServicesList = Arrays.asList(subServices.split(","));
            for (String subService : subServicesList) {
                subServiceToParentKeyMapping.put(subService,
                        DMAConstants.VEHICLE_DEVICE_MAPPING + subService);
            }
            logger.info("Sub-Service to VEHICLE_DEVICE_MAPPING initialized as {}", subServiceToParentKeyMapping);
        } else {
            mapParentKey = DMAConstants.VEHICLE_DEVICE_MAPPING + serviceName;
        }
    }

    /**
     * Force get.
     *
     * @param vehicleId the vehicle id
     * @param subServiceOpt the sub service opt
     * @return the concurrent hash set
     */
    @Override
    public ConcurrentHashSet<String> forceGet(String vehicleId, Optional<String> subServiceOpt) {
        String key = vehicleId;
        if (subServiceOpt.isPresent()) {
            String subService = subServiceOpt.get();
            if (StringUtils.isEmpty(subServiceToParentKeyMapping.get(subService))) {
                logger.error("No vehicleDeviceID mapping key found for subservice {} in "
                        + "subServiceToParentKeyMapping : {}", subService, subServiceToParentKeyMapping);
                return new ConcurrentHashSet<>();
            }
            String vehicleIdDeviceIdStatusParentKey = subServiceToParentKeyMapping.get(subService);
            return forceGet(vehicleIdDeviceIdStatusParentKey, new DeviceStatusKey(key));
        } else {
            return forceGet(mapParentKey, new DeviceStatusKey(key));
        }
    }

    /**
     * Force get.
     *
     * @param mapKey the map key
     * @param mapEntryKey the map entry key
     * @return the concurrent hash set
     */
    private ConcurrentHashSet<String> forceGet(String mapKey, DeviceStatusKey mapEntryKey) {
        ConcurrentHashSet<String> deviceIds = null;
        VehicleIdDeviceIdMapping vehicleIdDeviceIdMapping = deviceStatusDao.forceGet(mapKey, mapEntryKey);
        if (vehicleIdDeviceIdMapping != null) {
            deviceIds = vehicleIdDeviceIdMapping.getDeviceIds();
        }
        logger.debug("Force get for mapParentKey {}, key {} retured deviceIds {}", mapKey, 
                mapEntryKey.convertToString(), deviceIds);
        return deviceIds;
    }

    /**
     * Sets the sub services.
     *
     * @param subServices the new sub services
     */
    public void setSubServices(String subServices) {
        this.subServices = subServices;
    }
}