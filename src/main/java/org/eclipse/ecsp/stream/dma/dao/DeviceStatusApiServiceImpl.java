package org.eclipse.ecsp.stream.dma.dao;

import jakarta.annotation.PostConstruct;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0.ConnectionStatus;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of the DeviceStatusService interface for managing in-memory connection status
 * data.
 */

@Component("deviceStatusApiServiceImpl")
public class DeviceStatusApiServiceImpl implements DeviceStatusService<VehicleIdDeviceIdStatus> {

    /**
     * DAO for accessing in-memory connection status data.
     */
    @Qualifier("deviceConnStatusApiDaoImpl")
    @Autowired
    private DeviceConnStatusDao<VehicleIdDeviceIdStatus> deviceConnStatusDao;

    /**
     * DAO for accessing in-memory device connection data.
     */
    @Qualifier("deviceStatusDaoImpl")
    @Autowired
    private DeviceConnStatusDao<VehicleIdDeviceIdMapping> deviceConnDao;

    /**
     * Utility class for device status operations.
     */
    @Autowired
    private DeviceStatusUtil deviceStatusUtil;

    /**
     * Comma-separated list of sub-services.
     */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    private String subServices;

    /**
     * Logger instance for logging messages.
     */
    private static IgniteLogger logger =
            IgniteLoggerFactory.getLogger(DeviceStatusApiServiceImpl.class);

    /**
     * Name of the service.
     */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /**
     * Empty string value for default sub-service.
     */
    @Value("${" + PropertyNames.EMPTY_STRING + ":}")
    private String emptyString;

    /**
     * Map containing sub-service names to their corresponding Redis parent keys.
     */
    private Map<String, String> subServiceToParentKeyMapping = new HashMap<>();

    /**
     * Redis parent key for device status data when no sub-services are present.
     */
    private String mapParentKey = null;

    /**
     * Validates the service name and initializes sub-service mappings.
     */
    @PostConstruct
    public void validate() {
        deviceStatusUtil.validateServiceName(serviceName);
        subServiceToParentKeyMapping = deviceStatusUtil.getSubServiceToParentKeyMapping();
        mapParentKey = deviceStatusUtil.getMapParentKeyWithServiceName(serviceName);
    }

    /**
     * Retrieves connection status data from in-memory cache for a given vehicle ID.
     *
     * @param vehicleId Identifier of the vehicle.
     * @param subServiceOpt Optional sub-service identifier.
     * @return Connection status data for the vehicle.
     */
    @Override
    public VehicleIdDeviceIdStatus get(String vehicleId, Optional<String> subServiceOpt) {
        String subService = subServiceOpt.isPresent() ? subServiceOpt.get() : emptyString;
        logger.debug(
                "Fetching connection status from the in-memory for vehicleId: {}, with subService: {}",
                vehicleId, subService);
        DeviceStatusKey key = new DeviceStatusKey(vehicleId);
        // Get mapping for this vehicleId from in-memory cache.
        VehicleIdDeviceIdStatus mapping = deviceConnStatusDao.get(key);
        if (mapping != null) {
            logger.info("Mapping {} found for vehicleId {}", mapping.toString(), vehicleId);
            return mapping;
        }
        logger.info("No mapping found for vehicleId: {} in in-memory cache.", vehicleId);
        return null;
    }

    /**
     * In this method, both put and update operations have been handled. If mapping found, update
     * the connection status of the device in the mapping and put the mapping in in-memory. Else,
     * create and add a new mapping for this vehicleId and put it in in-memory.
     *
     * @param targetDeviceId Identifier of the target device.
     * @param connectionStatus New connection status of the device.
     */

    @Override
    public void update(String vehicleId, String targetDeviceId, String connectionStatus) {
        DeviceStatusKey key = new DeviceStatusKey(vehicleId);
        // Get mapping for this vehicleId from in-memory cache.
        VehicleIdDeviceIdStatus mapping = deviceConnStatusDao.get(key);
        if (mapping != null) {
            mapping.addDeviceId(targetDeviceId, connectionStatus);
            // put the mapping in in-memory cache for this vehicleId
            deviceConnStatusDao.put(key, mapping, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        } else {
            ConcurrentHashMap<String, ConnectionStatus> map = new ConcurrentHashMap<>();
            map.put(targetDeviceId, ConnectionStatus.valueOf(connectionStatus));
            deviceConnStatusDao.putIfAbsent(key, new VehicleIdDeviceIdStatus(Version.V1_0, map),
                    Optional.empty(), InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        }
        logger.debug(
                "Updated connection status for vehicleId {} and deviceId {} as {} in in-memory.",
                vehicleId, targetDeviceId, connectionStatus);
    }

    /**
     * Retrieves connection status data from Redis for a given vehicle ID.
     *
     * @param subServiceOpt Optional sub-service identifier.
     * @param vehicleId Identifier of the vehicle.
     * @return Connection status data for the vehicle.
     */
    public VehicleIdDeviceIdStatus forceGet(Optional<String> subServiceOpt, String vehicleId) {
        String subService =
                subServiceOpt.isPresent() ? subServiceOpt.get() : String.valueOf(Optional.empty());
        String redisMapKey =
                mapParentKey != null ? mapParentKey : subServiceToParentKeyMapping.get(subService);
        logger.debug("Redis map key for VehicleIdDeviceIdStatus is {}", redisMapKey);
        VehicleIdDeviceIdMapping mapping =
                deviceConnDao.forceGet(redisMapKey, new DeviceStatusKey(vehicleId));
        VehicleIdDeviceIdStatus status = new VehicleIdDeviceIdStatus();
        if (mapping != null && mapping.getDeviceIds() != null) {
            for (String deviceId : mapping.getDeviceIds()) {
                status.addDeviceId(deviceId, String.valueOf(ConnectionStatus.ACTIVE));
            }
        }
        logger.debug("Mapping got from redis for vehicleId {} is {}", vehicleId, status);
        return status;
    }

    /**
     * Puts connection status data into the in-memory cache.
     *
     * @param key Key for the data.
     * @param deviceIds Connection status data to store.
     * @param mutationId Optional mutation identifier.
     * @param subService Optional sub-service identifier.
     */
    @Override
    public void put(String key, VehicleIdDeviceIdStatus deviceIds, Optional<MutationId> mutationId,
            Optional<String> subService) {
        DeviceStatusKey deviceStatusKey = null;
        if (subService.isPresent()) {
            String keyWithSubService = key + DMAConstants.SEMI_COLON + subService.get();
            deviceStatusKey = new DeviceStatusKey(keyWithSubService);
        } else {
            deviceStatusKey = new DeviceStatusKey(key);
        }
        VehicleIdDeviceIdStatus mapping =
                new VehicleIdDeviceIdStatus(Version.V1_0, deviceIds.getDeviceIds());
        deviceConnStatusDao.putIfAbsent(deviceStatusKey, mapping, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        logger.info("Key {}, Value {} put in in-memory cache", key, mapping.toString());
    }

    /**
     * Deletes connection status data from the in-memory cache.
     *
     * @param key Key for the data.
     * @param deviceId Identifier of the device to delete.
     * @param mutationId Optional mutation identifier.
     * @param subService Optional sub-service identifier.
     */
    @Override
    public void delete(String key, String deviceId, Optional<MutationId> mutationId,
            Optional<String> subService) {
        String redisParentKey = mapParentKey;
        if (subService.isPresent()) {
            key = key + DMAConstants.SEMI_COLON + subService.get();
            redisParentKey = subServiceToParentKeyMapping.get(subService.get());
        }
        DeviceStatusKey deviceStatusKey = new DeviceStatusKey(key);
        logger.debug("In delete Mapping for key {}", deviceStatusKey.convertToString());
        VehicleIdDeviceIdStatus mapping = deviceConnStatusDao.get(deviceStatusKey);
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
                deviceConnStatusDao.putToMap(redisParentKey, deviceStatusKey, mapping, mutationId,
                        InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
            }
        }
    }

    /**
     * Deletes all connection status data for a given vehicle ID from the in-memory cache.
     *
     * @param vehicleId Identifier of the vehicle.
     * @param mutationId Optional mutation identifier.
     */
    @Override
    public void deleteKey(String vehicleId, Optional<MutationId> mutationId) {
        DeviceStatusKey deviceStatusKey = new DeviceStatusKey(vehicleId);
        logger.debug("Attempting to Delete Device Status in cache for key {}", vehicleId);
        if (subServiceToParentKeyMapping.size() > 0) {
            String[] arr = vehicleId.split(":");
            String subService = arr[arr.length - 1];
            deviceConnStatusDao.deleteFromMap(subServiceToParentKeyMapping.get(subService),
                    deviceStatusKey, mutationId,
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        } else {
            deviceConnStatusDao.deleteFromMap(mapParentKey, deviceStatusKey, mutationId,
                    InternalCacheConstants.CACHE_TYPE_DEVICE_CONN_STATUS_CACHE);
        }
    }

    /**
     * Retrieves the latest offset metadata for a given service name.
     *
     * @param serviceName Name of the service.
     * @return An {@link Optional} containing the latest offset metadata, if available.
     */
    @Override
    public Optional<OffsetMetadata> getOffsetMetadata(String serviceName) {
        return deviceConnStatusDao.getOffsetMetadata(serviceName);
    }
}
