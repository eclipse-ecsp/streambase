package org.eclipse.ecsp.stream.dma.dao;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidServiceNameException;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class to handle device status related operations.
 */
@Component
public class DeviceStatusUtil {

    /**
     * Logger instance for logging messages.
     */
    private static final IgniteLogger LOGGER =
            IgniteLoggerFactory.getLogger(DeviceStatusUtil.class);

    /**
     * Service name.
     */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /**
     * Sub-services.
     */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    private String subServices;

    /**
     * Retrieves the map parent key for a given service name and optional sub-service.
     *
     * @param serviceName the service name
     * @param subService the optional sub-service
     * @return the map parent key
     */
    public String getMapParentKey(String serviceName, Optional<String> subService) {
        String mapParentKey = null;
        if (StringUtils.isEmpty(subServices)) {
            mapParentKey = DMAConstants.VEHICLE_DEVICE_MAPPING + serviceName;
            return mapParentKey;
        }
        Map<String, String> subServiceToParentKeyMapping = getSubServiceToParentKeyMapping();
        if (subService.isPresent()) {
            mapParentKey = subServiceToParentKeyMapping.get(subService.get());
        }
        return mapParentKey;
    }

    /**
     * Retrieves the map parent key with the service name.
     *
     * @param serviceName the service name
     * @return the map parent key
     */
    public String getMapParentKeyWithServiceName(String serviceName) {
        String mapParentKey = null;
        if (StringUtils.isEmpty(subServices)) {
            mapParentKey = DMAConstants.VEHICLE_DEVICE_MAPPING + serviceName;
        }
        return mapParentKey;
    }

    /**
     * Retrieves the sub-service to parent key mapping.
     *
     * @return the sub-service to parent key mapping
     */
    public Map<String, String> getSubServiceToParentKeyMapping() {
        Map<String, String> subServiceToParentKeyMapping = new HashMap<>();
        if (StringUtils.isNotEmpty(subServices)) {
            List<String> subServicesList = Arrays.asList(subServices.split(","));
            for (String subService : subServicesList) {
                subServiceToParentKeyMapping.put(subService,
                        DMAConstants.VEHICLE_DEVICE_MAPPING + subService);
            }
            LOGGER.info("Sub-Service to VEHICLE_DEVICE_MAPPING initialized as {}",
                    subServiceToParentKeyMapping);
        }
        return subServiceToParentKeyMapping;
    }

    /**
     * Retrieves the service name.
     *
     * @return the service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Validates the service name.
     *
     * @param serviceName the service name
     */
    public void validateServiceName(String serviceName) {
        if (StringUtils.isEmpty(serviceName)) {
            throw new InvalidServiceNameException("Service name cannot be empty.");
        }
    }

    /**
     * Retrieves the sub-service name from the header.
     *
     * @param header the device message header
     * @return the sub-service name
     */
    public String getSubServiceNameFromHeader(DeviceMessageHeader header) {
        return header.getDevMsgTopicSuffix() != null ? header.getDevMsgTopicSuffix().toLowerCase()
                : null;
    }
}
