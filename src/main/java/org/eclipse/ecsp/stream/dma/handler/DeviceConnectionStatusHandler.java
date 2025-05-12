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

package org.eclipse.ecsp.stream.dma.handler;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidVehicleIDException;
import org.eclipse.ecsp.analytics.stream.base.exception.OfflineBufferEntriesException;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaConsumerCallback;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.analytics.stream.base.platform.utils.PlatformUtils;
import org.eclipse.ecsp.analytics.stream.base.utils.ConnectionStatusRetriever;
import org.eclipse.ecsp.analytics.stream.base.utils.ObjectUtils;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0.ConnectionStatus;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceMessagingException;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusAPIInMemoryService;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoInMemoryCache;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.presencemanager.DeviceFetchConnectionStatusProducer;
import org.eclipse.ecsp.stream.dma.scheduler.DeviceMessagingEventScheduler;
import org.eclipse.ecsp.stream.dma.shouldertap.DeviceShoulderTapService;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.ecsp.utils.metrics.IgniteErrorCounter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * DeviceConnectionStatusHandler is responsble for maintaining cache of
 * DeviceStatus and taking appropriate measures based on the device
 * status.
 * It also has the DeviceStatusCallBack which maintains the DeviceStatus
 * in Cache and triggers the processing of messages stored in
 * OfflineBuffer when device is ACTIVE.
 * Update TargetDeviceId before forwarding it to the next handle.
 *
 * @author avadakkootko
 */
@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DeviceConnectionStatusHandler implements DeviceMessageHandler {
    
    /** The ecu types. */
    private static List<String> ecuTypes = null;
    
    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceConnectionStatusHandler.class);

    /** The next handler. */
    private DeviceMessageHandler nextHandler;

    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /** The task id. */
    private String taskId;
    
    /** The offline buffer DAO. */
    @Autowired
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;
    
    /** The device status back door kafka consumer. */
    @Autowired
    private DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;
    
    /** The device service. */
    @Autowired
    private DeviceStatusService deviceService;
    
    /** The device service in memory. */
    @Autowired
    private DeviceStatusAPIInMemoryService deviceServiceInMemory;
    
    /** The device status dao. */
    @Autowired
    private DeviceStatusDaoCacheBackedInMemoryImpl deviceStatusDao;
    
    /** The device status API dao. */
    @Autowired
    private DeviceStatusDaoInMemoryCache deviceStatusAPIDao;
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The scheduler enabled. */
    @Value("${" + PropertyNames.SCHEDULER_ENABLED + ":true}")
    private String schedulerEnabled;

    /** The ttl expiry notification enabled. */
    @Value("${" + PropertyNames.DMA_TTL_EXPIRY_NOTIFICATION_ENABLED + ":true}")
    private String ttlExpiryNotificationEnabled;
    
    /** The offline buffer per device. */
    @Value("${" + PropertyNames.OFFLINE_BUFFER_PER_DEVICE + ":false}")
    private boolean offlineBufferPerDevice;
    
    /** The device shoulder tap service. */
    @Autowired
    private DeviceShoulderTapService deviceShoulderTapService;
    
    /** The device message utils. */
    @Autowired
    private DeviceMessageUtils deviceMessageUtils;
    
    /** The error counter. */
    @Autowired
    private IgniteErrorCounter errorCounter;
    
    /** The event scheduler. */
    @Autowired
    private DeviceMessagingEventScheduler eventScheduler;
    
    /** The filter DM offline entry impl class. */
    @Value("${" + PropertyNames.FILTER_DM_OFFLINE_BUFFER_ENTRIES_IMPL
            + ": org.eclipse.ecsp.stream.dma.handler.NoFilterDMOfflineBufferEntryImpl}")
    private String filterDMOfflineEntryImplClass;

    /** The fetch conn status producer. */
    @Autowired
    private DeviceFetchConnectionStatusProducer fetchConnStatusProducer;
    
    /** The platform utils. */
    @Autowired
    private PlatformUtils platformUtils;

    /** The filtered buffer entry. */
    FilterDMOfflineBufferEntry filteredBufferEntry;
    
    /** The connection status retriever. */
    private ConnectionStatusRetriever connectionStatusRetriever;

    /** The events to S kip offline buffer. */
    /*
     * CR-1758 property which will hold events that will not be saved to
     * offline buffer in DMA
     */
    @Value("${" + PropertyNames.DMA_EVENTS_SKIP_ONLINE_BUFFER + ":}")
    private String eventsToSKipOfflineBuffer;

    /** The sub services. */
    /*
     * RTC 355420. DMA should have the functionality to track device connection
     * status at sub-service level, if configured any.
     */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    private String subServices;
    
    /** The conn status retriever impl class. */
    @Value("${" + PropertyNames.DMA_CONNECTION_STATUS_RETRIEVER_IMPL + ":" 
            + PropertyNames.DEFAULT_CONNECTION_STATUS_RETRIEVER_IMPL + "}")
    private String connStatusRetrieverImplClass;
    
    /** The skip offline buffer events. */
    private List<String> skipOfflineBufferEvents = new ArrayList<>();
    
    /** The sub services list. */
    private List<String> subServicesList = new ArrayList<>();

    /** The process per sub service. */
    private boolean processPerSubService = false;

    /**
     * Sets the up ecu types.
     *
     * @param ecuTypes the new up ecu types
     */
    private static void setupEcuTypes(List<String> ecuTypes) {
        DeviceConnectionStatusHandler.ecuTypes = ecuTypes;
    }

    /**
     * Gets the service name.
     *
     * @return the service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Sets the service name.
     *
     * @param serviceName the new service name
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Sets the skip offline buffer events.
     *
     * @param skipOfflineBufferEvents the new skip offline buffer events
     */
    public void setSkipOfflineBufferEvents(List<String> skipOfflineBufferEvents) {
        this.skipOfflineBufferEvents = skipOfflineBufferEvents;
    }
    
    /**
     * Validates whether certain instances have been initialized.
     */
    @PostConstruct
    public void validate() {
        filteredBufferEntry = (FilterDMOfflineBufferEntry) 
            platformUtils.getInstanceByClassName(filterDMOfflineEntryImplClass);
        connectionStatusRetriever = (ConnectionStatusRetriever) 
            platformUtils.getInstanceByClassName(connStatusRetrieverImplClass);
        ObjectUtils.requireNonNull(deviceStatusBackDoorKafkaConsumer,
                "Uninitialized Backdoor KafkaConsumer Factory.");
        if (StringUtils.isNotEmpty(subServices)) {
            subServicesList = Arrays.asList(subServices.trim().split(","));
            logger.info("Sub services configured are {}", subServicesList);
            processPerSubService = true;
        }
    }

    /**
     * Check is the Event is deviceRoutable and forward it to the next handler for further processing.
     *
     * @param key the key
     * @param entity the entity
     */
    @Override
    public void handle(IgniteKey<?> key, DeviceMessage entity) {
        DeviceMessageHeader header = entity.getDeviceMessageHeader();
        if (header.isGlobalTopicNameProvided()) {
            nextHandler.handle(key, entity);
            return;
        }
        String vehicleId = header.getVehicleId();
        if (StringUtils.isEmpty(vehicleId)) {
            logger.error("VehicleId not set in IgniteEvent {} for IgniteKey {}", entity, key);
            throw new InvalidVehicleIDException("VehicleId not set in IgniteEvent " + entity + " for IgniteKey " + key);
        }
        if (isDeviceActive(key, entity)) {
            logger.debug("Forwarding the request with requestId {} to next handler.", header.getRequestId());
            nextHandler.handle(key, entity);
        } else {
            handleDeviceInactiveState(key, entity);
        }
    }

    /**
     * Checks if is device active.
     *
     * @param key the key
     * @param entity the entity
     * @return true, if is device active
     */
    private boolean isDeviceActive(IgniteKey<?> key, DeviceMessage entity) {
        DeviceMessageHeader header = entity.getDeviceMessageHeader();
        String vehicleId = header.getVehicleId();
        if (entity.isOtherBrokerConfigured()) {
            ConnectionStatus connStatus = getConnectionStatus(header);
            return connStatus != null && connStatus.toString().equals(DMAConstants.ACTIVE);
        } else {
            Optional<String> deviceId = getDeviceIdIfActive(key, header, vehicleId);
            if (deviceId.isPresent()) {
                header.withTargetDeviceId(deviceId.get());
            }
            return deviceId.isPresent();
        }
    }

    /**
     * <P>
     * Searches for connection status data for a vehicleId-deviceId
     * pair in the in-memory first, if not found in in-memory, then get the
     * connection status data from the third party API.
     *
     * Connection status can be null in two cases:
     *
     * 1. No data exists for this VIN in in-memory map.
     *
     * 2. If multiple devices are associated with a VIN and data exists for
     * this VIN in in-memory but not for this targetDeviceId.
     *
     * Eg. Suppose vehicleId = vin123 and devices associated are d1 and d2.
     * Now, in in-memory, following mapping exists: vin123 =
     * {d1=ACTIVE} and request comes for targetDeviceId = d2. In this case,
     *
     * status will be null for d2 device and it will be fetched from
     * the API. Suppose API returned INACTIVE status for d2 device,
     * then updated mapping for this VIN in in-memory will look like:
     *
     * vin123 = {d1=ACTIVE,d2=INACTIVE}
     *
     * Check {@link
     * org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever#getConnectionStatusData
     * (String, String, String)} for more on how it fetches data from
     * the third party API.
     * </P>
     *
     * @param header header
     * @return {@link ConnectionStatus}
     */
    protected ConnectionStatus getConnectionStatus(DeviceMessageHeader header) {
        String vehicleId = header.getVehicleId();
        String targetDeviceId = header.getTargetDeviceId();
        VehicleIdDeviceIdStatus mapping = deviceServiceInMemory.get(vehicleId);
        ConnectionStatus status = null;
        if (mapping != null && mapping.getDeviceIds().containsKey(targetDeviceId)) {
            logger.debug("Received connection status of vehicleId {} and deviceId {} from in-memory as {}", vehicleId,
                    targetDeviceId, mapping.getDeviceIds().get(targetDeviceId));
            return mapping.getDeviceIds().get(targetDeviceId);
        } else {
            // Get the connection status from a third party API and update the
            // same in in-memory cache.
            logger.debug("Fetching connection status from the API for vehicleId {} and deviceId {}", 
                    vehicleId, targetDeviceId);
            mapping = connectionStatusRetriever.getConnectionStatusData(header.getRequestId(), 
                 vehicleId, targetDeviceId);
            if (mapping != null) {
                // Add a new mapping in in-memory for this vehicleId
                deviceServiceInMemory.update(vehicleId, targetDeviceId, 
                        mapping.getDeviceIds().get(targetDeviceId).toString());
                // update the local variable status' value
                status = mapping.getDeviceIds().get(targetDeviceId);
            }
        }
        return status;
    }

    /**
     * Handle device inactive state.
     *
     * @param key the key
     * @param entity the entity
     */
    protected void handleDeviceInactiveState(IgniteKey<?> key, DeviceMessage entity) {
        DeviceMessageFailureEventDataV1_0 data = new DeviceMessageFailureEventDataV1_0();
        data.setFailedIgniteEvent(entity.getEvent());
        data.setErrorCode(DeviceMessageErrorCode.DEVICE_STATUS_INACTIVE);
        data.setDeviceStatusInactive(true);
        deviceMessageUtils.postFailureEvent(data, key, spc, entity.getFeedBackTopic());
        DeviceMessageHeader header = entity.getDeviceMessageHeader();
        String vehicleId = header.getVehicleId();
        String subService = processPerSubService ? entity.getDeviceMessageHeader()
                .getDevMsgTopicSuffix().toLowerCase() : null;

        // check to see from application properties if we need to skip any event
        // for offline buffer
        // and null check is handled in case no event ID is presnt (test cases
        if (entity.getEvent().getEventId() == null
                || skipOfflineBufferEvents.stream().noneMatch(entity.getEvent().getEventId()::equalsIgnoreCase)) {
            logger.debug("Connection status for vehicleId{} and service {} is INACTIVE. "
                    + "Adding to OfflineBuffer", vehicleId, serviceName);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, key, entity, subService);

            // WI-374794 Create a scheduler for entry added to offline buffer if
            // scheduler enabled
            if (Boolean.parseBoolean(schedulerEnabled) && Boolean.parseBoolean(ttlExpiryNotificationEnabled)) {
                eventScheduler.scheduleEvent(key, entity, spc);
            }
        }
        String requestId = header.getRequestId();
        boolean shoulderTapEnabled = header.isShoulderTapEnabled();
        if (shoulderTapEnabled) {
            Map<String, Object> extraParameters = new HashMap<>();
            String bizTransactionId = entity.getEvent().getBizTransactionId();
            extraParameters.put(DMAConstants.BIZ_TRANSACTION_ID, bizTransactionId);
            deviceShoulderTapService.wakeUpDevice(requestId, vehicleId, serviceName, key, entity, extraParameters);
        }
    }

    /**
     * <P>
     * Retrun deviceId is if vehicleId to deviceId mapping is present
     * in cache which indicates the status was ACTIVE.
     * 
     * First check if targetId is present, if present check the
     * mapping between vehicleId to targetDeviceId.
     * 
     * Else next chek if sourceDeviceId is set, if yes check the
     * mapping between vehicleId to sourceDeviceId.
     * 
     * If neither of the above options are available then check for the
     * mapping in cache for the given vehicleID. If mappings are not
     * greater than 1 then use that as the targetDeviceId.
     * </P>
     *
     * @param key the key
     * @param header header
     * @param vehicleId vehicleId
     * @return String
     * @throws DeviceMessagingException DeviceMessagingException
     */
    protected Optional<String> getDeviceIdIfActive(IgniteKey<?> key, DeviceMessageHeader header, String vehicleId) {
        String deviceId = null;
        String targetDeviceId = header.getTargetDeviceId();
        ConcurrentHashSet<String> deviceIdsInCache = null;
        /*
         * If a service has sub-services configured, then in the service's SP while constructing the IgniteEvent,
         * before forwarding the event to DMA, it has to set the sub-service this event is intended for through
         * devMsgTopicSuffix property. Otherwise DMA won't know.
         */
        String subService = header.getDevMsgTopicSuffix() != null ? header.getDevMsgTopicSuffix().toLowerCase() : null;
        deviceIdsInCache = getDeviceIdsInCache(vehicleId, subService);
        if (CollectionUtils.isEmpty(deviceIdsInCache)) {
            if (StringUtils.isNotEmpty(header.getPlatformId())) {
                getDeviceStatusFromPresenceManager(key, header);
            }
            return Optional.empty();
        }

        if (StringUtils.isNotEmpty(targetDeviceId) && deviceIdsInCache.contains(targetDeviceId)) {
            deviceId = targetDeviceId;
        } else {
            if (deviceIdsInCache.size() == 1) {
                deviceId = deviceIdsInCache.iterator().next();
            } else {
                logger.error("{} cannot be forwarded to DeviceMessaging as there are multiple vehicleId to "
                        + "deviceId mappings present.", header);
                throw new DeviceMessagingException("Multiple forwards not allowed for key :" + vehicleId);
            }
        }
        return Optional.ofNullable(deviceId);
    }

    /**
     * Gets the device status from presence manager.
     *
     * @param key the key
     * @param header the header
     * @return the device status from presence manager
     */
    private void getDeviceStatusFromPresenceManager(IgniteKey<?> key, DeviceMessageHeader header) {
        fetchConnStatusProducer.pushEventToFetchConnStatus(key, header, spc);
    }

    /**
     * Gets the device ids in cache.
     *
     * @param vehicleId the vehicle id
     * @param subService the sub service
     * @return the device ids in cache
     */
    private ConcurrentHashSet<String> getDeviceIdsInCache(String vehicleId, String subService) {
        ConcurrentHashSet<String> deviceIdsInCache;
        if (StringUtils.isNotEmpty(subService) && processPerSubService && subServicesList.contains(subService)) {
            deviceIdsInCache = deviceService.get(vehicleId, Optional.of(subService));
        } else {
            deviceIdsInCache = deviceService.get(vehicleId, Optional.empty());
        }
        return deviceIdsInCache;
    }
    
    /**
     * DeviceStatusCallBack received an Igniteevent with event data
     * DeviceConnStatusV1 from Kafka Back door Consumer.
     * If Device Connection status changes from INACTIVE to ACTIVE. Messges are
     * retrieved from Offline buffer and processed.
     * The DeviceStatus is updated in cache.

     * @author avadakkootko
     */
    public class DeviceStatusCallBack implements BackdoorKafkaConsumerCallback {

        /**
         * Process.
         *
         * @param key the key
         * @param value the value
         * @param meta the meta
         */
        @Override
        public void process(IgniteKey key, IgniteEvent value, OffsetMetadata meta) {
            logger.debug("Received in DeviceStatusCallBack with  IgniteKey {} and IgniteEvent {}", key, value);
            if (key != null && value.getEventData() != null) {
                try {
                    DeviceConnStatusV1_0 deviceStatus = (DeviceConnStatusV1_0) value.getEventData();
                    logger.debug("DeviceConnStatusV1 {}", deviceStatus);
                    String deviceId = value.getSourceDeviceId();
                    String vehicleId = value.getVehicleId();
                    /*
                     * In case sub-services are configured for the service, then
                     * in the connection status payload, against serviceName
                     * property, hivemq will tell DMA which VIN/device this
                     * payload is for. And, based on same sub-service name DMA
                     * will query events from mongo for the same sub-service
                     * because in case of sub-services, we need events from
                     * mongo based on VIN && sub-service.
                     */
                    String subService = deviceStatus.getServiceName().toLowerCase();
                    String ecuType = value.getEcuType() != null ? value.getEcuType().toLowerCase() : null;
                    boolean validSubService = (StringUtils.isNotEmpty(subService) 
                            && subServicesList.contains(subService));
                    boolean validEcuType = (StringUtils.isNotEmpty(ecuType) 
                            && DeviceConnectionStatusHandler.ecuTypes.contains(ecuType));

                    if (StringUtils.isNotEmpty(deviceId) && StringUtils.isNotEmpty(vehicleId)) {
                        performActionAsPerConnStatus(meta, deviceStatus, deviceId, vehicleId, subService, 
                                validSubService, validEcuType);
                    } else {
                        logger.error("DeviceId {} or VehicleId {} not set for IgniteEvent {} in device status "
                                + "Kafka topic with IgniteKey {}", deviceId, vehicleId, value, key);
                    }
                } catch (Exception e) {
                    errorCounter.incErrorCounter(Optional.ofNullable(taskId), e.getClass());
                    logger.error("Error while processing DeviceStatusCallBack {}", e);
                }
            } else {
                logger.error("Key {} or EventData {} cannot be null", key, value.getEventData());
            }
        }

        /**
         * Gets the committable offset.
         *
         * @return the committable offset
         */
        @Override
        public Optional<OffsetMetadata> getCommittableOffset() {
            return Optional.empty();
        }

        /**
         * Perform action as per conn status.
         *
         * @param meta the meta
         * @param deviceStatus the device status
         * @param deviceId the device id
         * @param vehicleId the vehicle id
         * @param subService the sub service
         * @param validSubService the valid sub service
         * @param validEcuType the valid ecu type
         */
        private void performActionAsPerConnStatus(OffsetMetadata meta, DeviceConnStatusV1_0 deviceStatus, 
                String deviceId, String vehicleId, String subService, boolean validSubService, boolean validEcuType) {
            String connStatus = deviceStatus.getConnStatus().getConnectionStatus();
            logger.info("Connection status from callback for vehicleId {}, deviceId {}, for service {} is {}", 
                    vehicleId, deviceId, subService, connStatus);
            if (connStatus.equals(DMAConstants.ACTIVE)) {
                performActionWhenStatusActive(vehicleId, deviceId, meta, subService, validSubService, validEcuType);
            } else if (connStatus.equals(DMAConstants.INACTIVE)) {
                performActionWhenStatusInactive(vehicleId, deviceId, meta, subService, validSubService, validEcuType);
            }
        }
    }

    /**
     * <P>
     * In cache we store a mapping of VehicleId to DeviceId.
     * If mapping is present it can be intepreted as DeviceId is ACTIVE and if mapping
     * is not present viceversa. Key is prefixed with a constant VEHICLE_DEVICE_MAPPING_.
     * 
     * When status is received as INACTIVE from hiveMq first
     * we check if vehicleId is present as key in cache or not.
     * 
     * If it is present in cache then the status is ACTIVE and no operation is performed.
     * 
     * Else if vehicleId to deviceId mapping is not present then it
     * means status was INACTIVE and it has changed to AVTIVE (i.e by storing
     * the mapping between vehicleId and deviceId). After that we retrieve
     * the events stored in mongo for this deviceId and push it to
     * mqtt.
     * </P>
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     * @param offsetMeta the offset meta
     * @param subService the sub service
     * @param validSubService the valid sub service
     * @param validEcuType the valid ecu type
     */

    public void performActionWhenStatusActive(String vehicleId, String deviceId, OffsetMetadata offsetMeta,
            String subService, boolean validSubService, boolean validEcuType) {
        /*
         * Provide comments on below if, else condition
         */
        if (validEcuType) {
            deviceServiceInMemory.update(vehicleId, deviceId, DMAConstants.ACTIVE);
            logger.info("Connection status for vehicleId {} and deviceId {} updated as ACTIVE in "
                    + "DeviceStatusAPIInMemoryService", vehicleId, deviceId);
        } else {
            ConcurrentHashSet<String> deviceIdsInCache = validSubService 
                    ? deviceService.get(vehicleId, Optional.of(subService))
                    : deviceService.get(vehicleId, Optional.empty());
            createDeviceIdsInCacheDataSet(vehicleId, deviceId, offsetMeta, subService, 
                    validSubService, deviceIdsInCache);
        }
        deviceShoulderTapService.executeOnDeviceActiveStatus(vehicleId, deviceId, serviceName);

        try {
            List<DMOfflineBufferEntry> bufferedEntries = null;
            if (offlineBufferPerDevice) {
                bufferedEntries = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId,
                        true, Optional.ofNullable(deviceId),
                        (validSubService) ? Optional.of(subService) : Optional.empty());
                logger.debug("{} OfflineBuffer entries found for vehicleId {}, deviceId {}",
                        bufferedEntries.size(), vehicleId, deviceId);
            } else {
                bufferedEntries = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId,
                        true, Optional.empty(), (validSubService) ? Optional.of(subService) : Optional.empty());
                logger.debug("{} OfflineBuffer entries found for vehicleId {}", bufferedEntries.size(),
                        vehicleId);
            }
            if (!bufferedEntries.isEmpty()) {
                bufferedEntries = filterBufferedEntries(vehicleId, bufferedEntries);
            }
            bufferedEntries.forEach(entry -> createAbstractIgniteEvent(vehicleId, deviceId, entry));
        } catch (Exception e) {
            throw new OfflineBufferEntriesException("Error while getting offline buffer entries from Mongo for "
                    + "vehicleId " + vehicleId + " ,service - " + serviceName, e);
        }
    }

    /**
     * Filter buffered entries.
     *
     * @param vehicleId the vehicle id
     * @param bufferedEntries the buffered entries
     * @return the list
     */
    private List<DMOfflineBufferEntry> filterBufferedEntries(String vehicleId, 
        List<DMOfflineBufferEntry> bufferedEntries) {
        int initialSize = bufferedEntries.size();        
        bufferedEntries = filteredBufferEntry.filterAndUpdateDmOfflineBufferEntries(bufferedEntries);
        int filteredSize = bufferedEntries.size();
        logger.debug("Out of {} OfflineBuffer entries {} filtered For vehicleId {}.", initialSize,
                (initialSize - filteredSize), vehicleId);
        return bufferedEntries;
    }

    /**
     * Creates the device ids in cache data set.
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     * @param offsetMeta the offset meta
     * @param subService the sub service
     * @param validSubService the valid sub service
     * @param deviceIdsInCache the device ids in cache
     */
    private void createDeviceIdsInCacheDataSet(String vehicleId, String deviceId, OffsetMetadata offsetMeta,
            String subService, boolean validSubService, ConcurrentHashSet<String> deviceIdsInCache) {
        if (deviceIdsInCache == null) {
            deviceIdsInCache = new ConcurrentHashSet<>();
        }
        deviceIdsInCache.add(deviceId);
        deviceService.put(vehicleId, deviceIdsInCache, Optional.ofNullable(offsetMeta),
                validSubService ? Optional.of(subService) : Optional.empty());
        logger.info("VehicleId {} to DeviceId {} mapping updated in cache.", vehicleId, deviceId);
    }

    /**
     * Creates the abstract ignite event.
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     * @param entry the entry
     */
    private void createAbstractIgniteEvent(String vehicleId, String deviceId, 
            DMOfflineBufferEntry entry) {
        String targetDeviceIdFromHeader = (entry.getEvent().getDeviceMessageHeader() != null)
                ? entry.getEvent().getDeviceMessageHeader().getTargetDeviceId()
                : null;
        if (StringUtils.isEmpty(targetDeviceIdFromHeader) || deviceId.equals(targetDeviceIdFromHeader)) {
            AbstractIgniteEvent event = entry.getEvent().getEvent();
            Optional<String> targetDeviceId = event.getTargetDeviceId();
            if (!targetDeviceId.isPresent()) {
                event.setTargetDeviceId(deviceId);
            }
            String id = entry.getId();
            DeviceMessageHeader deviceMessageHeader = entry.getEvent().getDeviceMessageHeader();
            offlineBufferDAO.removeOfflineBufferEntry(id);
            logger.info("OfflineBuffer entry with ID : {} and vehicleId : {}, requestId : {} "
                    + "and messageId : {} has been removed after processing.", id, vehicleId, 
                     deviceMessageHeader.getRequestId(), deviceMessageHeader.getMessageId());
            handle(entry.getIgniteKey(), entry.getEvent());
        }
    }

    /**
     * <p>
     * In cache we store a mapping of VehicleId to DeviceId. If mapping
     * is present it can be intepreted as DeviceId is ACTIVE and if mapping
     * is not present viceversa.
     * 
     * When status is received as INACTIVE from hiveMq first we check if
     * vehicleId is present as key in cache or not.
     * 
     * If it is present in cache then the status is ACTIVE (value
     * being stored is still the deviceId) and it is deleted from cache.
     * Else if it is not present no operation is performed.
     * </p>
     *
     * @param vehicleId the vehicle id
     * @param deviceId the device id
     * @param offsetMeta the offset meta
     * @param subService the sub service
     * @param validSubService the valid sub service
     * @param validEcuType the valid ecu type
     */
    public void performActionWhenStatusInactive(String vehicleId, String deviceId, OffsetMetadata offsetMeta,
            String subService, boolean validSubService, boolean validEcuType) {
        if (validEcuType) {
            deviceServiceInMemory.update(vehicleId, deviceId, DMAConstants.INACTIVE);
        } else {
            ConcurrentHashSet<String> deviceIdsInCache = validSubService 
                    ? deviceService.get(vehicleId, Optional.of(subService))
                    : deviceService.get(vehicleId, Optional.empty());
            if (deviceIdsInCache != null && deviceIdsInCache.contains(deviceId)) {
                if (validSubService) {
                    deviceService.delete(vehicleId, deviceId, Optional.ofNullable(offsetMeta), Optional.of(subService));
                } else {
                    deviceService.delete(vehicleId, deviceId, Optional.ofNullable(offsetMeta), Optional.empty());
                }
                logger.info("Deleted deviceId {} from cache", deviceId);
            } else {
                logger.debug("VehicleId {} to DeviceId {} mapping not present, hence status was already INACTIVE. "
                        + "No operation will be performed", vehicleId, deviceId);
            }
        }
    }

    /**
     * Sets the next handler.
     *
     * @param handler the new next handler
     */
    @Override
    public void setNextHandler(DeviceMessageHandler handler) {
        nextHandler = handler;

    }

    /**
     * Sets the stream processing context.
     *
     * @param ctx the ctx
     */
    @Override
    public void setStreamProcessingContext(StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctx) {
        spc = ctx;
        int partition = Integer.parseInt(spc.getTaskID().split("_")[1]);
        deviceShoulderTapService.setStreamProcessingContext(spc);
        deviceStatusBackDoorKafkaConsumer.addCallback(new DeviceStatusCallBack(), partition);
        skipOfflineBufferEvents = Arrays.asList(eventsToSKipOfflineBuffer.trim().split(","));
    }

    /**
     * setup().
     *
     * @param taskId taskId
     * @param ecuTypes ecuTypes
     */

    public void setup(String taskId, List<String> ecuTypes) {
        this.taskId = taskId;
        deviceShoulderTapService.setup(taskId);
        setupEcuTypes(ecuTypes);
        deviceStatusDao.setTaskId(taskId);
        deviceStatusAPIDao.setTaskId(taskId);
    }

    /**
     * Sets the offline buffer per device.
     *
     * @param flag the new offline buffer per device
     */
    protected void setOfflineBufferPerDevice(boolean flag) {
        offlineBufferPerDevice = flag;
    }

    /**
     * Sets the filtered buffer entry.
     *
     * @param filteredBufferEntry the new filtered buffer entry
     */
    // Providing setter for unit test case
    void setFilteredBufferEntry(FilterDMOfflineBufferEntry filteredBufferEntry) {
        this.filteredBufferEntry = filteredBufferEntry;
    }

    /**
     * Sets the sub services list.
     *
     * @param subServicesList the new sub services list
     */
    void setSubServicesList(List<String> subServicesList) {
        this.subServicesList = subServicesList;
    }

    /**
     * Sets the process per sub service.
     *
     * @param processPerSubService the new process per sub service
     */
    void setProcessPerSubService(boolean processPerSubService) {
        this.processPerSubService = processPerSubService;
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        // overridden method
    }
}
