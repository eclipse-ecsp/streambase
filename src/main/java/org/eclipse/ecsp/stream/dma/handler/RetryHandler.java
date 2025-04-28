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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.ThreadUtils;
import org.eclipse.ecsp.domain.DeviceConnStatusV1_0.ConnectionStatus;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.entities.dma.RetryRecord;
import org.eclipse.ecsp.entities.dma.RetryRecordIds;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.config.EventConfig;
import org.eclipse.ecsp.stream.dma.config.EventConfigProvider;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DMARetryBucketDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.DMARetryRecordDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.dao.key.RetryBucketKey;
import org.eclipse.ecsp.stream.dma.dao.key.RetryRecordKey;
import org.eclipse.ecsp.stream.dma.scheduler.DeviceMessagingEventScheduler;
import org.eclipse.ecsp.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.ecsp.utils.metrics.IgniteErrorCounter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * RetryHandler takes care of retrying events based on the configured thresholds of retry count and TTL.
 *
 * @author avadakkootko
 */
@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class RetryHandler implements DeviceMessageHandler {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(RetryHandler.class);
    
    /** The retry executor. */
    private ScheduledExecutorService retryExecutor = null;
    
    /** The next handler. */
    private DeviceMessageHandler nextHandler;
    
    /** The device message utils. */
    @Autowired
    private DeviceMessageUtils deviceMessageUtils;
    
    /** The retry bucket DAO. */
    @Autowired
    private DMARetryBucketDAOCacheBackedInMemoryImpl retryBucketDAO;
    
    /** The retry event DAO. */
    @Autowired
    private DMARetryRecordDAOCacheBackedInMemoryImpl retryEventDAO;

    /** The error counter. */
    @Autowired
    private IgniteErrorCounter errorCounter;

    /** The offline buffer DAO. */
    @Autowired
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;

    /** The event scheduler. */
    @Autowired
    private DeviceMessagingEventScheduler eventScheduler;

    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /** The task id. */
    private String taskId;

    /** The retry bucket map key. */
    private String retryBucketMapKey;
    
    /** The retry event map key. */
    private String retryEventMapKey;
    
    /** The conn status handler. */
    private DeviceConnectionStatusHandler connStatusHandler;

    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /** The max retry. */
    @Value("${" + PropertyNames.DMA_SERVICE_MAX_RETRY + ":3}")
    private int maxRetry;

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /** The retry interval. */
    @Value("${" + PropertyNames.DMA_SERVICE_RETRY_INTERVAL_MILLIS + ":60000}")
    private long retryInterval;

    /** The scheduler enabled. */
    @Value("${" + PropertyNames.SCHEDULER_ENABLED + ":true}")
    private String schedulerEnabled;
    
    /** The ttl expiry notification enabled. */
    @Value("${" + PropertyNames.DMA_TTL_EXPIRY_NOTIFICATION_ENABLED + ":true}")
    private String ttlExpiryNotificationEnabled;

    /** The retry min threshold. */
    @Value("${" + PropertyNames.DMA_SERVICE_RETRY_MIN_THRESHOLD_MILLIS + ":1000}")
    private int retryMinThreshold;

    /** The retry interval divisor. */
    @Value("${" + PropertyNames.DMA_SERVICE_RETRY_INTERVAL_DIVISOR + ":10}")
    private int retryIntervalDivisor;

    /** The Constant DEFAULT_EVENT_CONFIG_PROVIDER. */
    private static final String DEFAULT_EVENT_CONFIG_PROVIDER = 
            "org.eclipse.ecsp.stream.dma.config.DefaultEventConfigProvider";

    /** The event config provider impl class. */
    @Value("${" + PropertyNames.DMA_EVENT_CONFIG_PROVIDER_CLASS + ":" + DEFAULT_EVENT_CONFIG_PROVIDER + "}")
    private String eventConfigProviderImplClass;

    /** The sub services. */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    private String subServices;

    /** The config provider. */
    EventConfigProvider configProvider;
    
    /** The event config map. */
    private ConcurrentMap<String, EventConfig> eventConfigMap = new ConcurrentHashMap<>();

    /** The retry attempt log. */
    private static String retryAttemptLog = 
            "Current retry attempt is {} for messageId {}, with requestId {} and key {}";

    /**
     * Sets the retry min threshold.
     *
     * @param retryMinThreshold the new retry min threshold
     */
    public void setRetryMinThreshold(int retryMinThreshold) {
        this.retryMinThreshold = retryMinThreshold;
    }

    /**
     * Sets the max retry.
     *
     * @param maxRetry the new max retry
     */
    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
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
     * Sets the retry interval.
     *
     * @param retryInterval the new retry interval
     */
    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    /**
     * Sets the retry interval divisor.
     *
     * @param retryIntervalDivisor the new retry interval divisor
     */
    public void setRetryIntervalDivisor(int retryIntervalDivisor) {
        this.retryIntervalDivisor = retryIntervalDivisor;
    }

    /**
     * Sets the event config map.
     *
     * @param eventConfigMap the event config map
     */
    public void setEventConfigMap(ConcurrentMap<String, EventConfig> eventConfigMap) {
        this.eventConfigMap = eventConfigMap;
    }

    /**
     * Gets the event config map.
     *
     * @return the event config map
     */
    public ConcurrentMap<String, EventConfig> getEventConfigMap() {
        return this.eventConfigMap;
    }

    /**
     * Sets the event config provider impl class.
     *
     * @param eventConfigProviderImplClass the new event config provider impl class
     */
    public void setEventConfigProviderImplClass(String eventConfigProviderImplClass) {
        this.eventConfigProviderImplClass = eventConfigProviderImplClass;
    }

    /**
     * Gets the scheduled thread delay.
     *
     * @return the scheduled thread delay
     */
    long getScheduledThreadDelay() {
        long freq = retryInterval / retryIntervalDivisor;
        return freq > retryMinThreshold ? freq : retryMinThreshold;
    }

    /**
     * Gets the event config.
     *
     * @param eventId the event id
     * @return the event config
     */
    private EventConfig getEventConfig(String eventId) {
        EventConfig config = eventConfigMap.get(eventId);
        // if no EventConfig is present in map for this eventId
        if (config == null) {
            config = configProvider.getEventConfig(eventId);
            // if service has not created any EventConfig for this eventId
            // then return default one.
            if (config == null) {
                config = configProvider.getDefaultEventConfig(eventId);
            }
            eventConfigMap.put(eventId, config);
        }
        return config;
    }

    /**
     * **********HAPPY FLOW********
     * Check TTL exceeded -> Check Device ACTIVE -> Check if AckExpected -> Check
     * maxRetryExceeded -> Increment retry counter -> Add Event to retry map and add
     * messageId to appropriate bucket by timestamp -> Forward to next handle.
     *
     * @param key the key
     * @param value the value
     */
    @Override
    public void handle(IgniteKey<?> key, DeviceMessage value) {
        boolean fallbackToTTLOnMaxRetryExhausted = getEventConfig(value.getEvent().getEventId())
                .fallbackToTTLOnMaxRetryExhausted();
        /*
         * Setting pendingRetries in device message header as maxRetry in case of first
         * time arrival of event to RetryHandler Setting it in DeviceMessageHeader so
         * that pendingRetries could be accessible to us while saving the event into
         * mongo. The inner if check has been applied to make sure, when event comes the
         * second time, pendingRetries isn't set to maxRetry for this event.
         *
         * The outer if is to check, whether the following retry strategy: "Keep
         * retrying even when maxRetry is exhausted until TTL on event expires" has been
         * enabled or not. If not, then set pendingRetries only for such events. Else
         * for above retry strategy, do not set pendingRetries.
         */
        if (!fallbackToTTLOnMaxRetryExhausted) {
            DeviceMessageHeader header = value.getDeviceMessageHeader();
            if (!header.getIsPendingRetriesSet()) {
                header.withPendingRetries(maxRetry).isPendingRetriesSet(true);
                value.setDeviceMessageHeader(header);
            }
        }
        retryHandle(key, value, true);
    }

    /**
     * updateEnabled will be true for happy flow. and false when it is triggered by
     * the scheduled thread. This ensures that an event is not created in retry
     * event map when it is invoked from the scheduled thread.
     * When invoked from the scheduledthread is it is not able to find the messageId
     * in retry map then it implies the event has already been retried. Hence its
     * should not be retried again.
     *
     * @param key IgniteKey
     * @param value DeviceMessage
     * @param firstAttempt Whether it's the first attempt or not.
     */
    private void retryHandle(IgniteKey<?> key, DeviceMessage value, boolean firstAttempt) {
        logger.debug("Received IgniteKey {} and IgniteEvent {} in DeviceMessageRetryHandler", key, value);

        // Validate IgniteEvent - by checking if TTL has been exceeded or if the
        // device is still ACTIVE.
        DeviceMessageHeader header = value.getDeviceMessageHeader();
        if (header.isGlobalTopicNameProvided()) {
            nextHandler.handle(key, value);
            return;
        }
        boolean cutOffNotExceeded = validateIgniteEvent(header);
        String retryRecordKeyPart = RetryRecordKey.createVehiclePart(header.getVehicleId(), header.getMessageId());
        if (cutOffNotExceeded) {
            if (checkDeviceInactive(key, value)) {
                logger.debug("Device is inactive for ignitekey {} and value {}. Removing Retry entry Record "
                        + "with key {}.", key, value, retryRecordKeyPart);
                RetryRecordKey retryKey = new RetryRecordKey(retryRecordKeyPart, taskId);
                retryEventDAO.deleteFromMap(retryEventMapKey, retryKey, Optional.empty(),
                        InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
            } else {
                /*
                 * RTC 344443 Device Message should keep retrying events when all the retry
                 * attempts are exhausted AND TTL is still not expired for an event.
                 */
                boolean fallbackToTTLOnMaxRetryExhausted = getEventConfig(value.getEvent().getEventId())
                        .fallbackToTTLOnMaxRetryExhausted();
                if (fallbackToTTLOnMaxRetryExhausted && header.isResponseExpected()) {
                    logger.debug("fallbackToTTLOnMaxRetryExhausted is enabled for eventId: {}. Message will be "
                            + "valid for retry until TTL expires.", value.getEvent().getEventId());
                    RetryRecordKey retryEventKey = new RetryRecordKey(retryRecordKeyPart, taskId);
                    RetryRecord event = retryEventDAO.get(retryEventKey);
                    long currentTime = System.currentTimeMillis();
                    retryFOrMaxOrAddInMap(key, value, firstAttempt, retryEventKey, event, currentTime);
                } else if (header.isResponseExpected() && maxRetry > 0) {
                    // Check if ack is needed and maxRetry > 0, else do not add to retry
                    RetryRecordKey retryEventKey = new RetryRecordKey(retryRecordKeyPart, taskId);
                    RetryRecord event = retryEventDAO.get(retryEventKey);
                    long currentTime = System.currentTimeMillis();
                    retryOrAddInMap(key, value, firstAttempt, retryEventKey, event, currentTime);
                    logger.debug("ResponseExpected is set to true");
                }
                /*
                 * taking into account the case when either responseExpected == false for this
                 * event OR max retries are 0, then event should be dispatched only once. No
                 * retries should be attempted.
                 */
                handleNextKey(key, value, header);
            }
        } else {
            RetryRecordKey retryEventKey = new RetryRecordKey(retryRecordKeyPart, taskId);
            int attempts = 0;
            try {
                RetryRecord event = retryEventDAO.get(retryEventKey);
                attempts = event.getAttempts();
                retryEventDAO.deleteFromMap(retryEventMapKey, retryEventKey, Optional.empty(),
                        InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
            } catch (Exception e) {
                logger.warn("Retry record unavailable in redis for key {}", retryEventKey.toString());
            }
            DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
            failEventData.setFailedIgniteEvent(value.getEvent());
            failEventData.setErrorCode(DeviceMessageErrorCode.DEVICE_DELIVERY_CUTOFF_EXCEEDED);
            failEventData.setRetryAttempts(attempts);
            failEventData.setDeviceDeliveryCutoffExceeded(true);
            deviceMessageUtils.postFailureEvent(failEventData, key, spc, value.getFeedBackTopic());
            logger.error("For key {} and value {} cutoff exceeded will not send it to device.", key, value);
        }
    }

    /**
     * Handle next key.
     *
     * @param key the key
     * @param value the value
     * @param header the header
     */
    private void handleNextKey(IgniteKey<?> key, DeviceMessage value, DeviceMessageHeader header) {
        if (!header.isResponseExpected() || maxRetry == 0) {
            nextHandler.handle(key, value);
            logger.debug("ResponseExpected is set to false or retry attempts is 0, for key {} and event {}",
                    key, value);
        }
    }

    /**
     * Retry F or max or add in map.
     *
     * @param key the key
     * @param value the value
     * @param firstAttempt the first attempt
     * @param retryEventKey the retry event key
     * @param event the event
     * @param currentTime the current time
     */
    private void retryFOrMaxOrAddInMap(IgniteKey<?> key, DeviceMessage value, boolean firstAttempt, 
            RetryRecordKey retryEventKey, RetryRecord event, long currentTime) {
        if (event != null) {
            attemptRetryForFallbackToTLLOnMaxRetryExhausted(event, currentTime, retryEventKey);
        } else if (firstAttempt) {
            addToRetryMap(currentTime, key, value, retryEventKey);
        }
    }

    /**
     * Retry or add in map.
     *
     * @param key the key
     * @param value the value
     * @param firstAttempt the first attempt
     * @param retryEventKey the retry event key
     * @param event the event
     * @param currentTime the current time
     */
    private void retryOrAddInMap(IgniteKey<?> key, DeviceMessage value, boolean firstAttempt, 
            RetryRecordKey retryEventKey, RetryRecord event, long currentTime) {
        if (event != null) {
            attemptRetry(event, currentTime, retryEventKey);
        } else if (firstAttempt) {
            addToRetryMap(currentTime, key, value, retryEventKey);
        }
    }

    /**
     * Attempt retry for fallback to TLL on max retry exhausted.
     *
     * @param event the event
     * @param currentTime the current time
     * @param retryEventKey the retry event key
     */
    /*
     * This method is for new retry strategy
     *
     * @param RetryRecord : the event that will be retried
     *
     * @param currentTime
     *
     * @param RetryRecordKey : the key for which this RetryRecord will be fetched
     */
    private void attemptRetryForFallbackToTLLOnMaxRetryExhausted(RetryRecord event, long currentTime,
            RetryRecordKey retryEventKey) {
        IgniteKey<?> key = event.getIgniteKey();
        DeviceMessage value = event.getDeviceMessage();
        IgniteEventImpl currentEvent = value.getEvent();
        if (event.getAttempts() >= maxRetry) {
            // store into offline buffer and delete RetryRecord from redis as well as from
            // in-memory map
            // This is to treat the event as a fresh one when device will again come active
            // from inactive state.
            logger.debug("Retry exceeded maxRetry {} for retry strategy: fallbackOnTTLOnMaxRetryExhausted "
                    + "for messageId {}, with requestId {} and key {}", maxRetry, 
                    currentEvent.getMessageId(), currentEvent.getRequestId(), key);
            saveToOfflineBufferAndDeleteFromCache(key, value);

            // WI-374794 Create a scheduler for entry added to offline buffer if scheduler enabled
            if (Boolean.parseBoolean(schedulerEnabled) && Boolean.parseBoolean(ttlExpiryNotificationEnabled)) {
                eventScheduler.scheduleEvent(key, value, spc);
            }
        } else {
            // dispatch to mqtt topic
            event.addAttempt(currentTime);
            long retryIntervalTime = getNextRetryInterval(value, key);
            long nextRetry = currentTime + retryIntervalTime;
            retryEventDAO.putToMap(retryEventMapKey, retryEventKey, event, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
            logger.debug("Added event {} with key {} to retry event map.", event, retryEventKey.convertToString());
            // Add messageId to set of messageIds in retry
            // bucket keyed by timestamp
            RetryBucketKey nextRetryKey = new RetryBucketKey(nextRetry);
            String retryRecordKey = retryEventKey.getKey();
            retryBucketDAO.update(retryBucketMapKey, nextRetryKey, retryRecordKey);
            logger.debug("Added entry {} with timestamp {} to retry bucket.", retryRecordKey, nextRetry);
            DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
            failEventData.setFailedIgniteEvent(currentEvent);
            failEventData.setErrorCode(DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE);
            failEventData.setRetryAttempts(event.getAttempts());
            logger.debug(retryAttemptLog,
                    event.getAttempts(), currentEvent.getMessageId(), currentEvent.getRequestId(), key);
            deviceMessageUtils.postFailureEvent(failEventData, key, spc, value.getFeedBackTopic());
            logger.debug(retryAttemptLog,
                    event.getAttempts(), currentEvent.getMessageId(), currentEvent.getRequestId(), key);
            nextHandler.handle(key, value);
        }
    }
    
    /**
     * Gets the next retry interval.
     *
     * @param deviceMessage the device message
     * @param key the key
     * @return the next retry interval
     */
    private long getNextRetryInterval(DeviceMessage deviceMessage, IgniteKey<?> key) {
        logger.debug("Retry interval for event with key: {} , requestId: {} , messageId: {} is {}",
                key, deviceMessage.getDeviceMessageHeader().getRequestId(), deviceMessage
                .getDeviceMessageHeader().getMessageId(), deviceMessage.getEventLevelRetryInterval());
        return deviceMessage.getEventLevelRetryInterval();
    }

    /**
     * Save to offline buffer and delete from cache.
     *
     * @param key the key
     * @param value the value
     */
    /*
     * persist to mongo and remove data from cache so DMA doesn't retry.
     *
     * @param IgniteKey
     *
     * @param DeviceMessage : the payload to forward to device
     */
    private void saveToOfflineBufferAndDeleteFromCache(IgniteKey<?> key, DeviceMessage value) {
        offlineBufferDAO.addOfflineBufferEntry(value.getDeviceMessageHeader().getVehicleId(), key, value,
                (StringUtils.isNotEmpty(subServices)) 
                ? value.getDeviceMessageHeader().getDevMsgTopicSuffix().toLowerCase() : null);
        logger.info("Saved event with key: {} and value: {} to mongo as max retries have exhausted.",
                key, value);
        String retryRecordKeyPart = RetryRecordKey.createVehiclePart(value.getDeviceMessageHeader().getVehicleId(),
                value.getDeviceMessageHeader().getMessageId());
        RetryRecordKey retryKey = new RetryRecordKey(retryRecordKeyPart, taskId);
        retryEventDAO.deleteFromMap(retryEventMapKey, retryKey, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
    }

    /**
     * Attempt retry.
     *
     * @param event the event
     * @param currentTime the current time
     * @param retryEventKey the retry event key
     */
    private void attemptRetry(RetryRecord event, long currentTime, RetryRecordKey retryEventKey) {
        IgniteKey<?> key = event.getIgniteKey();
        DeviceMessage value = event.getDeviceMessage();
        int pendingRetries = value.getDeviceMessageHeader().getPendingRetries();
        // Check if number of retries has exeeded max retries, If
        // yes delete entry from retryEvent map and return true else
        // return false.
        IgniteEventImpl currentEvent = value.getEvent();
        if (pendingRetries == 0) {
            logger.debug("Retry exceeded maxRetry {} for messageId {}, with requestId {} and key {}", maxRetry,
                    currentEvent.getMessageId(), currentEvent.getRequestId(), key);
            DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
            failEventData.setFailedIgniteEvent(currentEvent);
            failEventData.setErrorCode(DeviceMessageErrorCode.RETRY_ATTEMPTS_EXCEEDED);
            failEventData.setRetryAttempts(maxRetry);
            deviceMessageUtils.postFailureEvent(failEventData, key, spc, value.getFeedBackTopic());
            retryEventDAO.deleteFromMap(retryEventMapKey, retryEventKey, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
            logger.debug("Deleted key {} from retryEventDAO", retryEventKey.convertToString());
        } else {
            event.addAttempt(currentTime);
            /*
             * next three lines involve: a. decrementing pendingRetries for this event by 1.
             * b. updating DeviceMessage with DeviceMessageHeader with updated
             * pendingRetries value. c. setting that DeviceMessage into this RetryRecord
             * event.(As per RTC 285555)
             */
            pendingRetries--;
            logger.debug("Retries left for this event are {}", pendingRetries);
            value.setDeviceMessageHeader(value.getDeviceMessageHeader().withPendingRetries(pendingRetries));
            event.setDeviceMessage(value);
            long retryIntervalTime = getNextRetryInterval(value, key);
            long nextRetry = currentTime + retryIntervalTime;
            retryEventDAO.putToMap(retryEventMapKey, retryEventKey, event, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
            logger.debug("Added event {} with key {} to retry event map.", event, retryEventKey.convertToString());
            // Add messageId to set of messageIds in retry
            // bucket keyed by timestamp
            RetryBucketKey nextRetryKey = new RetryBucketKey(nextRetry);
            String retryRecordKey = retryEventKey.getKey();
            retryBucketDAO.update(retryBucketMapKey, nextRetryKey, retryRecordKey);
            logger.debug("Added entry {} with timestamp {} to retry bucket.", retryRecordKey, nextRetry);
            DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
            failEventData.setFailedIgniteEvent(currentEvent);
            failEventData.setErrorCode(DeviceMessageErrorCode.RETRYING_DEVICE_MESSAGE);
            failEventData.setRetryAttempts(maxRetry - pendingRetries);
            logger.debug(retryAttemptLog,
                    event.getAttempts(), currentEvent.getMessageId(), currentEvent.getRequestId(), key);
            deviceMessageUtils.postFailureEvent(failEventData, key, spc, value.getFeedBackTopic());
            nextHandler.handle(key, value);
        }
    }

    /**
     * Adds the to retry map.
     *
     * @param currentTime the current time
     * @param key the key
     * @param value the value
     * @param retryEventKey the retry event key
     */
    private void addToRetryMap(long currentTime, IgniteKey<?> key, DeviceMessage value, RetryRecordKey retryEventKey) {
        long retryIntervalTime = getNextRetryInterval(value, key);
        long nextRetry = currentTime + retryIntervalTime;
        RetryRecord event = new RetryRecord(key, value, currentTime);
        retryEventDAO.putToMap(retryEventMapKey, retryEventKey, event, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_RETRY_RECORD);
        logger.debug("Created event {} with key {} to retry event map.", event, retryEventKey.convertToString());
        RetryBucketKey nextRetryKey = new RetryBucketKey(nextRetry);
        String retryRecordKey = retryEventKey.getKey();
        retryBucketDAO.update(retryBucketMapKey, nextRetryKey, retryRecordKey);
        logger.debug("Created entry {} with timestamp {} to retry event map.", retryRecordKey, nextRetry);
        nextHandler.handle(key, value);
    }

    /**
     * Checks if the TTL of the event has exceeded or if Device is inactive. If yes
     * remove event from Retry event map. Do not process further.
     *
     * @param header the header
     * @return Whether the event is expired or not.
     */
    private boolean validateIgniteEvent(DeviceMessageHeader header) {
        boolean validated = true;
        if (checkTTLExceeded(header)) {
            validated = false;
        }
        return validated;
    }

    /**
     * Checks if the TTL of the event has exceeded.
     * If TTL is not set default value needs to be provided by DMA (should be
     * discussed and Implementataion Pending).
     *
     * @param value the value
     * @return Whether the event is exipred or not.
     */
    private boolean checkTTLExceeded(DeviceMessageHeader value) {
        boolean flag = true;
        long cutOffTs = value.getDeviceDeliveryCutoff();
        if (cutOffTs != Constants.DEFAULT_DELIVERY_CUTOFF) {
            if (cutOffTs > System.currentTimeMillis()) {
                // Device cutoff not exceeded. Valid event
                flag = false;
            }
        } else {
            // Device cutoff not exceeded. Valid event
            flag = false;
        }
        return flag;
    }

    /**
     * Checks if Device is inactive.
     *
     * @param key the key
     * @param entity the entity
     * @return Whether the device is inactive or not.
     */
    private boolean checkDeviceInactive(IgniteKey<?> key, DeviceMessage entity) {
        boolean flag = true;
        DeviceMessageHeader header = entity.getDeviceMessageHeader();
        String vehicleId = header.getVehicleId();
        if (entity.isOtherBrokerConfigured()) {
            ConnectionStatus connStatus = connStatusHandler.getConnectionStatus(header);
            if (connStatus.toString().equals(DMAConstants.INACTIVE)) {
                connStatusHandler.handleDeviceInactiveState(key, entity);
                return true;
            }
            return false;
        }
        Optional<String> deviceId = connStatusHandler.getDeviceIdIfActive(key, header, vehicleId);
        if (deviceId.isPresent()) {
            flag = false;
        } else {
            // Device Inactive move to offlinebuffer
            connStatusHandler.handleDeviceInactiveState(key, entity);
        }
        return flag;
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
     * Initializes RetryHandler for the given partitionId.
     *
     * @param taskId The partitionId.
     */
    public void setup(String taskId) {
        this.taskId = taskId;
        retryBucketMapKey = RetryBucketKey.getMapKey(serviceName, taskId);
        retryEventMapKey = RetryRecordKey.getMapKey(serviceName, taskId);
        retryBucketDAO.initialize(taskId);
        retryEventDAO.initialize(taskId);
        if (retryMinThreshold <= 0) {
            throw new IllegalArgumentException(
                    "Retry Minimum threshold " + retryMinThreshold + ", should be greater than one second ");
        }
        if (retryInterval <= 0) {
            throw new IllegalArgumentException("Retry Interval " + retryInterval + ", should be greater than zero ");
        }
        if (retryInterval < retryMinThreshold) {
            throw new IllegalArgumentException("Retry Interval " + retryInterval
                    + ", should be greater than Minimum threshold " + retryMinThreshold);
        }

        if (maxRetry < 0) {
            logger.warn("Max retry cannot be less than 0. No retry will be attempted.");
            maxRetry = 0;
        }
        if (eventConfigProviderImplClass == null) {
            eventConfigProviderImplClass = DEFAULT_EVENT_CONFIG_PROVIDER;
        }

        configProvider = getEventConfigProviderImpl(eventConfigProviderImplClass);

        long delay = getScheduledThreadDelay();
        logger.info(
                "Minimum threshold for retry is {} ; Scheduled thread delay is {} ; Retry Interval for service is {}",
                retryMinThreshold, delay, retryInterval);
        if (retryExecutor == null || retryExecutor.isShutdown()) {
            retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(new RetryUncaughtExceptionHandler());
                t.setName(Thread.currentThread().getName() + ":" + "DMARetryHandler" + ":" + taskId);
                return t;
            });
            logger.info("Created retry handler executor for taskId {}", taskId);
            retryExecutor.scheduleWithFixedDelay(() -> {
                try {
                    processRetries();
                } catch (Exception e) {
                    logger.error("Error occured while retrying {}", e);
                }
            }, 0, delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Gets the event config provider impl.
     *
     * @param eventConfigProviderImplClass the event config provider impl class
     * @return the event config provider impl
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private EventConfigProvider getEventConfigProviderImpl(String eventConfigProviderImplClass) {
        EventConfigProvider eventConfigProvider = null;
        Class classObject = null;
        try {
            classObject = getClass().getClassLoader().loadClass(eventConfigProviderImplClass);
            eventConfigProvider = (EventConfigProvider) ctx.getBean(classObject);
            logger.info("Class {} loaded as EventConfigProvider", eventConfigProvider.getClass().getName());
        } catch (Exception e) {
            try {
                if (classObject == null) {
                    throw new IllegalArgumentException("Could not load the class " + eventConfigProviderImplClass);
                }
                eventConfigProvider = (EventConfigProvider) classObject.getDeclaredConstructor().newInstance();
            } catch (Exception exception) {
                String msg = String.format("Class %s could not be loaded. Not found on classpath.",
                        eventConfigProviderImplClass);
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }
        }
        return eventConfigProvider;
    }

    /**
     * Process retries.
     */
    private void processRetries() {
        /*
         * Iterate over the timestamps in map which is less than equal to current
         * timestamp.
         */
        KeyValueIterator<RetryBucketKey, RetryRecordIds> headMap = retryBucketDAO
                .getHead(new RetryBucketKey(System.currentTimeMillis()));
        /*
         * If same keys are present in two different buckets, avoid processing them
         * twice which could lead to duplicate requests at the same time. This can also
         * occur mainly due to 2 reasons : if redis entries were not properly cleared or
         * huge delay in processing
         *
         */
        Set<String> processedKeys = new HashSet<>();
        if (headMap != null) {
            while (headMap.hasNext()) {
                KeyValue<RetryBucketKey, RetryRecordIds> keyValue = headMap.next();
                RetryBucketKey bucket = keyValue.key;
                long timestamp = bucket.getTimestamp();
                Set<String> retryRecordKeys = keyValue.value.getRecordIds();
                if (retryRecordKeys != null && !retryRecordKeys.isEmpty()) {
                    logger.debug("Processing key {} from retry bucket with size {} with service {} , taskId {}",
                            timestamp, retryRecordKeys.size(), serviceName, taskId);
                    retryRecordKeys.forEach(retryRecordKey ->
                        createProcessesKeySet(processedKeys, timestamp, retryRecordKey));
                } else {
                    logger.debug("No deviceIds found for retrying at ts {} and service {}", timestamp, serviceName);
                }
                retryBucketDAO.deleteFromMap(retryBucketMapKey, bucket, Optional.empty(),
                        InternalCacheConstants.CACHE_TYPE_RETRY_BUCKET);
                logger.debug("Deleted key {} and value from retry bucket.", timestamp);
            }
        }
    }

    /**
     * Creates the processes key set.
     *
     * @param processedKeys the processed keys
     * @param timestamp the timestamp
     * @param retryRecordKey the retry record key
     */
    private void createProcessesKeySet(Set<String> processedKeys, long timestamp, String retryRecordKey) {
        if (!processedKeys.contains(retryRecordKey)) {
            RetryRecordKey retryEventKey = new RetryRecordKey(retryRecordKey, taskId);
            RetryRecord retryRecord = retryEventDAO.get(retryEventKey);
            try {
                if (retryRecord != null) {
                    logger.debug("Retrying key {} and value {} form timestamp {} bucket.",
                            retryRecord.getIgniteKey(), retryRecord.getDeviceMessage(), timestamp);
                    retryHandle(retryRecord.getIgniteKey(), retryRecord.getDeviceMessage(), false);
                    processedKeys.add(retryRecordKey);
                } else {
                    logger.debug("Record not present/deleted from eventDao for key {} for ts {}",
                            retryRecord, retryEventKey, timestamp);
                }
            } catch (Exception e) {
                logger.error(
                        "Error occured while retrying record {} from eventDao for key {} for ts {}",
                        retryRecord, retryEventKey, timestamp, e);
                errorCounter.incErrorCounter(Optional.ofNullable(taskId), e.getClass());
            }
        }
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        retryBucketDAO.close();
        retryEventDAO.close();
        if (retryExecutor != null && !retryExecutor.isShutdown()) {
            logger.info("Shutting the SingleThreadScheduledExecutor for retry service!");
            ThreadUtils.shutdownExecutor(retryExecutor, 
                    org.eclipse.ecsp.analytics.stream.base.utils.Constants.THREAD_SLEEP_TIME_2000, false);
        }
    }

    /**
     * The Class RetryUncaughtExceptionHandler.
     */
    private class RetryUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        
        /**
         * Uncaught exception.
         *
         * @param thread the thread
         * @param t the t
         */
        @Override
        public void uncaughtException(Thread thread, Throwable t) {
            logger.error("Uncaught exception detected!. Exception is: {} ", t);
        }
    }

    /**
     * Sets the stream processing context.
     *
     * @param ctx the ctx
     */
    @Override
    public void setStreamProcessingContext(StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctx) {
        spc = ctx;
    }

    /**
     * Sets the conn status handler.
     *
     * @param connStatusHandler the new conn status handler
     */
    public void setConnStatusHandler(DeviceConnectionStatusHandler connStatusHandler) {
        this.connStatusHandler = connStatusHandler;
    }
}