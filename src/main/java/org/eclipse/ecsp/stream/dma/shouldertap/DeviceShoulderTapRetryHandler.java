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

import jakarta.annotation.PostConstruct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.utils.InternalCacheConstants;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.entities.dma.RetryRecord;
import org.eclipse.ecsp.entities.dma.RetryRecordIds;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.ShoulderTapRetryBucketDAO;
import org.eclipse.ecsp.stream.dma.dao.ShoulderTapRetryRecordDAOCacheImpl;
import org.eclipse.ecsp.stream.dma.dao.key.RetryVehicleIdKey;
import org.eclipse.ecsp.stream.dma.dao.key.ShoulderTapRetryBucketKey;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.ecsp.utils.metrics.IgniteErrorCounter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.DMA_SHOULDER_TAP_INVOKER_IMPL_CLASS;


/**
 * DeviceShoulderTapRetryHandler maintains a cache of retry entities and handles shoulder tap retries.
 *
 * @author KJalawadi
 */

@Component
@Scope("prototype")
public class DeviceShoulderTapRetryHandler {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceShoulderTapRetryHandler.class);
    
    /** The shouder tap retry executor. */
    private ScheduledExecutorService shouderTapRetryExecutor = null;

    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /** The device shoulder tap invoker impl class. */
    @Value("${" + DMA_SHOULDER_TAP_INVOKER_IMPL_CLASS
            + ": org.eclipse.ecsp.stream.dma.shouldertap.DummyShoulderTapInvokerImpl}")
    private String deviceShoulderTapInvokerImplClass;

    /** The device shoulder tap invoker. */
    private DeviceShoulderTapInvoker deviceShoulderTapInvoker;

    /** The shoulder tap retry bucket DAO. */
    @Autowired
    private ShoulderTapRetryBucketDAO shoulderTapRetryBucketDAO;

    /** The shoulder tap retry record DAO. */
    @Autowired
    private ShoulderTapRetryRecordDAOCacheImpl shoulderTapRetryRecordDAO;

    /** The task id. */
    private String taskId;

    /** The shoulder tap retry bucket map key. */
    private String shoulderTapRetryBucketMapKey;
    
    /** The shoulder tap retry event map key. */
    private String shoulderTapRetryEventMapKey;

    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /** The max retry. */
    @Value("${" + PropertyNames.SHOULDER_TAP_MAX_RETRY + ":3}")
    private int maxRetry;

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /** The retry interval. */
    @Value("${" + PropertyNames.SHOULDER_TAP_RETRY_INTERVAL_MILLIS + ":60000}")
    private long retryInterval;

    /** The retry min threshold. */
    @Value("${" + PropertyNames.SHOULDER_TAP_RETRY_MIN_THRESHOLD_MILLIS + ":1000}")
    private int retryMinThreshold;

    /** The retry interval divisor. */
    @Value("${" + PropertyNames.SHOULDER_TAP_RETRY_INTERVAL_DIVISOR + ":10}")
    private int retryIntervalDivisor;

    /** The device message utils. */
    @Autowired
    private DeviceMessageUtils deviceMessageUtils;

    /** The error counter. */
    @Autowired
    private IgniteErrorCounter errorCounter;

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
     * Gets the scheduled thread delay.
     *
     * @return the scheduled thread delay
     */
    long getScheduledThreadDelay() {
        long freq = retryInterval / retryIntervalDivisor;
        return freq > retryMinThreshold ? freq : retryMinThreshold;
    }

    /**
     * Register device.
     *
     * @param key the key
     * @param deviceMessage the device message
     * @param extraParameters the extra parameters
     * @return true, if successful
     */
    public boolean registerDevice(IgniteKey<?> key, DeviceMessage deviceMessage, Map<String, Object> extraParameters) {
        return shoulderTapRetry(key, deviceMessage, true, extraParameters);
    }

    /**
     * Shoulder tap retry.
     *
     * @param key the key
     * @param deviceMessage the device message
     * @param firstAttempt the first attempt
     * @param extraParameters the extra parameters
     * @return true, if successful
     */
    private boolean shoulderTapRetry(IgniteKey<?> key, DeviceMessage deviceMessage, boolean firstAttempt,
            Map<String, Object> extraParameters) {
        DeviceMessageHeader header = deviceMessage.getDeviceMessageHeader();
        boolean registered = false;
        long currentTime = System.currentTimeMillis();
        if (maxRetry > 0) {
            String vehicleId = header.getVehicleId();
            RetryVehicleIdKey retryEventKey = new RetryVehicleIdKey(vehicleId);
            RetryRecord event = shoulderTapRetryRecordDAO.get(retryEventKey);
            if (event != null) {
                if (firstAttempt) {
                    logger.debug("Shoulder Tap already being retried for vehicleId {}: ", vehicleId);
                    return true;
                }
                attemptRetry(event, currentTime, vehicleId, retryEventKey);
            } else if (firstAttempt) {
                addToRetryMap(currentTime, key, deviceMessage, vehicleId, retryEventKey, extraParameters);
                registered = true;
            }
        } else {
            RetryRecord event = new RetryRecord(key, deviceMessage, currentTime);
            event.setExtraParameters(extraParameters);
            event.setLastRetryTimestamp(System.currentTimeMillis());
            DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
            failEventData.setFailedIgniteEvent(event.getDeviceMessage().getEvent());
            failEventData.setErrorCode(DeviceMessageErrorCode.RETRYING_SHOULDER_TAP);
            // In Shoulder tap unlike message retry, service needs to
            // know whenever a shoulder tap is sent. Hence the attempt is zero
            // in this scenario where its the first ever shoulder tap being
            // attempted and actual shoulder tap retry starts from next attempt.
            failEventData.setShoudlerTapRetryAttempts(0);
            failEventData.setDeviceStatusInactive(true);
            deviceMessageUtils.postFailureEvent(failEventData, key, spc, deviceMessage.getFeedBackTopic());
            wakeUpDevice(event);
            logger.debug("Retry attempts is 0, for key {} and event {}", key,
                    deviceMessage);
        }
        return registered;
    }

    /**
     * Adds the to retry map.
     *
     * @param currentTime the current time
     * @param key the key
     * @param value the value
     * @param vehicleId the vehicle id
     * @param retryEventKey the retry event key
     * @param extraParameters the extra parameters
     */
    private void addToRetryMap(long currentTime, IgniteKey<?> key, DeviceMessage value, String vehicleId,
            RetryVehicleIdKey retryEventKey, Map<String, Object> extraParameters) {
        RetryRecord event = new RetryRecord(key, value, currentTime);
        event.setExtraParameters(extraParameters);
        event.setLastRetryTimestamp(currentTime);
        shoulderTapRetryRecordDAO.putToMap(shoulderTapRetryEventMapKey, retryEventKey, event, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_RECORD);
        long nextRetry = currentTime + retryInterval;
        logger.debug("Created event {} with key {} to retry event map.", event, retryEventKey.convertToString());
        ShoulderTapRetryBucketKey nextRetryKey = new ShoulderTapRetryBucketKey(nextRetry);
        shoulderTapRetryBucketDAO.update(shoulderTapRetryBucketMapKey, nextRetryKey, vehicleId);
        logger.debug("Created entry {} with timestamp {} to retry event map.", vehicleId, nextRetry);
        DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
        failEventData.setFailedIgniteEvent(event.getDeviceMessage().getEvent());
        failEventData.setErrorCode(DeviceMessageErrorCode.RETRYING_SHOULDER_TAP);
        failEventData.setShoudlerTapRetryAttempts(event.getAttempts());
        failEventData.setDeviceStatusInactive(true);
        deviceMessageUtils.postFailureEvent(failEventData, key, spc, value.getFeedBackTopic());
        wakeUpDevice(event);
    }

    /**
     * Attempt retry.
     *
     * @param event the event
     * @param currentTime the current time
     * @param vehicleId the vehicle id
     * @param retryEventKey the retry event key
     */
    private void attemptRetry(RetryRecord event, long currentTime, String vehicleId, RetryVehicleIdKey retryEventKey) {
        IgniteKey<?> key = event.getIgniteKey();
        DeviceMessage value = event.getDeviceMessage();
        // Check if number of retries has exeeded max retries, If
        // yes delete entry from retryEvent map and return true else
        // return false.
        if (event.getAttempts() >= maxRetry) {
            logger.debug("Retry exceeded maxRetry {} for key {}", maxRetry, key);
            DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
            failEventData.setFailedIgniteEvent(event.getDeviceMessage().getEvent());
            failEventData.setErrorCode(DeviceMessageErrorCode.SHOULDER_TAP_RETRY_ATTEMPTS_EXCEEDED);
            failEventData.setShoudlerTapRetryAttempts(maxRetry);
            failEventData.setDeviceStatusInactive(true);
            deviceMessageUtils.postFailureEvent(failEventData, key, spc, value.getFeedBackTopic());
            shoulderTapRetryRecordDAO.deleteFromMap(shoulderTapRetryEventMapKey, retryEventKey, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_RECORD);
            logger.debug("Deleted key {} from retryEventDAO", retryEventKey.convertToString());
        } else {
            event.addAttempt(currentTime);
            long nextRetry = currentTime + retryInterval;
            shoulderTapRetryRecordDAO.putToMap(shoulderTapRetryEventMapKey, retryEventKey, event, Optional.empty(),
                    InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_RECORD);
            logger.debug("Added event {} with key {} to retry event map.", event, retryEventKey.convertToString());
            // Add vehicleId to set of vehicleIds in retry
            // bucket keyed by timestamp
            ShoulderTapRetryBucketKey nextRetryKey = new ShoulderTapRetryBucketKey(nextRetry);
            shoulderTapRetryBucketDAO.update(shoulderTapRetryBucketMapKey, nextRetryKey, vehicleId);
            logger.debug("Added entry {} with timestamp {} to retry bucket.", vehicleId, nextRetry);
            DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
            failEventData.setFailedIgniteEvent(value.getEvent());
            failEventData.setErrorCode(DeviceMessageErrorCode.RETRYING_SHOULDER_TAP);
            failEventData.setShoudlerTapRetryAttempts(event.getAttempts());
            failEventData.setDeviceStatusInactive(true);
            deviceMessageUtils.postFailureEvent(failEventData, key, spc, value.getFeedBackTopic());
            wakeUpDevice(event);
        }
    }

    /**
     * Wake up device.
     *
     * @param retryRecord the retry record
     * @return true, if successful
     */
    private boolean wakeUpDevice(RetryRecord retryRecord) {
        boolean wakeUpStatus = false;
        DeviceMessage message = retryRecord.getDeviceMessage();
        Map<String, Object> extraParameters = retryRecord.getExtraParameters();
        DeviceMessageHeader header = message.getDeviceMessageHeader();
        wakeUpStatus = deviceShoulderTapInvoker.sendWakeUpMessage(header.getRequestId(), 
                header.getVehicleId(), extraParameters, spc);
        logger.debug("Waking up device: requestId={} vehicleId={} serviceName={} wakeUpStatus={}", 
                header.getRequestId(), header.getVehicleId(), serviceName, wakeUpStatus);
        return wakeUpStatus;
    }

    /**
     * When Device comes ACTIVE this method is invoked so that shoulder tap will not be invoked.
     *
     * @param vehicleId vehicleId
     * @return boolean
     */
    public boolean deregisterDevice(String vehicleId) {
        RetryVehicleIdKey retryEventKey = new RetryVehicleIdKey(vehicleId);
        shoulderTapRetryRecordDAO.deleteFromMap(shoulderTapRetryEventMapKey, retryEventKey, Optional.empty(),
                InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_RECORD);
        logger.debug("Deleted {} from cache", retryEventKey.convertToString());
        return true;
    }

    /**
     * setup().
     *
     * @param taskId taskId
     */
    public void setup(String taskId) {
        this.taskId = taskId;
        shoulderTapRetryBucketMapKey = ShoulderTapRetryBucketKey.getMapKey(serviceName, taskId);
        shoulderTapRetryEventMapKey = new StringBuilder().append(DMAConstants
                        .SHOULDER_TAP_RETRY_VEHICLEID).append(DMAConstants.COLON)
                .append(serviceName).append(DMAConstants.COLON).append(taskId).toString();
        if (retryMinThreshold <= 0) {
            throw new IllegalArgumentException(
                    "Shoulder tap Retry Minimum threshold " + retryMinThreshold
                            + ", should be greater than one second ");
        }
        if (retryInterval <= 0) {
            throw new IllegalArgumentException(
                    "Shoulder tap Retry Interval " + retryInterval + ", should be greater than zero ");
        }
        if (retryInterval < retryMinThreshold) {
            throw new IllegalArgumentException("Retry Interval " + retryInterval
                    + ", should be greater than Minimum threshold " + retryMinThreshold);
        }

        if (maxRetry < 0) {
            logger.warn("Max Shoulder tap retry cannot be less than 0. No retry will be attempted.");
            maxRetry = 0;
        }
        shoulderTapRetryBucketDAO.initialize(taskId);
        shoulderTapRetryRecordDAO.initialize(taskId);
        long delay = getScheduledThreadDelay();
        logger.info("Minimum threshold for shoulder tap retry is {} ; Scheduled thread delay is {}."
                + " Retry Interval for service is {}", retryMinThreshold, delay, retryInterval);
        shouderTapRetryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((thread, t1) -> logger.error("Uncaught exception detected!. "
                    + "Exception is: {}", t1));
            t.setName(Thread.currentThread().getName() + ":" + "ShoulderTapRetryHandler" + ":" + taskId);
            return t;
        });

        shouderTapRetryExecutor.scheduleWithFixedDelay(() -> {
            try {
                processRetries();
            } catch (Exception e) {
                logger.error("Error occured while retrying {}", e);
            }

        }, 0, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * processRetries().
     */
    private void processRetries() {
        /*
         * Iterate over the timestamps in map which is less than equal to
         * current timestamp.
         */
        KeyValueIterator<ShoulderTapRetryBucketKey, RetryRecordIds> headMap = shoulderTapRetryBucketDAO
                .getHead(new ShoulderTapRetryBucketKey(System.currentTimeMillis()));
        /*
         * If same keys are present in two different buckets, avoid processing
         * them twice which could lead to duplicate requests at the same time.
         * This can also occur mainly due to 2 reasons : if redis entries were
         * not properly cleared or huge delay in processing
         *
         */
        Set<String> processedKeys = new HashSet<>();
        if (headMap != null) {
            while (headMap.hasNext()) {
                KeyValue<ShoulderTapRetryBucketKey, RetryRecordIds> keyValue = headMap.next();
                ShoulderTapRetryBucketKey bucket = keyValue.key;
                long timestamp = bucket.getTimestamp();
                Set<String> vehicleIds = keyValue.value.getRecordIds();
                if (vehicleIds != null && !vehicleIds.isEmpty()) {
                    logger.trace("Processing key {} from shoulder tap retry bucket with size {} with service {}, "
                            + "taskId {}", timestamp, vehicleIds.size(), serviceName, taskId);
                    vehicleIds.forEach(vehicleId ->
                        createProcessedKeysSet(processedKeys, timestamp, vehicleId)
                    );
                } else {
                    logger.debug("No deviceIds found for retrying at ts {} and service {}", timestamp, serviceName);
                }
                shoulderTapRetryBucketDAO.deleteFromMap(shoulderTapRetryBucketMapKey, bucket, Optional.empty(),
                        InternalCacheConstants.CACHE_TYPE_SHOULDER_TAP_RETRY_BUCKET);
                logger.trace("Deleted key {} and value from retry bucket.", timestamp);
            }
        }

    }

    /**
     * Creates the processed keys set.
     *
     * @param processedKeys the processed keys
     * @param timestamp the timestamp
     * @param vehicleId the vehicle id
     */
    private void createProcessedKeysSet(Set<String> processedKeys, long timestamp, String vehicleId) {
        if (!processedKeys.contains(vehicleId)) {
            RetryVehicleIdKey shoulderTapKey = new RetryVehicleIdKey(vehicleId);
            RetryRecord retryRecord = shoulderTapRetryRecordDAO.get(shoulderTapKey);
            try {
                if (retryRecord != null) {
                    logger.trace("Retrying key {} and value {} form timestamp {} bucket.", 
                            retryRecord.getIgniteKey(), retryRecord.getDeviceMessage(), timestamp);
                    shoulderTapRetry(retryRecord.getIgniteKey(), retryRecord.getDeviceMessage(), false, 
                            retryRecord.getExtraParameters());
                    processedKeys.add(vehicleId);
                } else {
                    logger.debug("Record {} not present/deleted from eventDao for key {} for ts {}", retryRecord,
                            shoulderTapKey, timestamp);
                }
            } catch (Exception e) {
                logger.error("Error occured while retrying record {} from eventDao for key {} for ts {}", retryRecord,
                        shoulderTapKey, timestamp, e);
                errorCounter.incErrorCounter(Optional.ofNullable(taskId), e.getClass());
            }
        }
    }

    /**
     * close(): closes the opened resources.
     */
    public void close() {
        if (shouderTapRetryExecutor != null && !shouderTapRetryExecutor.isShutdown()) {
            logger.info("Shutting the SingleThreadScheduledExecutor for shoulder tap retry service!");
            shouderTapRetryExecutor.shutdown();
        }
    }

    /**
     * Sets the stream processing context.
     *
     * @param ctx the ctx
     */
    public void setStreamProcessingContext(StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctx) {
        spc = ctx;
    }

    /**
     * Inits the.
     */
    @PostConstruct
    public void init() {
        deviceShoulderTapInvoker = createDeviceShoulderTapInvoker();
    }

    /**
     * Creates the device shoulder tap invoker.
     *
     * @return the device shoulder tap invoker
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private DeviceShoulderTapInvoker createDeviceShoulderTapInvoker() {
        DeviceShoulderTapInvoker shoulderTapInvoker = null;
        try {
            Class classObject = getClass().getClassLoader().loadClass(deviceShoulderTapInvokerImplClass);
            shoulderTapInvoker = (DeviceShoulderTapInvoker) ctx.getBean(classObject);
        } catch (Exception e) {
            String msg = String.format("Failed to initialize Shoulder tap retry handler. "
                            + "%s is not available on the classpath",
                    deviceShoulderTapInvokerImplClass);
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }
        return shoulderTapInvoker;
    }

}
