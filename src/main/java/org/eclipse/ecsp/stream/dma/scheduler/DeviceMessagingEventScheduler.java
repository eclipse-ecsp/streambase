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

package org.eclipse.ecsp.stream.dma.scheduler;

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.GlobalMessageIdGenerator;
import org.eclipse.ecsp.analytics.stream.base.utils.JsonUtils;
import org.eclipse.ecsp.domain.EventAttribute;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.events.scheduler.CreateScheduleEventData;
import org.eclipse.ecsp.events.scheduler.CreateScheduleEventData.RecurrenceType;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.dao.DMNextTtlExpirationTimer;
import org.eclipse.ecsp.stream.dma.dao.DMNextTtlExpirationTimerDAOImpl;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import static org.eclipse.ecsp.stream.dma.dao.DMAConstants.DM_NEXT_TTL_EXPIRATION_TIMER_KEY;


/**
 * DeviceMessagingEventScheduler is responsible to schedule events with ignite-scheduler.
 * It also updates entry in DMNextTtlExpirationTimer
 * with the timer for job scheduled
 *
 * @author karora
 */
@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DeviceMessagingEventScheduler {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceMessagingEventScheduler.class);
    
    /** The Constant FIRING_COUNT. */
    private static final int FIRING_COUNT = 1;
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The source topics. */
    @Value("#{'${" + PropertyNames.SOURCE_TOPIC_NAME + "}'.split(',')}")
    private List<String> sourceTopics;
    
    /** The dma notification topic. */
    @Value("${" + PropertyNames.DMA_NOTIFICATION_TOPIC_NAME + ":}")
    private String dmaNotificationTopic;
    
    /** The scheduler agent topic. */
    @Value("${" + PropertyNames.SCHEDULER_AGENT_TOPIC_NAME + "}")
    private String schedulerAgentTopic;
    
    /** The global message id generator. */
    @Autowired
    private GlobalMessageIdGenerator globalMessageIdGenerator;
    
    /** The offline buffer DAO. */
    @Autowired
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;
    
    /** The dm next ttl expiration timer DAO. */
    @Autowired
    private DMNextTtlExpirationTimerDAOImpl dmNextTtlExpirationTimerDAO;
    
    /** The spc. */
    private StreamProcessingContext<?, ?> spc;
    

    /**
     * Schedule a event with ignite-scheduler based on the entry with earliest TTL expiry in offline buffer collection.
     *
     * @param ctx StreamProcessingContext
     */
    public void scheduleEvent(StreamProcessingContext<?, ?> ctx) {

        setContext(ctx);
        DMOfflineBufferEntry offlineEntry = getEntryWithEarliestTtl();
        if (offlineEntry == null) {
            // remove entry for scheduler executed, if no latest TTL available
            dmNextTtlExpirationTimerDAO.deleteById(DM_NEXT_TTL_EXPIRATION_TIMER_KEY);
            return;
        }
        createAndSendEvent(offlineEntry.getIgniteKey(), offlineEntry.getEvent(), offlineEntry.getTtlExpirationTime());
    }
    
    /**
     * Schedule a event with ignite-scheduler based on the device delivery cutoff time
     * of entity passed as parameter.

     * @param key The IgniteKey
     * @param entity The DeviceMessage 
     * @param ctx StreamProcessingContext instance
     */
    public void scheduleEvent(@SuppressWarnings("rawtypes") IgniteKey key, DeviceMessage entity, 
            StreamProcessingContext ctx) {
        setContext(ctx);
        DeviceMessageHeader msgHeader = entity.getDeviceMessageHeader();
        long deviceDeliveryCutOff = msgHeader.getDeviceDeliveryCutoff();
        
        // Event not scheduled if valid value not present for device delivery cutoff
        if (deviceDeliveryCutOff < 0) {
            return;
        }
        long currentScheduledTime = getCurrentScheduledTimer();
        if (currentScheduledTime != 0 && currentScheduledTime < deviceDeliveryCutOff) {
            logger.debug("Scheduler existing with time scheduled {}. No scheduler created for message with id: {}, "
                    + "vehicleId: {}, deviceDeliveryCutOff : {}", currentScheduledTime, msgHeader.getMessageId(), 
                    msgHeader.getVehicleId(), deviceDeliveryCutOff);
            return;
        }
        createAndSendEvent(key, entity, deviceDeliveryCutOff);
    }

    /**
     * Creates the and send event.
     *
     * @param key the key
     * @param event the event
     * @param ttlExpirationTime the ttl expiration time
     */
    private void createAndSendEvent(@SuppressWarnings("rawtypes") IgniteKey key, DeviceMessage event,
            long ttlExpirationTime) {

        saveNextTtlExpirationTime(ttlExpirationTime);
        IgniteEventImpl createScheduleEvent = createEvent(event, getInitialDelay(ttlExpirationTime));
        logger.info("Sending event to topic : {} to create scheduler job : {}, scheduled to run at : {}",
                schedulerAgentTopic, createScheduleEvent, Instant.ofEpochSecond(ttlExpirationTime));
        spc.forwardDirectly(key, createScheduleEvent, schedulerAgentTopic);
    }

    /**
     * Save next ttl expiration time.
     *
     * @param time the time
     */
    private void saveNextTtlExpirationTime(long time) {
        DMNextTtlExpirationTimer timer = new DMNextTtlExpirationTimer(time);
        dmNextTtlExpirationTimerDAO.update(timer);
        logger.debug("Timer set in dmNextTtlExpirationTimer {}, for service : {} ", timer, serviceName);
    }

    /**
     * Creates the event.
     *
     * @param event the event
     * @param initialDelay the initial delay
     * @return the ignite event impl
     */
    private IgniteEventImpl createEvent(DeviceMessage event, long initialDelay) {

        DeviceMessageHeader msgHeader = event.getDeviceMessageHeader();
        CreateScheduleEventData createEventData = new CreateScheduleEventData();
        if (sourceTopics.isEmpty() && !StringUtils.hasText(dmaNotificationTopic)) {
            logger.error("Failed to create scheduler event for vehicleId {}, requestId {}."
                            + " Source topic and notification topic is empty. ",
                    msgHeader.getVehicleId(), msgHeader.getRequestId());
            return null;
        }
        if (StringUtils.hasText(dmaNotificationTopic)) {
            createEventData.setNotificationTopic(dmaNotificationTopic);
        } else {
            createEventData.setNotificationTopic(sourceTopics.get(0));
        }
        createEventData.setServiceName(serviceName);
        createEventData.setRecurrenceType(RecurrenceType.CUSTOM_MS);
        createEventData.setFiringCount(FIRING_COUNT);
        createEventData.setInitialDelayMs(initialDelay);
        createEventData.setNotificationKey(new IgniteStringKey(msgHeader.getVehicleId()));
        createEventData.setNotificationPayload(getNotificationPayload(msgHeader));

        IgniteEventImpl createScheduleEvent = new IgniteEventImpl();
        createScheduleEvent.setEventId(EventID.CREATE_SCHEDULE_EVENT);
        createScheduleEvent.setTimestamp(System.currentTimeMillis());
        createScheduleEvent.setVersion(Version.V1_0);
        createScheduleEvent.setRequestId(msgHeader.getRequestId());
        createScheduleEvent.setMessageId(globalMessageIdGenerator.generateUniqueMsgId(msgHeader.getVehicleId()));
        createScheduleEvent.setSourceDeviceId(msgHeader.getVehicleId());
        createScheduleEvent.setVehicleId(msgHeader.getVehicleId());
        createScheduleEvent.setEventData(createEventData);

        return createScheduleEvent;
    }

    /**
     * query dmNextTtlExpirationTimer collection to get the current scheduled timer against service name.
     *
     * @return long
     */
    private long getCurrentScheduledTimer() {
        DMNextTtlExpirationTimer dmNextTtlExpirationTimer = 
                dmNextTtlExpirationTimerDAO.findById(DM_NEXT_TTL_EXPIRATION_TIMER_KEY);
        if (dmNextTtlExpirationTimer != null) {
            return dmNextTtlExpirationTimer.getTtlExpirationTimer();
        } else {
            logger.debug("No timer set for scheduling job against key: {} in dmNextTtlExpirationTimer, for service: {}",
                    DM_NEXT_TTL_EXPIRATION_TIMER_KEY, serviceName);
            return 0;
        }
    }

    /**
     * get the time at which scheduler will be executed. return initial delay as 1ms if ttl already expired.
     *
     * @param ttlExpirationTime ttlExpirationTime
     * @return long
     */
    private long getInitialDelay(long ttlExpirationTime) {
        long initialDelay = ttlExpirationTime - System.currentTimeMillis();
        return initialDelay > 0 ? initialDelay : 1L;
    }

    /**
     * Gets the entry with earliest ttl.
     *
     * @return the entry with earliest ttl
     */
    private DMOfflineBufferEntry getEntryWithEarliestTtl() {
        return offlineBufferDAO.getOfflineBufferEntryWithEarliestTtl();
    }

    /**
     * Gets the notification payload.
     *
     * @param msgHeader the msg header
     * @return the notification payload
     */
    private byte[] getNotificationPayload(DeviceMessageHeader msgHeader) {
        HashMap<String, String> notificationPaylod = new HashMap<>();
        notificationPaylod.put(EventAttribute.REQUEST_ID, msgHeader.getRequestId());
        notificationPaylod.put(EventAttribute.MESSAGE_ID, msgHeader.getMessageId());
        notificationPaylod.put(EventAttribute.DEVICE_DELIVERY_CUTOFF,
                String.valueOf(msgHeader.getDeviceDeliveryCutoff()));
        notificationPaylod.put(EventAttribute.VEHICLE_ID, msgHeader.getVehicleId());
        notificationPaylod.put(PropertyNames.SERVICE_NAME, serviceName);
        notificationPaylod.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopics.toString());
        notificationPaylod.put(PropertyNames.DMA_NOTIFICATION_TOPIC_NAME, dmaNotificationTopic);
        logger.debug("Schedule Notification payload: {}", notificationPaylod.toString());

        return JsonUtils.getObjectValueAsString(notificationPaylod).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Sets the context.
     *
     * @param ctx the ctx
     */
    private void setContext(StreamProcessingContext<?, ?> ctx) {
        spc = ctx;
    }
}
