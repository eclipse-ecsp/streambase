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

package org.eclipse.ecsp.analytics.stream.base.processors;

import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessorFilter;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.events.scheduler.CreateScheduleEventData;
import org.eclipse.ecsp.events.scheduler.DeleteScheduleEventData;
import org.eclipse.ecsp.events.scheduler.ScheduleNotificationEventData;
import org.eclipse.ecsp.events.scheduler.ScheduleOpStatusEventData;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageUtils;
import org.eclipse.ecsp.stream.dma.scheduler.DeviceMessagingEventScheduler;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.SCHEDULER_AGENT_TOPIC_NAME;

/**
 * SchedulerAgentPostProcessor is responsible for accepting create schedule
 * and delete schedule events, and forwards them directly to Kafka
 * SCHEDULER AGENT topic. It is also responsible for handling scheduler notifications and acknowledgement events.
 * Event which has to be sent to Kafka SCHEDULER AGENT topic has to
 * be of type {@link CreateScheduleEventData} or {@link DeleteScheduleEventData} ,
 * otherwise the events are forwarded to next handler in the post processor chain.
 *
 * @author KJalawadi
 */
@Service
@ConditionalOnProperty(name = PropertyNames.SCHEDULER_ENABLED, havingValue = "true")
public class SchedulerAgentPostProcessor implements IgniteEventStreamProcessor, StreamProcessorFilter {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(SchedulerAgentPostProcessor.class);

    /** The scheduler agent topic. */
    @Value("${" + SCHEDULER_AGENT_TOPIC_NAME + "}")
    @NonNull
    private String schedulerAgentTopic;

    /** The ttl expiry notification enabled. */
    @Value("${" + PropertyNames.DMA_TTL_EXPIRY_NOTIFICATION_ENABLED + ":true}")
    private String ttlExpiryNotificationEnabled;

    /** The dma enabled. */
    @Value("${" + PropertyNames.DMA_ENABLED + ":true}")
    private String dmaEnabled;
    
    /** The remove on ttl expiry enabled. */
    @Value("${" + PropertyNames.DMA_REMOVE_ON_TTL_EXPIRY_ENABLED + ":true}")
    private String removeOnTtlExpiryEnabled;

    /** The ctxt. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctxt;

    /** The offline buffer dao. */
    @Autowired(required = false)
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDao;

    /** The device message utils. */
    @Autowired
    private DeviceMessageUtils deviceMessageUtils;

    /** The event scheduler. */
    @Autowired(required = false)
    private DeviceMessagingEventScheduler eventScheduler;

    /**
     * Inits the.
     *
     * @param spc the spc
     */
    @Override
    public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        this.ctxt = spc;
        if (Boolean.parseBoolean(ttlExpiryNotificationEnabled) 
                && Boolean.parseBoolean(dmaEnabled)) {
            logger.debug("SchedulerAgentPostProcessor initialized with ttlExpiryNotification and dma enabled.");
            removeOfflineBufferEntriesWithExpiredTtl();
            eventScheduler.scheduleEvent(spc);
        }
    }

    /**
     * Inits the config.
     *
     * @param props the props
     */
    @Override
    public void initConfig(Properties props) {
        //
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return "SchedulerAgent";
    }

    /**
     * Process.
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
        IgniteKey<?> key = kafkaRecord.key();
        IgniteEvent value = kafkaRecord.value();
        logger.debug(value, "SchedulerAgentPostProcessor event received is: key={}, value={}", 
                key.toString(), value.toString());
        if ((EventID.CREATE_SCHEDULE_EVENT.equals(value.getEventId()) 
                && value.getEventData() instanceof CreateScheduleEventData)
                || (EventID.DELETE_SCHEDULE_EVENT.equals(value.getEventId())
                        && value.getEventData() instanceof DeleteScheduleEventData)) {
            logger.debug(value, "SchedulerAgentPostProcessor forwarding schedule event directly to "
                    + "ScheduleEventProcessor: " + "key={}, value={}, topic={}", key.toString(), 
                    value.toString(), schedulerAgentTopic);
            this.ctxt.forwardDirectly(key, value, schedulerAgentTopic);
        }

        // WI-374794 Added handling for ScheduleOpStatusEvent
        if (EventID.SCHEDULE_OP_STATUS_EVENT.equals(value.getEventId())
                && value.getEventData() instanceof ScheduleOpStatusEventData) {
            logger.debug(value, "SchedulerAgentPostProcessor received " + "acknowledgement for "
                    + "scheduled event with key={}, " + "value={}", key.toString(), value.toString());

            ScheduleOpStatusEventData eventData = (ScheduleOpStatusEventData) value.getEventData();
            if (eventData.getStatusErrorCode() != null) {
                logger.error(value, "Scheduler service failed to create/delete a scheduler with error code : {}",
                        eventData.getStatusErrorCode().toString());
            }
        }

        // WI-374794 Added handling for ScheduleNotificationEvent
        if (EventID.SCHEDULE_NOTIFICATION_EVENT.equals(value.getEventId()) 
                && value.getEventData() instanceof ScheduleNotificationEventData) {
            logger.info(value, "SchedulerAgentPostProcessor received notification for scheduled event with key={}, "
                    + "value={}", key.toString(), value.toString());
            if (Boolean.parseBoolean(dmaEnabled)) {
                removeOfflineBufferEntriesWithExpiredTtl();
                eventScheduler.scheduleEvent(ctxt);
            }
        }

        logger.debug(value, "SchedulerAgentPostProcessor forwarding event to next post processor in chain: "
                + "key={}, value={}", key.toString(), value.toString());
        this.ctxt.forward(kafkaRecord);
    }

    /**
     * Remove entries from offline buffer collection for which TTL has expired.
     * Send failure event to feedback topic for such entries.
     */
    private void removeOfflineBufferEntriesWithExpiredTtl() {
        List<DMOfflineBufferEntry> offlineEntries = offlineBufferDao.getOfflineBufferEntriesWithExpiredTtl();
        if (null != offlineEntries && !offlineEntries.isEmpty()) {

            offlineEntries.parallelStream().forEach(offlineEntry -> {
                DeviceMessage event = offlineEntry.getEvent();
                DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
                failEventData.setFailedIgniteEvent(event.getEvent());
                failEventData.setErrorCode(DeviceMessageErrorCode.DEVICE_DELIVERY_CUTOFF_EXCEEDED);
                failEventData.setDeviceDeliveryCutoffExceeded(true);
                deviceMessageUtils.postFailureEvent(failEventData,
                        offlineEntry.getIgniteKey(), ctxt, event.getFeedBackTopic());
                logger.info("For key {} and value {} cutoff exceeded, "
                                + "will not send it to device. Failure event posted to {}",
                        offlineEntry.getIgniteKey(), event, event.getFeedBackTopic());
                if (Boolean.parseBoolean(removeOnTtlExpiryEnabled)) {
                    offlineBufferDao.removeOfflineBufferEntry(offlineEntry.getId());
                    logger.info("Removed offline entry with id {} and key {} with expired TTL", 
                              offlineEntry.getId(), offlineEntry.getIgniteKey());
                } else {
                    offlineEntry.setTtlNotifProcessed(true);
                    offlineBufferDao.update(offlineEntry);
                }
            });
        }
    }

    /**
     * Punctuate.
     *
     * @param timestamp the timestamp
     */
    @Override
    public void punctuate(long timestamp) {
        // Overridden method
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        // Overridden method
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        // Overridden method
    }

    /**
     * Creates the state store.
     *
     * @return the harman persistent KV store
     */
    @Override
    public HarmanPersistentKVStore createStateStore() {
        return null;
    }

    /**
     * returns if current stream processor is enabled or not.
     *
     * @param props props
     * @return boolean
     */
    @Override
    public boolean includeInProcessorChain(Properties props) {
        return Boolean.parseBoolean(props.getProperty(PropertyNames.SCHEDULER_ENABLED));
    }
}
