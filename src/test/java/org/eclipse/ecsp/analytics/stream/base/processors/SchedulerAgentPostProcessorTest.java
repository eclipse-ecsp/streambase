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
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.SpeedV1_0;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.events.scheduler.CreateScheduleEventData;
import org.eclipse.ecsp.events.scheduler.DeleteScheduleEventData;
import org.eclipse.ecsp.events.scheduler.ScheduleNotificationEventData;
import org.eclipse.ecsp.events.scheduler.ScheduleOpStatusEventData;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageUtils;
import org.eclipse.ecsp.stream.dma.scheduler.DeviceMessagingEventScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;



/**
 * UT class for {@link SchedulerAgentPostProcessor}.
 */
public class SchedulerAgentPostProcessorTest {

    /** The Constant FEEDBACK_TOPIC. */
    private static final String FEEDBACK_TOPIC = "testTopic";
    
    /** The scheduler agent post processor. */
    @InjectMocks
    private SchedulerAgentPostProcessor schedulerAgentPostProcessor;
    
    /** The ctxt. */
    @Mock
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctxt;
    
    /** The event scheduler. */
    @Mock
    private DeviceMessagingEventScheduler eventScheduler;
    
    /** The offline buffer DAO. */
    @Mock
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;
    
    /** The device message utils. */
    @Mock
    private DeviceMessageUtils deviceMessageUtils;

    /**
     * to initialize properties.
     *
     * @throws Exception Exception
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        ReflectionTestUtils.setField(schedulerAgentPostProcessor, "schedulerAgentTopic",
                "scheduler");
        ReflectionTestUtils.setField(schedulerAgentPostProcessor, "ttlExpiryNotificationEnabled",
                "true");
        ReflectionTestUtils.setField(schedulerAgentPostProcessor, "dmaEnabled",
                "true");
        ReflectionTestUtils.setField(schedulerAgentPostProcessor, "removeOnTtlExpiryEnabled",
                "true");
    }
    
    /**
     * Test process schedule notification event without removal.
     */
    @Test
    public void testProcess_ScheduleNotificationEventWithoutRemoval() {
        ReflectionTestUtils.setField(schedulerAgentPostProcessor, "removeOnTtlExpiryEnabled",
                "false");
        DMOfflineBufferEntry entry = new DMOfflineBufferEntry();
        DeviceMessage deviceMessage = new DeviceMessage();
        deviceMessage.setEvent(new TestEvent());
        deviceMessage.setFeedBackTopic(FEEDBACK_TOPIC);
        IgniteKey<String> key = new IgniteStringKey("testKey");
        entry.setEvent(deviceMessage);
        entry.setIgniteKey(key);
        entry.setTtlNotifProcessed(false);
        List<DMOfflineBufferEntry> offlineEntries = new ArrayList<>();
        offlineEntries.add(entry);

        TestEvent event = new TestEvent();
        event.setEventId(EventID.SCHEDULE_NOTIFICATION_EVENT);
        ScheduleNotificationEventData eventData = new ScheduleNotificationEventData();
        eventData.setScheduleIdId("test1");
        eventData.setTriggerTimeMs(1L);
        event.setEventData(eventData);
        IgniteKey<String> testKey = new TestKey();
        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesWithExpiredTtl()).thenReturn(offlineEntries);
        
        schedulerAgentPostProcessor.process(new Record<>(testKey, event, System.currentTimeMillis()));
        
        Mockito.verify(deviceMessageUtils, Mockito.times(1))
            .postFailureEvent(ArgumentMatchers.any(DeviceMessageFailureEventDataV1_0.class), 
                ArgumentMatchers.any(IgniteKey.class), 
                ArgumentMatchers.any(StreamProcessingContext.class), ArgumentMatchers.contains(FEEDBACK_TOPIC));
        Assert.assertEquals(true, entry.isTtlNotifProcessed());
        
        Mockito.verify(offlineBufferDAO, Mockito.times(0)).removeOfflineBufferEntry(ArgumentMatchers.any());
        Mockito.verify(offlineBufferDAO, Mockito.times(1)).update(ArgumentMatchers.any(DMOfflineBufferEntry.class));
        Mockito.verify(ctxt, Mockito.times(1)).forward(ArgumentMatchers.<Record<IgniteKey<?>, IgniteEvent>>any());
        Mockito.verify(eventScheduler, Mockito.times(1))
            .scheduleEvent(ArgumentMatchers.any(StreamProcessingContext.class));
    }

    /**
     * Test process create schedule event.
     */
    @Test
    public void testProcess_CreateScheduleEvent() {
        IgniteKey<String> testKey = new TestKey();
        TestEvent event = new TestEvent();
        event.setEventId(EventID.CREATE_SCHEDULE_EVENT);

        CreateScheduleEventData createScheduleEventData = new CreateScheduleEventData();
        event.setEventData(createScheduleEventData);

        schedulerAgentPostProcessor.process(new Record<>(testKey, event, System.currentTimeMillis()));

        Mockito.verify(ctxt,
                Mockito.times(1)).forwardDirectly(ArgumentMatchers.any(TestKey.class),
                ArgumentMatchers.any(TestEvent.class),
                ArgumentMatchers.any(String.class));

        Mockito.verify(ctxt, Mockito.times(1)).forward(ArgumentMatchers.<Record<IgniteKey<?>, IgniteEvent>>any());

    }

    /**
     * Test process schedule op status event.
     */
    @Test
    public void testProcess_ScheduleOpStatusEvent() {
        TestEvent event = new TestEvent();
        event.setEventId(EventID.SCHEDULE_OP_STATUS_EVENT);

        ScheduleOpStatusEventData eventData = new ScheduleOpStatusEventData();
        eventData.setValid(true);
        eventData.setScheduleId("testId");
        event.setEventData(eventData);
        IgniteKey<String> testKey = new TestKey();
        schedulerAgentPostProcessor.process(new Record<>(testKey, event, System.currentTimeMillis()));

        Mockito.verify(ctxt, Mockito.times(1)).forward(ArgumentMatchers.<Record<IgniteKey<?>, IgniteEvent>>any());
    }

    /**
     * Test process schedule notification event.
     */
    @Test
    public void testProcess_ScheduleNotificationEvent() {

        List<DMOfflineBufferEntry> offlineEntries = new ArrayList<>();

        for (int i = 0; i < Constants.THREE; i++) {
            DMOfflineBufferEntry entry = new DMOfflineBufferEntry();

            DeviceMessage deviceMessage = new DeviceMessage();
            deviceMessage.setEvent(new TestEvent());
            deviceMessage.setFeedBackTopic(FEEDBACK_TOPIC);

            entry.setEvent(deviceMessage);
            IgniteKey<String> key = new IgniteStringKey(i + "");
            entry.setIgniteKey(key);
            offlineEntries.add(entry);
        }

        TestEvent event = new TestEvent();
        event.setEventId(EventID.SCHEDULE_NOTIFICATION_EVENT);
        ScheduleNotificationEventData eventData = new ScheduleNotificationEventData();
        eventData.setScheduleIdId("test1");
        eventData.setTriggerTimeMs(1L);
        event.setEventData(eventData);

        Mockito.when(offlineBufferDAO.getOfflineBufferEntriesWithExpiredTtl()).thenReturn(offlineEntries);
        IgniteKey<String> testKey = new TestKey();
        schedulerAgentPostProcessor.process(new Record<>(testKey, event, System.currentTimeMillis()));        
        Mockito.verify(deviceMessageUtils, Mockito.times(Constants.THREE))
            .postFailureEvent(ArgumentMatchers.any(DeviceMessageFailureEventDataV1_0.class), 
                    ArgumentMatchers.any(IgniteKey.class), ArgumentMatchers.any(StreamProcessingContext.class), 
                    ArgumentMatchers.contains(FEEDBACK_TOPIC));
        for (int i = 0; i < Constants.THREE; i++) {
            Mockito.verify(offlineBufferDAO, Mockito.times(1))
                .removeOfflineBufferEntry(ArgumentMatchers.contains(offlineEntries.get(i).getId()));
        }
        Mockito.verify(ctxt, Mockito.times(1)).forward(ArgumentMatchers.<Record<IgniteKey<?>, IgniteEvent>>any());
        Mockito.verify(eventScheduler, Mockito.times(1))
                .scheduleEvent(ArgumentMatchers.any(StreamProcessingContext.class));
    }

    /**
     * Test init.
     */
    @Test
    public void testInit() {

        schedulerAgentPostProcessor.init(ctxt);
        Mockito.verify(eventScheduler, Mockito.times(1))
                .scheduleEvent(ArgumentMatchers.any(StreamProcessingContext.class));
    }

    /**
     * Test process delete schedule event.
     */
    @Test
    public void testProcess_DeleteScheduleEvent() {
        IgniteKey<String> testKey = new TestKey();
        TestEvent event = new TestEvent();
        event.setEventId(EventID.DELETE_SCHEDULE_EVENT);

        String scheduleId = "123";
        DeleteScheduleEventData eventData = new DeleteScheduleEventData(scheduleId);
        event.setEventData(eventData);

        schedulerAgentPostProcessor.process(new Record<>(testKey, event, System.currentTimeMillis()));

        Mockito.verify(ctxt,
                Mockito.times(1)).forwardDirectly(ArgumentMatchers.any(TestKey.class),
                ArgumentMatchers.any(TestEvent.class), ArgumentMatchers.any(String.class));

        Mockito.verify(ctxt, Mockito.times(1)).forward(ArgumentMatchers.<Record<IgniteKey<?>, IgniteEvent>>any());

    }

    /**
     * Test process non scheduler events.
     */
    @Test
    public void testProcess_NonSchedulerEvents() {
        IgniteKey<String> testKey = new TestKey();
        TestEvent event = new TestEvent();
        SpeedV1_0 speed = new SpeedV1_0();
        event.setEventData(speed);
        schedulerAgentPostProcessor.process(new Record<>(testKey, event, System.currentTimeMillis()));
        Mockito.verify(ctxt,
                Mockito.times(0)).forwardDirectly(ArgumentMatchers.any(TestKey.class),
                ArgumentMatchers.any(TestEvent.class), ArgumentMatchers.any(String.class));
        Mockito.verify(ctxt, Mockito.times(1)).forward(ArgumentMatchers.<Record<IgniteKey<?>, IgniteEvent>>any());
    }

    /**
     * Test process misc methods.
     */
    @Test
    public void testProcess_MiscMethods() {
        schedulerAgentPostProcessor.init(ctxt);
        schedulerAgentPostProcessor.initConfig(new Properties());
        schedulerAgentPostProcessor.configChanged(new Properties());
        Assert.assertEquals("SchedulerAgent", schedulerAgentPostProcessor.name());
        schedulerAgentPostProcessor.punctuate(new Date().getTime());
        schedulerAgentPostProcessor.createStateStore();
        schedulerAgentPostProcessor.close();
    }

    /**
     * The Class TestKey.
     */
    private class TestKey implements IgniteKey<String> {
        
        /**
         * Gets the key.
         *
         * @return the key
         */
        @Override
        public String getKey() {

            return "Vehicle12345";
        }
    }

    /**
     * inner class TestEvent extends IgniteEventImpl.
     */
    public class TestEvent extends IgniteEventImpl {
        
        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /**
         * Instantiates a new test event.
         */
        public TestEvent() {
            // Nothing to do.
        }

        /**
         * Gets the target device id.
         *
         * @return the target device id
         */
        @Override
        public Optional<String> getTargetDeviceId() {
            return Optional.of("test");
        }
    }
}
