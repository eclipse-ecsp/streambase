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

package org.eclipse.ecsp.stream.dma;

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.GlobalMessageIdGenerator;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.dao.DMNextTtlExpirationTimer;
import org.eclipse.ecsp.stream.dma.dao.DMNextTtlExpirationTimerDAOImpl;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.scheduler.DeviceMessagingEventScheduler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.List;


/**
 * {@link DeviceMessagingEventScheduler} UT class for {@link DeviceMessagingEventSchedulerTest}.
 *
 * @author karora
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DeviceMessagingEventSchedulerTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The device messaging event scheduler. */
    @InjectMocks
    private DeviceMessagingEventScheduler deviceMessagingEventScheduler;

    /** The ctxt. */
    @Mock
    private StreamProcessingContext ctxt;

    /** The global message id generator. */
    @Mock
    private GlobalMessageIdGenerator globalMessageIdGenerator;

    /** The offline buffer DAO. */
    @Mock
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;

    /** The dm next ttl expiration timer DAO. */
    @Mock
    private DMNextTtlExpirationTimerDAOImpl dmNextTtlExpirationTimerDAO;

    /**
     * setUp().
     *
     * @throws Exception Exception
     */
    @Before
    public void setUp() throws Exception {

        MockitoAnnotations.initMocks(this);

        List<String> sourceTopics = Arrays.asList("topic1", "topic2");
        ReflectionTestUtils.setField(deviceMessagingEventScheduler, "schedulerAgentTopic", "scheduler");
        ReflectionTestUtils.setField(deviceMessagingEventScheduler, "sourceTopics", sourceTopics);
        ReflectionTestUtils.setField(deviceMessagingEventScheduler, "dmaNotificationTopic", "dma-noti-topic");
    }

    /**
     * Test schedule event new device message with earlier TTL.
     */
    @Test
    public void testScheduleEvent_newDeviceMessage_withEarlierTTL() {

        long currTime = System.currentTimeMillis();
        long deliveryCutOff = currTime + (Constants.TWENTY * Constants.THREAD_SLEEP_TIME_1000 * 1);

        DeviceMessageHeader msgHeader = new DeviceMessageHeader();
        msgHeader.withDeviceDeliveryCutoff(deliveryCutOff);
        msgHeader.withRequestId("requestId123");
        msgHeader.withVehicleId("Vehicle1");
        msgHeader.withMessageId("msgId123");

        DeviceMessage entity = new DeviceMessage();
        entity.setFeedBackTopic("ro");
        entity.setDeviceMessageHeader(msgHeader);

        long currSchTime = deliveryCutOff + (Constants.TWENTY * Constants.THREAD_SLEEP_TIME_1000 * 1);
        DMNextTtlExpirationTimer timer = new DMNextTtlExpirationTimer(currSchTime);
        Mockito.when(dmNextTtlExpirationTimerDAO.findById(ArgumentMatchers.anyString())).thenReturn(timer);
        IgniteStringKey key = new IgniteStringKey("testEventKey");
        deviceMessagingEventScheduler.scheduleEvent(key, entity, ctxt);

        Mockito.verify(ctxt, Mockito.times(1))
                .forwardDirectly(ArgumentMatchers.any(IgniteKey.class), ArgumentMatchers.any(IgniteEventImpl.class),
                        ArgumentMatchers.startsWith("scheduler"));
    }

    /**
     * Test schedule event new device message with later TTL.
     */
    @Test
    public void testScheduleEvent_newDeviceMessage_withLaterTTL() {

        long currTime = System.currentTimeMillis();
        long deliveryCutOff = currTime + (Constants.TWENTY * Constants.THREAD_SLEEP_TIME_1000 * 1);

        DeviceMessageHeader msgHeader = new DeviceMessageHeader();
        msgHeader.withDeviceDeliveryCutoff(deliveryCutOff);
        msgHeader.withRequestId("requestId123");
        msgHeader.withVehicleId("Vehicle1");
        msgHeader.withMessageId("msgId123");

        DeviceMessage entity = new DeviceMessage();
        entity.setFeedBackTopic("ro");
        entity.setDeviceMessageHeader(msgHeader);

        long currSchTime = deliveryCutOff - (Constants.TWENTY * Constants.THREAD_SLEEP_TIME_1000 * 1);
        DMNextTtlExpirationTimer timer = new DMNextTtlExpirationTimer(currSchTime);
        Mockito.when(dmNextTtlExpirationTimerDAO.findById(ArgumentMatchers.anyString())).thenReturn(timer);
        IgniteStringKey key = new IgniteStringKey("testEventKey");
        deviceMessagingEventScheduler.scheduleEvent(key, entity, ctxt);

        Mockito.verify(ctxt, Mockito.times(0))
                .forwardDirectly(ArgumentMatchers.any(IgniteKey.class), ArgumentMatchers.any(IgniteEventImpl.class),
                        ArgumentMatchers.startsWith("scheduler"));
    }

    /**
     * Test schedule event post notification event.
     */
    @Test
    public void testScheduleEvent_postNotificationEvent() {

        long currTime = System.currentTimeMillis();
        long deliveryCutOff = currTime + (Constants.TWENTY * Constants.THREAD_SLEEP_TIME_1000 * 1);

        DeviceMessageHeader msgHeader = new DeviceMessageHeader();
        msgHeader.withDeviceDeliveryCutoff(deliveryCutOff);
        msgHeader.withRequestId("requestId123");
        msgHeader.withVehicleId("Vehicle1");
        msgHeader.withMessageId("msgId123");

        DeviceMessage entity = new DeviceMessage();
        entity.setFeedBackTopic("ro");
        entity.setDeviceMessageHeader(msgHeader);

        DMOfflineBufferEntry entry = new DMOfflineBufferEntry();
        entry.setEvent(entity);
        IgniteKey<String> key = new IgniteStringKey("123");
        entry.setIgniteKey(key);
        entry.setTtlExpirationTime(deliveryCutOff);

        Mockito.when(offlineBufferDAO.getOfflineBufferEntryWithEarliestTtl()).thenReturn(entry);

        deviceMessagingEventScheduler.scheduleEvent(ctxt);

        Mockito.verify(ctxt, Mockito.times(1))
                .forwardDirectly(ArgumentMatchers.any(IgniteKey.class), ArgumentMatchers.any(IgniteEventImpl.class),
                        ArgumentMatchers.startsWith("scheduler"));
    }

}
