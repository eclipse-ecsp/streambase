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

import dev.morphia.AdvancedDatastore;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.nosqldao.IgniteCriteria;
import org.eclipse.ecsp.nosqldao.IgniteCriteriaGroup;
import org.eclipse.ecsp.nosqldao.IgniteQuery;
import org.eclipse.ecsp.nosqldao.Operator;
import org.eclipse.ecsp.nosqldao.Updates;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;



/**
 * UT class for {@link DMOfflineBufferServiceImplTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@TestPropertySource("/dma-handler-test.properties")
public class DMOfflineBufferServiceImplTest {
    
    /** The Constant REDIS. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS = new EmbeddedRedisServer();
    
    /** The mongo server. */
    @ClassRule
    public static EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The offline buffer DAO. */
    @Autowired
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;

    /** The mongo datastore. */
    @Autowired
    private AdvancedDatastore mongoDatastore;

    /** The transformer. */
    @Autowired
    private DeviceMessageIgniteEventTransformer transformer;

    /**
     * Clean up collection.
     */
    @Before
    public void cleanUpCollection() {
        offlineBufferDAO.deleteAll();
    }

    /**
     * Test offline buffer service.
     */
    @Test
    public void testOfflineBufferService() {
        String vehicleId = "Vehicle1";
        for (int i = 0; i < Constants.FIVE; i++) {
            IgniteEventImpl event = new IgniteEventImpl();
            event.setTargetDeviceId("Device1");
            event.setEventId(EventID.LOCATION);
            event.setMessageId("Message_" + i);
            event.setTimestamp(Constants.INT_4736565 + Constants.TEN * i);
            event.setVehicleId(vehicleId);
            DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                    event, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
            IgniteStringKey key = new IgniteStringKey();
            key.setKey(vehicleId);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, key, entity, null);
        }
        List<DMOfflineBufferEntry> events = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.empty(), Optional.empty());
        events.forEach(event -> assertEquals("Wrong device fetched", vehicleId, event.getVehicleId()));
        assertEquals("Service failed to get expected events", Constants.FIVE, events.size());

        events.forEach(event -> offlineBufferDAO.removeOfflineBufferEntry(event.getId()));
        List<DMOfflineBufferEntry> events2 = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.empty(), Optional.empty());
        assertEquals("Service failed to get expected events", 0, events2.size());
    }

    /**
     * Test get offline buffer entry with earliest ttl.
     */
    @Test
    public void testGetOfflineBufferEntryWithEarliestTtl() {
        for (int i = 1; i < Constants.FIVE; i++) {
            IgniteEventImpl event = new IgniteEventImpl();
            String vehicleId = "Vehicle" + i;
            event.setTargetDeviceId("Device1");
            event.setEventId(EventID.LOCATION);
            event.setMessageId("Message_" + i);
            event.setTimestamp(Constants.INT_4736565 + Constants.TEN * i);
            event.setVehicleId(vehicleId);
            event.setDeviceDeliveryCutoff(Constants.INT_4792987 + Constants.TWENTY * i);
            DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                    event, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, new IgniteStringKey(String.valueOf(i)), entity, null);
        }
        DMOfflineBufferEntry entry = offlineBufferDAO.getOfflineBufferEntryWithEarliestTtl();
        IgniteEventImpl event = entry.getEvent().getEvent();
        assertEquals("Wrong device fetched", "Vehicle1", event.getVehicleId());
        assertEquals("Wrong offline entry fetched", Constants.INT_4792987
                + Constants.TWENTY * 1, entry.getTtlExpirationTime());
    }

    /**
     * Test get offline buffer entries with expired ttl.
     */
    @Test
    public void testGetOfflineBufferEntriesWithExpiredTtl() {
        long currTime = System.currentTimeMillis();
        IgniteEventImpl event;
        for (int i = 1; i < Constants.SIX; i++) {
            event = new IgniteEventImpl();
            String vehicleId = "Vehicle" + i;
            event.setTargetDeviceId("Device1");
            event.setEventId(EventID.LOCATION);
            event.setMessageId("Message_" + i);
            event.setTimestamp(currTime);
            event.setVehicleId(vehicleId);
            event.setDeviceDeliveryCutoff(i % Constants.TWO == 0 ? currTime
                    - (Constants.TWENTY * Constants.THREAD_SLEEP_TIME_1000 * i)
                    : currTime + (Constants.TWENTY * Constants.THREAD_SLEEP_TIME_1000 * i));
            DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0, event, "testTopic",
                    Constants.THREAD_SLEEP_TIME_60000);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, new IgniteStringKey(String.valueOf(i)), entity, null);
            // Add and update an entry with TTL notification processed flag as true
            // This entry should not be returned by the method even though TTL has expired
            IgniteEventImpl event2 = new IgniteEventImpl();
            String vehicleId7 = "Vehicle7";
            event2.setTargetDeviceId("Device1");
            event2.setEventId(EventID.LOCATION);
            event2.setMessageId("Message_7");
            event2.setTimestamp(currTime);
            event2.setVehicleId(vehicleId7);
            event2.setDeviceDeliveryCutoff(currTime - (TestConstants.TWENTY * TestConstants.INT_1000));
            DeviceMessage entity2 = new DeviceMessage(transformer.toBlob(event2), 
                 Version.V1_0, event2, "testTopic", TestConstants.THREAD_SLEEP_TIME_60000);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId7, 
                 new IgniteStringKey(String.valueOf(TestConstants.SEVEN)), entity2, null);
            
            IgniteCriteria igCriteria = new IgniteCriteria("vehicleId", Operator.EQ, vehicleId7);
            IgniteCriteriaGroup igniteCriteriaGroup = new IgniteCriteriaGroup(igCriteria);
            IgniteQuery query = new IgniteQuery(igniteCriteriaGroup);
            Updates u = new Updates();
            u.addFieldSet(DMAConstants.IS_TTL_NOTIF_PROCESSED_FIELD, true);
            offlineBufferDAO.update(query, u);
        }

        // add an entry with ttl as -1 i.e no TTL set by service.
        // This record should not be considered expired, and not returned by the method
        event = new IgniteEventImpl();
        String vehicleId = "Vehicle6";
        event.setTargetDeviceId("Device1");
        event.setEventId(EventID.LOCATION);
        event.setMessageId("Message_6");
        event.setTimestamp(currTime);
        event.setVehicleId(vehicleId);
        event.setDeviceDeliveryCutoff(Constants.LONG_MINUS_ONE);
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                event, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
        offlineBufferDAO.addOfflineBufferEntry(vehicleId,
                new IgniteStringKey(String.valueOf(Constants.SIX)), entity, null);

        List<DMOfflineBufferEntry> entries = offlineBufferDAO.getOfflineBufferEntriesWithExpiredTtl();
        List<String> vehicleIdList = new ArrayList<>();
        for (DMOfflineBufferEntry entry : entries) {
            vehicleIdList.add(entry.getVehicleId());
        }

        assertEquals("Incorrect number of entries returned", Constants.TWO, entries.size());
        assertEquals("Entry with expired TTL not returned", true, vehicleIdList.contains("Vehicle2"));
        assertEquals("Entry with expired TTL not returned", true, vehicleIdList.contains("Vehicle4"));
        assertEquals("Entry with TTL notification processed should not be returned", 
            false, vehicleIdList.contains("Vehicle7"));
    }

    /**
     * Test offline buffer service if sub services configured.
     */
    @Test
    public void testOfflineBufferServiceIfSubServicesConfigured() {
        String vehicleId = "Vehicle1";
        String subService1 = "ecall/test/ftd";
        String subService2 = "ecall/test/ubi";
        for (int i = 0; i < Constants.FIVE; i++) {
            IgniteEventImpl event = new IgniteEventImpl();
            event.setTargetDeviceId("Device1");
            event.setEventId(EventID.LOCATION);
            event.setMessageId("Message_" + i);
            event.setTimestamp(Constants.INT_4736565 + Constants.TEN * i);
            event.setVehicleId(vehicleId);
            event.setDevMsgTopicSuffix(subService1);
            DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0, 
                    event, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
            IgniteStringKey key = new IgniteStringKey();
            key.setKey(vehicleId);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, key, entity, subService1);
        }
        for (int i = Constants.FIVE; i < Constants.TEN; i++) {
            IgniteEventImpl event = new IgniteEventImpl();
            event.setTargetDeviceId("Device1");
            event.setEventId(EventID.LOCATION);
            event.setMessageId("Message_" + i);
            event.setTimestamp(Constants.INT_4736565 + Constants.TEN * i);
            event.setVehicleId(vehicleId);
            event.setDevMsgTopicSuffix(subService2);
            DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                    event, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
            IgniteStringKey key = new IgniteStringKey();
            key.setKey(vehicleId);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, key, entity, subService2);
        }

        List<DMOfflineBufferEntry> events = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.ofNullable("Device1"), Optional.of(subService1));

        events.forEach(event -> assertEquals("Wrong device fetched", vehicleId, event.getVehicleId()));
        events.forEach(event -> assertEquals(subService1, event.getSubService()));
        assertEquals("Service failed to get expected events", Constants.FIVE, events.size());
        events.forEach(event -> offlineBufferDAO.removeOfflineBufferEntry(event.getId()));
        List<DMOfflineBufferEntry> events2 = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.empty(), Optional.of(subService2));
        assertEquals("Service failed to get expected events", Constants.FIVE, events2.size());
        events2.forEach(event -> assertEquals(subService2, event.getSubService()));
    }

    /**
     * Test get offline buffer entries sorted by priority and device id.
     */
    @Test
    public void testGetOfflineBufferEntriesSortedByPriorityAndDeviceId() {
        String deviceId1 = "DeviceId1";
        String deviceId2 = "DeviceId2";

        String vehicleId = "Vehicle2";
        for (int i = 0; i < Constants.FIVE; i++) {
            IgniteEventImpl event = new IgniteEventImpl();
            event.setTargetDeviceId(deviceId1);
            event.setEventId(EventID.LOCATION);
            event.setMessageId("Message_" + i);
            event.setTimestamp(Constants.INT_4736565 + Constants.TEN * i);
            event.setVehicleId(vehicleId);
            DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                    event, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
            IgniteStringKey key = new IgniteStringKey();
            key.setKey(vehicleId);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, key, entity, null);

            IgniteEventImpl event2 = new IgniteEventImpl();
            event2.setTargetDeviceId(deviceId2);
            event2.setEventId(EventID.LOCATION);
            event2.setMessageId("Message_" + i);
            event2.setTimestamp(Constants.INT_4736565 + Constants.TEN * i);
            event2.setVehicleId(vehicleId);
            DeviceMessage entity2 = new DeviceMessage(transformer.toBlob(event2), Version.V1_0,
                    event2, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, key, entity2, null);

        }
        List<DMOfflineBufferEntry> eventsForDeviceId = offlineBufferDAO
                .getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.ofNullable(deviceId1), Optional.empty());
        eventsForDeviceId.forEach(event -> assertEquals("Wrong device fetched", deviceId1, event.getDeviceId()));
        assertEquals("Service failed to get expected events", Constants.FIVE, eventsForDeviceId.size());

        List<DMOfflineBufferEntry> allEventsForVehicle = offlineBufferDAO
                .getOfflineBufferEntriesSortedByPriority(vehicleId,
                true, Optional.empty(), Optional.empty());
        allEventsForVehicle.forEach(event -> assertEquals("Wrong vehicle fetched", vehicleId, event.getVehicleId()));
        assertEquals("Service failed to get expected events", Constants.TEN, allEventsForVehicle.size());
    }

    /**
     * Test offline buffer service shoulder tap enabled.
     */
    @Test
    public void testOfflineBufferServiceShoulderTapEnabled() {
        String vehicleId = "Vehicle3";
        for (int i = 0; i < Constants.FIVE; i++) {
            IgniteEventImpl event = new IgniteEventImpl();
            event.setTargetDeviceId("Device1");
            event.setEventId(EventID.LOCATION);
            event.setMessageId("Message_" + i);
            event.setTimestamp(Constants.INT_4736565 + Constants.TEN * i);
            event.setVehicleId(vehicleId);

            if (i % Constants.TWO == 0) {
                event.setShoulderTapEnabled(true);
            }

            DeviceMessage entity = new DeviceMessage(transformer.toBlob(event), Version.V1_0,
                    event, "testTopic", Constants.THREAD_SLEEP_TIME_60000);
            IgniteStringKey key = new IgniteStringKey();
            key.setKey(vehicleId);
            offlineBufferDAO.addOfflineBufferEntry(vehicleId, key, entity, null);
        }

        List<DMOfflineBufferEntry> events = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, true,
                Optional.empty(), Optional.empty());
        for (int index = 0; index < events.size(); index++) {
            DMOfflineBufferEntry event = events.get(index);

            if (index < Constants.THREE) {
                assertEquals("Device message is not shoulder tap enabled", true,
                        event.getEvent().getDeviceMessageHeader().isShoulderTapEnabled());
                assertEquals("Device message priority is not 10", Constants.TEN, event.getPriority());
            } else {
                assertEquals("Device message priority is not 0", 0, event.getPriority());
            }
            assertEquals("Wrong device fetched", vehicleId, event.getVehicleId());
        }

        events = offlineBufferDAO.getOfflineBufferEntriesSortedByPriority(vehicleId, false,
                Optional.empty(), Optional.empty());
        for (int index = 0; index < events.size(); index++) {
            DMOfflineBufferEntry event = events.get(index);

            if (index < Constants.TWO) {
                assertEquals("Device message priority is not 0", 0, event.getPriority());
            } else {
                assertEquals("Device message is not shoulder tap enabled", true,
                        event.getEvent().getDeviceMessageHeader().isShoulderTapEnabled());
                assertEquals("Device message priority is not 10", Constants.TEN, event.getPriority());
            }
            assertEquals("Wrong device fetched", vehicleId, event.getVehicleId());
        }

        assertEquals("Service failed to get expected events", Constants.FIVE, events.size());
    }
}
