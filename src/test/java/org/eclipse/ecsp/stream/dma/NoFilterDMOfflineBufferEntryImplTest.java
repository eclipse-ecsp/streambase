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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.handler.NoFilterDMOfflineBufferEntryImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;



/**
 * UT test class for {@link NoFilterDMOfflineBufferEntryImplTest}.
 */
public class NoFilterDMOfflineBufferEntryImplTest {
    
    /** The no filter DM offline buffer entry impl. */
    @InjectMocks
    private NoFilterDMOfflineBufferEntryImpl noFilterDMOfflineBufferEntryImpl = new NoFilterDMOfflineBufferEntryImpl();

    /**
     * Sets the test filter DM offline buffer entry impl.
     */
    @Test
    public void setTestFilterDMOfflineBufferEntryImpl() {
        DMOfflineBufferEntry bufferEntry = new DMOfflineBufferEntry();
        bufferEntry.setDeviceId("vehicle1");
        DeviceMessage event = new DeviceMessage();
        IgniteEventImpl eventImpl = new IgniteEventImpl();
        String eventId = "eventId1";
        eventImpl.setEventId(eventId);
        event.setEvent(eventImpl);
        DeviceMessageHeader deviceMessageHeader = new DeviceMessageHeader();
        deviceMessageHeader.withRequestId("reqId1");
        event.setDeviceMessageHeader(deviceMessageHeader);
        bufferEntry.setEvent(event);
        LocalDateTime eventTs = LocalDateTime.now();
        bufferEntry.setEventTs(eventTs);
        IgniteStringKey igniteKey = new IgniteStringKey();
        igniteKey.setKey("Vehicle12345");
        bufferEntry.setIgniteKey(igniteKey);
        bufferEntry.setVehicleId("vehicleId");
        List<DMOfflineBufferEntry> bufferedEntries = new ArrayList<DMOfflineBufferEntry>();
        bufferedEntries.add(bufferEntry);

        DMOfflineBufferEntry bufferEntry2 = new DMOfflineBufferEntry();
        bufferEntry2.setDeviceId("vehicle2");
        DeviceMessage event2 = new DeviceMessage();
        IgniteEventImpl eventImpl2 = new IgniteEventImpl();
        String eventId2 = "eventId2";
        eventImpl2.setEventId(eventId2);
        DeviceMessageHeader deviceMessageHeader2 = new DeviceMessageHeader();
        deviceMessageHeader2.withRequestId("reqId2");
        event2.setDeviceMessageHeader(deviceMessageHeader2);
        event2.setEvent(eventImpl2);
        bufferEntry2.setEvent(event2);
        LocalDateTime eventTs2 = LocalDateTime.now();
        bufferEntry2.setEventTs(eventTs2);
        IgniteStringKey igniteKey2 = new IgniteStringKey();
        igniteKey.setKey("Vehicle12345");
        bufferEntry2.setIgniteKey(igniteKey2);
        bufferEntry2.setVehicleId("Vehicle12345");
        bufferedEntries.add(bufferEntry2);

        DMOfflineBufferEntry bufferEntry3 = new DMOfflineBufferEntry();
        bufferEntry3.setDeviceId("vehicle3");
        DeviceMessage event3 = new DeviceMessage();
        IgniteEventImpl eventImpl3 = new IgniteEventImpl();
        String eventId3 = "eventId3";
        eventImpl3.setEventId(eventId3);
        DeviceMessageHeader deviceMessageHeader3 = new DeviceMessageHeader();
        deviceMessageHeader3.withRequestId("reqId3");
        event3.setDeviceMessageHeader(deviceMessageHeader3);
        event3.setEvent(eventImpl3);
        bufferEntry3.setEvent(event3);
        LocalDateTime eventTs3 = LocalDateTime.now();
        bufferEntry3.setEventTs(eventTs3);
        IgniteStringKey igniteKey3 = new IgniteStringKey();
        igniteKey.setKey("Vehicle12345");
        bufferEntry3.setIgniteKey(igniteKey3);
        bufferEntry3.setVehicleId("Vehicle12345");
        bufferedEntries.add(bufferEntry3);

        List<DMOfflineBufferEntry> testList = noFilterDMOfflineBufferEntryImpl
                .filterAndUpdateDmOfflineBufferEntries(bufferedEntries);

        Assert.assertEquals(Constants.THREE, testList.size());
    }

}
