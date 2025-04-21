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
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.GlobalMessageIdGenerator;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.presencemanager.DeviceFetchConnectionStatusProducer;
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


/**
 * Unit test for {@link DeviceFetchConnectionStatusProducer}.
 *
 * @author karora
 */
@RunWith(MockitoJUnitRunner.class)
public class DeviceFetchConnectionStatusProducerTest {
    
    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The fetch connection status producer. */
    @InjectMocks
    private DeviceFetchConnectionStatusProducer fetchConnectionStatusProducer;

    /** The ctxt. */
    @Mock
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctxt;

    /** The global message id generator. */
    @Mock
    private GlobalMessageIdGenerator globalMessageIdGenerator;

    /**
     * Setup for this test class.
     *
     * @throws Exception exception
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        ReflectionTestUtils.setField(fetchConnectionStatusProducer, "fetchConnectionStatusTopic", 
                "fetchConnStatusTopic");
    }

    /**
     * Test fetch connection status event.
     */
    @Test
    public void testFetchConnectionStatusEvent() {
        long currTime = System.currentTimeMillis();
        long deliveryCutOff = currTime + (TestConstants.TWENTY * TestConstants.THREAD_SLEEP_TIME_1000 * 1);
        DeviceMessageHeader msgHeader = new DeviceMessageHeader();
        msgHeader.withDeviceDeliveryCutoff(deliveryCutOff);
        msgHeader.withRequestId("requestId123");
        msgHeader.withVehicleId("Vehicle1");
        msgHeader.withMessageId("msgId123");
        msgHeader.withPlatformId("platform1");
        msgHeader.withTimestamp((short) TestConstants.INT_60);
        IgniteStringKey key = new IgniteStringKey("testEventKey");
        fetchConnectionStatusProducer.pushEventToFetchConnStatus(key, msgHeader, ctxt);
        Mockito.verify(ctxt, Mockito.times(1)).forwardDirectly(ArgumentMatchers.any(IgniteKey.class), 
                ArgumentMatchers.any(IgniteEventImpl.class), ArgumentMatchers.startsWith("fetchConnStatusTopic"));
    }
}
