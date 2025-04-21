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

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 * Test class for {@link DummyShoulderTapInvokerImpl}.
 */
public class DummyShoulderTapInvokerImplTest {

    /** The spc. */
    @Mock
    private StreamProcessingContext spc;

    /** The dummy shoulder tap invoker impl. */
    @InjectMocks
    private DummyShoulderTapInvokerImpl dummyShoulderTapInvokerImpl;

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test send wake up message.
     */
    @Test
    public void testSendWakeUpMessage() {
        String requestId = "Request123";
        String vehicleId = "Vehicle123";
        Map<String, Object> extraParameters = new HashMap<>();
        boolean wakeUpStatus = dummyShoulderTapInvokerImpl.sendWakeUpMessage(requestId, vehicleId, 
            extraParameters, spc);
        assertEquals(false, wakeUpStatus);
    }
}
