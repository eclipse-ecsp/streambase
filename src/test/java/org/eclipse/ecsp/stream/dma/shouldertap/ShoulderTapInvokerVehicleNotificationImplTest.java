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

import org.eclipse.ecsp.stream.dma.shouldertap.ShoulderTapInvokerVehicleNotificationImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * The Class ShoulderTapInvokerVehicleNotificationImplTest.
 */
class ShoulderTapInvokerVehicleNotificationImplTest {

    /** The shoulder tap invoker vehicle notification impl. */
    private ShoulderTapInvokerVehicleNotificationImpl shoulderTapInvokerVehicleNotificationImpl;

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @BeforeEach
    void setUp() throws Exception {
        shoulderTapInvokerVehicleNotificationImpl = new ShoulderTapInvokerVehicleNotificationImpl();
    }

    /**
     * ShoulderTapInvokerVehicleNotificationImpl.sendWakeUpMessage() always should return false.
     */
    @Test
    void testSendWakeUpMessage() {
        assertNotNull(shoulderTapInvokerVehicleNotificationImpl);
        assertFalse(shoulderTapInvokerVehicleNotificationImpl.sendWakeUpMessage("requestId", "vehicleId", 
                new HashMap<>(), null));
        assertNotEquals(true, shoulderTapInvokerVehicleNotificationImpl.sendWakeUpMessage("", "", null, null));

    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @AfterEach
    void tearDown() throws Exception {
        shoulderTapInvokerVehicleNotificationImpl = null;
    }

}
