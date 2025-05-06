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

package org.eclipse.ecsp.stream.dma.handler;

import org.eclipse.ecsp.stream.dma.dao.DeviceStatusDaoCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;



/**
 * Test class for {@link DeviceStatusBackDoorKafkaConsumer}.
 */
public class DeviceStatusBackDoorKafkaConsumerUnitTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The backdoor consumer. */
    @InjectMocks
    private DeviceStatusBackDoorKafkaConsumer backdoorConsumer;

    /** The connection status dao. */
    @Mock
    private DeviceStatusDaoCacheBackedInMemoryImpl connectionStatusDao;

    /**
     * Sets the up.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test monitor name.
     */
    @Test
    public void testMonitorName() {
        Assert.assertEquals("DEVICE_STATUS_BACKDOOR_HEALTH_MONITOR", backdoorConsumer.monitorName());
    }

    /**
     * Test metric name.
     */
    @Test
    public void testMetricName() {
        Assert.assertEquals("DEVICE_STATUS_BACKDOOR_HEALTH_GUAGE", backdoorConsumer.metricName());
    }

    /**
     * Test needs restart on failure.
     */
    @Test
    public void testNeedsRestartOnFailure() {
        backdoorConsumer.setNeedsRestartOnFailure(true);
        Assert.assertTrue(backdoorConsumer.needsRestartOnFailure());
        backdoorConsumer.setNeedsRestartOnFailure(false);
        Assert.assertFalse(backdoorConsumer.needsRestartOnFailure());
    }

    /**
     * Test is enabled.
     */
    @Test
    public void testIsEnabled() {
        backdoorConsumer.setHealthMonitorEnabled(true);
        backdoorConsumer.setIsDmaEnabled(true);
        Assert.assertTrue(backdoorConsumer.isEnabled());

        backdoorConsumer.setIsDmaEnabled(false);
        Assert.assertFalse(backdoorConsumer.isEnabled());

        backdoorConsumer.setIsDmaEnabled(true);
        backdoorConsumer.setHealthMonitorEnabled(false);
        Assert.assertFalse(backdoorConsumer.isEnabled());
    }

}
