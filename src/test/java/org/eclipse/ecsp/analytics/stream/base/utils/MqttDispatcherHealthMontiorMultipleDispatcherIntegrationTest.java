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

package org.eclipse.ecsp.analytics.stream.base.utils;

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;



/**
 * class MqttDispatcherHealthMontiorMultipleDispatcherIntegrationTest extends KafkaStreamsApplicationTestBase.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/mqtt-health-monitor.properties")
public class MqttDispatcherHealthMontiorMultipleDispatcherIntegrationTest extends KafkaStreamsApplicationTestBase {

    /** The monitor. */
    @Autowired
    MqttHealthMonitor monitor;

    /** The Mqtt dispatcher one. */
    @Autowired
    MqttDispatcher MqttDispatcherOne;

    /** The Mqtt dispatcher two. */
    @Autowired
    MqttDispatcher MqttDispatcherTwo;

    /**
     * Setup.
     *
     * @throws Exception the exception
     */
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    /**
     * Test mqtt health monitor integration.
     */
    @Test
    public void testMqttHealthMonitorIntegration() {

        List<MqttDispatcher> dispatchers = monitor.getDispatchers();
        Assert.assertEquals(Constants.THREE, monitor.getDispatchers().size());
        MqttDispatcher mqttDispatcherOne = dispatchers.get(0);
        MqttDispatcher mqttDispatcherTwo = dispatchers.get(1);
        if (mqttDispatcherOne.isHealthy(false) && mqttDispatcherTwo.isHealthy(false)) {
            Assert.assertEquals(true, monitor.isHealthy(false));
        } else {
            Assert.assertEquals(false, monitor.isHealthy(false));
        }
        ReflectionTestUtils.setField(MqttDispatcherOne, "healthy", false);
        Assert.assertEquals(false, monitor.isHealthy(false));
        Assert.assertEquals(true, monitor.isHealthy(true));
        Assert.assertEquals(true, ReflectionTestUtils.getField(MqttDispatcherOne, "healthy"));
    }
}
