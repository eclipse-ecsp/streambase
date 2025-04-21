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

package org.eclipse.ecsp.analytics.stream.base;

import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.ecsp.analytics.stream.base.kafka.support.KafkaStreamsThreadStatusPrinter;
import org.eclipse.ecsp.healthcheck.HealthMonitor;
import org.eclipse.ecsp.healthcheck.HealthService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * class {@link KafkaStreamsLauncherMockTest}.
 */
public class KafkaStreamsLauncherMockTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The launcher. */
    @InjectMocks
    private KafkaStreamsLauncher launcher;

    /** The health service. */
    @Mock
    private HealthService healthService;

    /** The kafka state listener health monitor. */
    @Mock
    private KafkaStateListener kafkaStateListenerHealthMonitor;

    /** The streams. */
    @Mock
    private KafkaStreams streams;

    /** The thread status printer. */
    @Mock
    private KafkaStreamsThreadStatusPrinter threadStatusPrinter;

    /** The props. */
    @Mock
    private Properties props;

    /**
     * setUp().
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        List<String> ignoredMonitorNames = new ArrayList<String>();
        ignoredMonitorNames.add("KAFKA_CONSUMER_GROUP_HEALTH_MONITOR");
        ignoredMonitorNames.add("DEVICE_STATUS_BACKDOOR_HEALTH_MONITOR");
        launcher.setBootstrapIgnoredMonitors(ignoredMonitorNames);
        launcher.setHealthService(healthService);
    }

    /**
     * Test health check.
     */
    @Test
    public void testHealthCheck() {
        List<HealthMonitor> failedMonitors = new ArrayList<HealthMonitor>();
        failedMonitors.add(kafkaStateListenerHealthMonitor);
        Mockito.when(healthService.triggerInitialCheck()).thenReturn(failedMonitors);
        Mockito.when(kafkaStateListenerHealthMonitor.monitorName())
                .thenReturn("KAFKA_CONSUMER_GROUP_HEALTH_MONITOR");
        Assert.assertFalse(launcher.bootstrapHealthCheck());

        Mockito.when(kafkaStateListenerHealthMonitor.monitorName())
                .thenReturn("NOT_KAFKA_CONSUMER_GROUP_HEALTH_MONITOR");
        Assert.assertTrue(launcher.bootstrapHealthCheck());
    }

    /**
     * Test terminate.
     */
    @Test
    public void testTerminate() {
        ReflectionTestUtils.setField(launcher, "streams", streams);
        launcher.terminate();
        Mockito.verify(threadStatusPrinter, Mockito.times(1)).close();
        Mockito.verify(streams, Mockito.times(1)).close();
    }

    /**
     * Test check merge defaults if application ID null.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCheckMergeDefaultsIfApplicationIDNull() {
        Mockito.when(props.getProperty(Mockito.anyString())).thenReturn(null);
        ReflectionTestUtils.invokeMethod(launcher, "checkMergeDefaults", props);
    }

    /*
     * Commented below test case scenario as it calls System.exit() internally
     * which leads jvm to exit and also affects jacoco maven plugin execution.
     *
     * @Test public void testBootstrapHealthCheckException() {
     *
     *       Mockito.when(launcher.bootstrapHealthCheck()).thenThrow(new
     *       IllegalStateException()); launcher.doLaunch(new Properties()); }
     *
     */
}
