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

import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3ClientConfig;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.platform.MqttTopicNameGenerator;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;



/**
 * {@link HiveMQMqttDispatcherTestHealthMontior}.
 */

public class HiveMQMqttDispatcherHealthMontiorTest {
    
    /** The mqtt health monitor. */
    @Spy
    private MqttHealthMonitor mqttHealthMonitor;
    
    /** The mqtt dispatcher one. */
    @Spy
    private HiveMqMqttDispatcher mqttDispatcherOne;
    
    /** The mqtt dispatcher two. */
    @Spy
    private HiveMqMqttDispatcher mqttDispatcherTwo;
    
    /** The mqtt client one. */
    @Mock
    private Mqtt3AsyncClient mqttClientOne;
    
    /** The mqtt client two. */
    @Mock
    private Mqtt3AsyncClient mqttClientTwo;
    
    /** The mqtt client map one. */
    @Mock
    Map<String, Mqtt3AsyncClient> mqttClientMapOne;

    /** The mqtt client map two. */
    @Mock
    Map<String, Mqtt3AsyncClient> mqttClientMapTwo;

    /** The mqtt 3 client config. */
    @Mock
    Mqtt3ClientConfig mqtt3ClientConfig;

    /** The forced check key. */
    private IgniteStringKey forcedCheckKey;
    
    /** The forced check value. */
    private DeviceMessage forcedCheckValue;
    
    /** The name generator. */
    @Mock
    private MqttTopicNameGenerator nameGenerator;

    /**
     * setUp().
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        ReflectionTestUtils.setField(mqttHealthMonitor, "mqttHealthMonitorEnabled", true);
        ReflectionTestUtils.setField(mqttDispatcherOne, "retryCount", 1);
        ReflectionTestUtils.setField(mqttDispatcherTwo, "retryCount", 1);
        ReflectionTestUtils.setField(mqttHealthMonitor, "dispatchers",
                Arrays.asList(mqttDispatcherOne, mqttDispatcherTwo));
        forcedCheckKey = new IgniteStringKey();
        forcedCheckKey.setKey(Constants.FORCED_HEALTH_CHECK_DEVICE_ID);
        ReflectionTestUtils.setField(mqttDispatcherOne, "forcedCheckKey", forcedCheckKey);
        ReflectionTestUtils.setField(mqttDispatcherTwo, "forcedCheckKey", forcedCheckKey);
        forcedCheckValue = new DeviceMessage();
        IgniteEventImpl event = new IgniteEventImpl();
        event.setPlatformId(PropertyNames.DEFAULT_PLATFORMID);
        forcedCheckValue.setEvent(event);
        forcedCheckValue.setMessage("forcedHealthCheckDummyMsg".getBytes());
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withTargetDeviceId(Constants.FORCED_HEALTH_CHECK_DEVICE_ID);
        forcedCheckValue.setDeviceMessageHeader(header);
        ReflectionTestUtils.setField(mqttDispatcherOne, "forcedCheckValue", forcedCheckValue);
        ReflectionTestUtils.setField(mqttDispatcherTwo, "forcedCheckValue", forcedCheckValue);
        Mockito.doReturn(MqttClientState.CONNECTED).when(mqttClientOne).getState();
        Mockito.doReturn(MqttClientState.CONNECTED).when(mqttClientTwo).getState();
        Mockito.doReturn(Optional.of(mqttClientTwo))
            .when(mqttDispatcherOne).getMqttClient(PropertyNames.DEFAULT_PLATFORMID);
        Mockito.doReturn(Optional.of(mqttClientOne))
            .when(mqttDispatcherTwo).getMqttClient(PropertyNames.DEFAULT_PLATFORMID);
        Mockito.doNothing().when(mqttDispatcherOne)
            .publishMessageToMqttTopic(any(), eq(false), eq(PropertyNames.DEFAULT_PLATFORMID));
        Mockito.doNothing().when(mqttDispatcherTwo)
            .publishMessageToMqttTopic(any(), eq(false), eq(PropertyNames.DEFAULT_PLATFORMID));
        when(mqttClientOne.getConfig()).thenReturn(mqtt3ClientConfig);
        when(mqttClientTwo.getConfig()).thenReturn(mqtt3ClientConfig);

        Mockito.when(mqttClientMapOne.get(PropertyNames.DEFAULT_PLATFORMID)).thenReturn(mqttClientOne);
        Mockito.when(mqttClientMapTwo.get(PropertyNames.DEFAULT_PLATFORMID)).thenReturn(mqttClientTwo);
        ReflectionTestUtils.setField(mqttDispatcherOne, "mqttClientMap", mqttClientMapOne);
        ReflectionTestUtils.setField(mqttDispatcherTwo, "mqttClientMap", mqttClientMapTwo);
    }

    /**
     * Test is healthy.
     */
    @Test
    public void testIsHealthy() {

        ReflectionTestUtils.setField(mqttDispatcherOne, "healthy", false);
        ReflectionTestUtils.setField(mqttDispatcherTwo, "healthy", true);

        ReflectionTestUtils.setField(mqttDispatcherTwo, "mqttTopicNameGenerator", nameGenerator);
        ReflectionTestUtils.setField(mqttDispatcherOne, "mqttTopicNameGenerator", nameGenerator);
        when(nameGenerator.getMqttTopicName(any(), any(), any())).thenReturn(Optional.of("topic"));
        Assert.assertEquals(false, mqttDispatcherOne.isHealthy(false));
        Assert.assertEquals(true, mqttDispatcherTwo.isHealthy(false));
        Assert.assertEquals(false, mqttHealthMonitor.isHealthy(false));

        Mockito.verify(mqttDispatcherOne,
                Mockito.times(0)).dispatch(forcedCheckKey, forcedCheckValue);
        Mockito.verify(mqttDispatcherTwo,
                Mockito.times(0)).dispatch(forcedCheckKey, forcedCheckValue);
        Assert.assertEquals(true, mqttHealthMonitor.isHealthy(true));

        Mockito.verify(mqttDispatcherOne,
                Mockito.times(1)).dispatch(forcedCheckKey, forcedCheckValue);
        Mockito.verify(mqttDispatcherTwo,
                Mockito.times(1)).dispatch(forcedCheckKey, forcedCheckValue);
        Assert.assertEquals(true, mqttDispatcherOne.isHealthy(false));
        Assert.assertEquals(true, mqttDispatcherTwo.isHealthy(false));

        ReflectionTestUtils.setField(mqttDispatcherTwo, "healthy", false);
        Assert.assertEquals(true, mqttDispatcherOne.isHealthy(false));
        Assert.assertEquals(false, mqttDispatcherTwo.isHealthy(false));
        Assert.assertEquals(false, mqttHealthMonitor.isHealthy(false));

        Assert.assertEquals(true, mqttHealthMonitor.isHealthy(true));
        Assert.assertEquals(true, mqttDispatcherOne.isHealthy(false));
        Assert.assertEquals(true, mqttDispatcherTwo.isHealthy(false));

    }

    /**
     * Test monitor name.
     */
    @Test
    public void testMonitorName() {
        Assert.assertEquals("MQTT_HEALTH_MONITOR", mqttHealthMonitor.monitorName());
    }

    /**
     * Test metric name.
     */
    @Test
    public void testMetricName() {
        String metricName = mqttHealthMonitor.metricName();
        Assert.assertEquals("MQTT_HEALTH_GUAGE", metricName);
    }

    /**
     * Test is enabled.
     */
    @Test
    public void testIsEnabled() {
        boolean isEnabled = mqttHealthMonitor.isEnabled();
        Assert.assertEquals(true, isEnabled);

    }

    /**
     * Test needs restart on failure.
     */
    @Test
    public void testNeedsRestartOnFailure() {
        ReflectionTestUtils.setField(mqttHealthMonitor, "mqttRestartOnFailure", true);
        Assert.assertEquals(true, mqttHealthMonitor.needsRestartOnFailure());

    }

    /**
     * Test initialize forced health check event.
     */
    @Test
    public void testInitializeForcedHealthCheckEvent() {
        mqttDispatcherOne.initializeForcedHealthCheckEvent();
        IgniteStringKey igniteStringKey = (IgniteStringKey) ReflectionTestUtils.getField(mqttDispatcherOne, 
                "forcedCheckKey");
        Assert.assertEquals(Constants.FORCED_HEALTH_CHECK_DEVICE_ID, igniteStringKey.getKey());
        DeviceMessage dmForcedCheckValue = (DeviceMessage) ReflectionTestUtils.getField(mqttDispatcherOne, 
                "forcedCheckValue");
        Assert.assertEquals(Constants.FORCED_HEALTH_CHECK_DEVICE_ID, 
            dmForcedCheckValue.getDeviceMessageHeader().getTargetDeviceId());
        Assert.assertEquals(Constants.FORCED_HEALTH_DEFAULT_TEST_TOPIC_NAME,
            dmForcedCheckValue.getDeviceMessageHeader().getDevMsgTopicSuffix());
        Assert.assertEquals(Constants.FORCED_HEALTH_DEFAULT_TEST_TOPIC_NAME, 
            dmForcedCheckValue.getEvent().getDevMsgTopicSuffix().get());
        Assert.assertEquals(Constants.FORCED_HEALTH_CHECK_DEVICE_ID, 
            dmForcedCheckValue.getEvent().getTargetDeviceId().get());
    }

}