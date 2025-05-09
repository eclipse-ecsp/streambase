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
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3PublishBuilder;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.platform.MqttTopicNameGenerator;
import org.eclipse.ecsp.domain.BlobDataV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteBlobEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.serializer.IngestionSerializer;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageUtils;
import org.eclipse.ecsp.utils.metrics.IgniteErrorCounter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;



/**
 * test class HiveMQMqttDispatcherTest.
 */
public class HiveMQMqttDispatcherTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The mqtt dispatcher. */
    @InjectMocks
    private MqttDispatcher mqttDispatcher = new HiveMqMqttDispatcher();

    /** The client. */
    @Mock
    private Mqtt3AsyncClient client;

    /** The default mqtt topic name generator impl. */
    @Mock
    private DefaultMqttTopicNameGeneratorImpl defaultMqttTopicNameGeneratorImpl;

    /** The transformer. */
    @Mock
    private IngestionSerializer transformer;

    /** The hive mq mqtt dispatcher. */
    @InjectMocks
    HiveMqMqttDispatcher hiveMqMqttDispatcher;
    
    /** The mqtt 3 client config. */
    @Mock
    Mqtt3ClientConfig mqtt3ClientConfig;
    
    /** The device message utils. */
    @Mock
    DeviceMessageUtils deviceMessageUtils;
    
    /** The error counter. */
    @Mock
    IgniteErrorCounter errorCounter;

    /** The mqtt 3 publish. */
    @Mock
    Mqtt3PublishBuilder.Send<CompletableFuture<Mqtt3Publish>> mqtt3Publish;

    /** The mqtt client map. */
    Map<String, Mqtt3AsyncClient> mqttClientMap;

    /** The mqtt platform config map. */
    Map<String, MqttConfig> mqttPlatformConfigMap;

    /** The mqtt topic name generator. */
    @Mock
    MqttTopicNameGenerator mqttTopicNameGenerator;

    /**
     * Setup for this test case.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mqttClientMap = new HashMap<>();
        mqttClientMap.put("default", client);
        ReflectionTestUtils.setField(mqttDispatcher, "mqttClientMap", mqttClientMap);

        mqttPlatformConfigMap = new HashMap<>();
        MqttConfig config = new MqttConfig();
        config.setMqttQosValue(0);
        mqttPlatformConfigMap.put(PropertyNames.DEFAULT_PLATFORMID, config);
        ReflectionTestUtils.setField(mqttDispatcher, "mqttPlatformConfigMap", mqttPlatformConfigMap);
        ReflectionTestUtils.setField(mqttDispatcher, "mqttTopicNameGenerator", mqttTopicNameGenerator);
        when(mqttTopicNameGenerator.getMqttTopicName(any(), any(), any())).thenReturn(Optional.of("topic"));
    }

    /**
     * Test wrapevent frequency validation.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWrapeventFrequencyValidation() {
        mqttDispatcher.setEventWrapFrequency(0);
        mqttDispatcher.validateEventWrapFrequency();
    }

    /**
     * Test wrapevent.
     */
    /*
     * Happy flow where wrap event is set to true and frequency is 1 so all
     * events will be wrapped.
     */
    @Test
    public void testWrapevent() {
        String input = "input";
        String transformed = "transformed";
        DeviceMessage entity = new DeviceMessage(input.getBytes(), Version.V1_0,
                new TestEvent(), "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("globaltopic");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");
        when(transformer.serialize(any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        hiveMqMqttDispatcher.setMqttClientMap(mqttClientMap);
        when(client.getState()).thenReturn(MqttClientState.valueOf("CONNECTED"));
        when(client.getConfig()).thenReturn(mqtt3ClientConfig);
        mqttDispatcher.setEventWrapFrequency(1);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(1)).serialize(any());

        ArgumentCaptor<IgniteBlobEvent> keyArgument = ArgumentCaptor.forClass(IgniteBlobEvent.class);
        Mockito.verify(transformer).serialize(keyArgument.capture());
        IgniteBlobEvent actualKey = keyArgument.getValue();
        BlobDataV1_0 blobDataV10 = (BlobDataV1_0) actualKey.getEventData();
        Assert.assertEquals(new String(blobDataV10.getPayload()), new String(input.getBytes()));

    }

    /**
     * Test wrapevent false.
     */
    /*
     * Here the flaf wrap event is set to false hence event should not be
     * wrapped
     */
    @Test
    public void testWrapeventFalse() {
        String input = "input";
        String transformed = "transformed";
        DeviceMessage entity = new DeviceMessage(input.getBytes(), Version.V1_0,
                new TestEvent(), "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("globaltopic");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");
        when(client.getState()).thenReturn(MqttClientState.valueOf("CONNECTED"));
        when(client.getConfig()).thenReturn(mqtt3ClientConfig);
        when(transformer.serialize(any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        hiveMqMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(1);
        mqttDispatcher.setWrapDispatchEvent(false);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(0)).serialize(any());

    }

    /**
     * Test wrapevent when event fequency is not met.
     */
    /*
     * Here we invoke dispatch just once and frequency of sampling is 2 hence
     * wrapping should not be done.
     */
    @Test
    public void testWrapeventWhenEventFequencyIsNotMet() {
        String input = "input";
        String transformed = "transformed";
        DeviceMessage entity = new DeviceMessage(input.getBytes(), Version.V1_0,
                new TestEvent(), "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("globaltopic");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");
        when(client.getState()).thenReturn(MqttClientState.valueOf("CONNECTED"));
        when(client.getConfig()).thenReturn(mqtt3ClientConfig);
        when(transformer.serialize(any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        hiveMqMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(Constants.TWO);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(0)).serialize(any());

    }

    /**
     * Test client connection when exception occurs.
     */
    @Test
    public void testClientConnection_when_exception_occurs() {
        defaultMqttTopicNameGeneratorImpl.setTopicNamePrefix("");
        DeviceMessage entity = new DeviceMessage("input".getBytes(), Version.V1_0, new TestEvent(), 
                "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("globaltopic");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");
        when(client.getState()).thenReturn(MqttClientState.valueOf("CONNECTED"));
        when(transformer.serialize(any())).thenReturn("transformed".getBytes());
        when(client.getConfig()).thenReturn(mqtt3ClientConfig);
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        hiveMqMqttDispatcher.setMqttClientMap(mqttClientMap);
        when(client.publish(any())).thenThrow(RuntimeException.class);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Assert.assertFalse(mqttDispatcher.healthy);
        verify(client, times(1)).disconnect();
    }

    /**
     * Test wrapevent for global topic listed.
     */
    @Test
    public void testWrapeventForGlobalTopicListed() {
        String input = "input";
        String transformed = "transformed";
        DeviceMessage entity = new DeviceMessage(input.getBytes(), Version.V1_0,
                new TestEvent(), "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("globaltopic");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");

        when(transformer.serialize(any())).thenReturn(transformed.getBytes());
        List<String> topicList = new ArrayList<>();
        topicList.add("test");
        topicList.add("topic");
        mqttDispatcher.setGlobalBroadcastRetentionTopicList(topicList);
        when(client.getState()).thenReturn(MqttClientState.valueOf("CONNECTED"));
        when(client.getConfig()).thenReturn(mqtt3ClientConfig);
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        hiveMqMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(1);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(1)).serialize(any());

        ArgumentCaptor<IgniteBlobEvent> keyArgument = ArgumentCaptor.forClass(IgniteBlobEvent.class);
        Mockito.verify(transformer).serialize(keyArgument.capture());
        IgniteBlobEvent actualKey = keyArgument.getValue();
        BlobDataV1_0 blobDataV10 = (BlobDataV1_0) actualKey.getEventData();
        Assert.assertEquals(new String(blobDataV10.getPayload()), new String(input.getBytes()));

    }

    /**
     * Test wrapevent for non global topic listed.
     */
    @Test
    public void testWrapeventForNonGlobalTopicListed() {
        String input = "input";
        String transformed = "transformed";
        DeviceMessage entity = new DeviceMessage(input.getBytes(), Version.V1_0,
                new TestEvent(), "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("globaltopic");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");

        when(transformer.serialize(any())).thenReturn(transformed.getBytes());
        List<String> topicList = new ArrayList<>();
        topicList.add("globaltopic");
        topicList.add("topic");
        mqttDispatcher.setGlobalBroadcastRetentionTopicList(topicList);
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        hiveMqMqttDispatcher.setMqttClientMap(mqttClientMap);
        when(client.getState()).thenReturn(MqttClientState.valueOf("CONNECTED"));
        when(client.getConfig()).thenReturn(mqtt3ClientConfig);
        mqttDispatcher.setEventWrapFrequency(1);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(1)).serialize(any());

        ArgumentCaptor<IgniteBlobEvent> keyArgument = ArgumentCaptor.forClass(IgniteBlobEvent.class);
        Mockito.verify(transformer).serialize(keyArgument.capture());
        IgniteBlobEvent actualKey = keyArgument.getValue();
        BlobDataV1_0 blobDataV10 = (BlobDataV1_0) actualKey.getEventData();
        Assert.assertEquals(new String(blobDataV10.getPayload()), new String(input.getBytes()));

    }

    /**
     * inner class TestKey implements IgniteKey.
     */
    
    public class TestKey implements IgniteKey<String> {

        /**
         * Gets the key.
         *
         * @return the key
         */
        @Override
        public String getKey() {

            return "test";
        }

    }

    /**
     * inner class TestEvent extends IgniteEventImpl.
     */
    public class TestEvent extends IgniteEventImpl {

        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /**
         * Instantiates a new test event.
         */
        public TestEvent() {
            // Nothing to do.
        }

        /**
         * Gets the event id.
         *
         * @return the event id
         */
        @Override
        public String getEventId() {
            return "Sample";
        }

        /**
         * Gets the target device id.
         *
         * @return the target device id
         */
        @Override
        public Optional<String> getTargetDeviceId() {
            return Optional.of("test");
        }

    }
}
