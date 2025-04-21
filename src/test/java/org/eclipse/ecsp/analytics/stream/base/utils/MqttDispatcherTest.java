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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.platform.MqttTopicNameGenerator;
import org.eclipse.ecsp.domain.BlobDataV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteBlobEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.serializer.IngestionSerializer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;



/**
 * class {@link MqttDispatcherTest}: UT class for {@link MqttDispatcher}.
 */
public class MqttDispatcherTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    
    /** The paho mqtt dispatcher. */
    @InjectMocks
    PahoMqttDispatcher pahoMqttDispatcher;
    
    /** The mqtt dispatcher. */
    @InjectMocks
    private MqttDispatcher mqttDispatcher = new PahoMqttDispatcher();
    
    /** The client. */
    @Mock
    private MqttClient client;
    
    /** The transformer. */
    @Mock
    private IngestionSerializer transformer;

    /** The mqtt topic name generator. */
    @Mock
    private MqttTopicNameGenerator mqttTopicNameGenerator;

    /** The mqtt client map. */
    Map<String, MqttClient> mqttClientMap;

    /** The mqtt platform config map. */
    Map<String, MqttConfig> mqttPlatformConfigMap;
    
    /**
     * Setup for this test case.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mqttClientMap = new HashMap<>();
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        ReflectionTestUtils.setField(mqttDispatcher, "mqttClientMap", mqttClientMap);

        mqttPlatformConfigMap = new HashMap<>();
        MqttConfig config = new MqttConfig();
        config.setMqttQosValue(0);
        mqttPlatformConfigMap.put(PropertyNames.DEFAULT_PLATFORMID, config);
        ReflectionTestUtils.setField(mqttDispatcher, "mqttPlatformConfigMap", mqttPlatformConfigMap);

        Mockito.when(mqttTopicNameGenerator.getMqttTopicName(any(), any(), any())).thenReturn(Optional.of("topic"));
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
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.when(transformer.serialize(Mockito.any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        pahoMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(1);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(1)).serialize(Mockito.any());

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
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.when(transformer.serialize(Mockito.any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        pahoMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(1);
        mqttDispatcher.setWrapDispatchEvent(false);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(0)).serialize(Mockito.any());

    }

    /**
     * Test wrapevent when event fequency is not met.
     */
    /*
     * Here we invoke dispatch just once and frequency of sampling is Constants.TWO hence
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
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.when(transformer.serialize(Mockito.any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        pahoMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(Constants.TWO);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(0)).serialize(Mockito.any());

    }

    /**
     * Test wrapevent when event fequency is met.
     */
    /*
     * Here we will invoke dispatch 4 times . The frequency of sampling is set
     * to Constants.TWO hence two events should be wrapped.
     */
    @Test
    public void testWrapeventWhenEventFequencyIsMet() {
        String input = "input";
        String transformed = "transformed";
        DeviceMessage entity = new DeviceMessage(input.getBytes(), Version.V1_0,
                new TestEvent(), "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("globaltopic");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.when(transformer.serialize(Mockito.any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        pahoMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(Constants.TWO);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.dispatch(new TestKey(), entity);
        mqttDispatcher.dispatch(new TestKey(), entity);
        mqttDispatcher.dispatch(new TestKey(), entity);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(Constants.TWO)).serialize(Mockito.any());

    }

    /**
     * Test close mqtt connection when exception occur in disconnect.
     *
     * @throws MqttException the mqtt exception
     */
    @Test
    public void testCloseMqttConnectionWhenExceptionOccurInDisconnect() throws MqttException {
        Mockito.when(client.isConnected()).thenReturn(true);
        doThrow(MqttException.class).when(client).disconnect();
        mqttDispatcher.close();
        Mockito.verify(client, Mockito.times(1)).disconnectForcibly();
    }

    /**
     * Test close mqtt connection when exception occur in disconnect forcibly.
     *
     * @throws MqttException the mqtt exception
     */
    @Test
    public void testCloseMqttConnectionWhenExceptionOccurInDisconnectForcibly() throws MqttException {
        Mockito.when(client.isConnected()).thenReturn(true);
        doThrow(MqttException.class).when(client).disconnect();
        doThrow(MqttException.class).when(client).disconnectForcibly();
        mqttDispatcher.close();
        Mockito.verify(client, Mockito.times(1)).disconnectForcibly();
        Mockito.verify(client, Mockito.times(0)).close();
    }

    /**
     * Test close mqtt connection when client close successfully.
     *
     * @throws MqttException the mqtt exception
     */
    @Test
    public void testCloseMqttConnectionWhenClientCloseSuccessfully() throws MqttException {
        Mockito.when(client.isConnected()).thenReturn(true);
        mqttDispatcher.close();
        Mockito.verify(client, Mockito.times(1)).close();
        Mockito.verify(client, Mockito.times(1)).disconnect();
        Mockito.verify(client, Mockito.times(0)).disconnectForcibly();
    }

    /**
     * Test close mqtt connection when exception occur in closing.
     *
     * @throws MqttException the mqtt exception
     */
    @Test
    public void testCloseMqttConnectionWhenExceptionOccurInClosing() throws MqttException {
        Mockito.when(client.isConnected()).thenReturn(true);
        doThrow(MqttException.class).when(client).close();
        mqttDispatcher.close();
        Mockito.verify(client, Mockito.times(Constants.TWO)).close();
        Mockito.verify(client, Mockito.times(1)).disconnectForcibly();
    }

    /**
     * Test wrapevent for global topic listed.
     */
    @Test
    public void testWrapeventForGlobalTopicListed() {
        String input = "input";
        List<String> topicList = new ArrayList<>();
        topicList.add("globaltopic");
        topicList.add("topic");
        DeviceMessage entity = new DeviceMessage(input.getBytes(), Version.V1_0,
                new TestEvent(), "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("globaltopic");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");
        String transformed = "transformed";
        Mockito.when(client.isConnected()).thenReturn(true);
        Mockito.when(transformer.serialize(Mockito.any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        pahoMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(1);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.setGlobalBroadcastRetentionTopicList(topicList);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(1)).serialize(Mockito.any());

        ArgumentCaptor<IgniteBlobEvent> keyArgument = ArgumentCaptor.forClass(IgniteBlobEvent.class);
        Mockito.verify(transformer).serialize(keyArgument.capture());
        IgniteBlobEvent actualKey = keyArgument.getValue();
        BlobDataV1_0 blobDataV10 = (BlobDataV1_0) actualKey.getEventData();
        Assert.assertEquals(new String(blobDataV10.getPayload()), new String(input.getBytes()));

    }

    /**
     * Test wrapevent for global topic not listed.
     */
    @Test
    public void testWrapeventForGlobalTopicNotListed() {
        String input = "input";
        List<String> topicList = new ArrayList<>();
        topicList.add("globaltopic");
        topicList.add("topic");
        DeviceMessage entity = new DeviceMessage(input.getBytes(), Version.V1_0,
                new TestEvent(), "topic", Constants.THREAD_SLEEP_TIME_60000);
        entity.getDeviceMessageHeader().withDevMsgGlobalTopic("test");
        entity.getDeviceMessageHeader().withTargetDeviceId("td");
        entity.getDeviceMessageHeader().withVehicleId("vd");
        entity.getDeviceMessageHeader().withRequestId("rd");
        Mockito.when(client.isConnected()).thenReturn(true);
        String transformed = "transformed";
        Mockito.when(transformer.serialize(Mockito.any())).thenReturn(transformed.getBytes());
        mqttClientMap.put(PropertyNames.DEFAULT_PLATFORMID, client);
        pahoMqttDispatcher.setMqttClientMap(mqttClientMap);
        mqttDispatcher.setEventWrapFrequency(1);
        mqttDispatcher.setWrapDispatchEvent(true);
        mqttDispatcher.setTransformer(transformer);
        mqttDispatcher.setGlobalBroadcastRetentionTopicList(topicList);
        mqttDispatcher.dispatch(new TestKey(), entity);

        Mockito.verify(transformer, Mockito.times(1)).serialize(Mockito.any());

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
     * innerc class TestEvent extends IgniteEventImpl.
     */
    public class TestEvent extends IgniteEventImpl {

        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /**
         * Instantiates a new test event.
         */
        public TestEvent() {

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
