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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaDispatcher;
import org.eclipse.ecsp.analytics.stream.base.utils.MqttDispatcher;
import org.eclipse.ecsp.domain.SpeedV1_0;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;


/**
 * test class for {@link DispatchHandler}.
 */
public class DispatchHandlerTest {

    /** The handler. */
    @InjectMocks
    DispatchHandler handler = new DispatchHandler();

    /** The mqtt dispatcher. */
    @Mock
    MqttDispatcher mqttDispatcher;

    /** The kafka dispatcher. */
    @Mock
    KafkaDispatcher kafkaDispatcher;

    /** The test key. */
    private RetryTestKey testKey = new RetryTestKey();

    /**
     * Setup.
     */
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        testKey.setKey("Vehicle123");
    }

    /**
     * Test handle.
     */
    @Test
    public void testHandle() {
        Map<String, String> ecuTypeToTopicMap = new HashMap<>();
        ecuTypeToTopicMap.put("testecu", "testKafkaTopic");
        Map<String, Map<String, String>> brokerToEcuTypeMap = new HashMap<>();
        brokerToEcuTypeMap.put("kafka", ecuTypeToTopicMap);
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String payload = "payload";
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";

        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        //set ecuType for which dispatch of msg needs to be done on kafka topic
        event.setEcuType("testecu");
        DeviceMessage msg = new DeviceMessage(payload.getBytes(),
                Version.V1_0, event, "topic", Constants.THREAD_SLEEP_TIME_60000);

        handler.setup("taskId", brokerToEcuTypeMap);
        handler.handle(testKey, msg);

        Mockito.verify(kafkaDispatcher, Mockito.times(1)).dispatch(testKey, msg);
    }

    /**
     * Test handle if no broker to ecu type map configured.
     */
    @Test
    public void testHandleIfNoBrokerToEcuTypeMapConfigured() {
        IgniteEventImpl event = new IgniteEventImpl();
        SpeedV1_0 speed = new SpeedV1_0();
        speed.setValue(Constants.THREAD_SLEEP_TIME_100);
        event.setMessageId("Msg123");
        event.setBizTransactionId("Biz123");
        event.setEventData(speed);
        String payload = "payload";
        String vehicleId = "Vehicle12345";
        String deviceId = "Device12345";
        event.setVehicleId(vehicleId);
        event.setSourceDeviceId(deviceId);
        DeviceMessage msg = new DeviceMessage(payload.getBytes(),
                Version.V1_0, event, "topic", Constants.THREAD_SLEEP_TIME_60000);

        handler.handle(testKey, msg);

        Mockito.verify(kafkaDispatcher, Mockito.times(0)).dispatch(testKey, msg);
        Mockito.verify(mqttDispatcher, Mockito.times(1)).dispatch(testKey, msg);
    }
}
