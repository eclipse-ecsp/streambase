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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;


/**
 * class EmbeddedMQTTServerTest extends KafkaStreamsApplicationTestBase.
 */
public class EmbeddedMQTTServerTest extends KafkaStreamsApplicationTestBase {
    
    /** The Constant TOPIC. */
    private static final String TOPIC = "test";
    
    /** The Constant PAYLOAD. */
    private static final String PAYLOAD = "testPayload";

    /**
     * Subscribe to topic.
     *
     * @throws MqttException the mqtt exception
     */
    @Before
    public void subscribeToTopic() throws MqttException {
        subscibeToMqttTopic(TOPIC);
    }

    /**
     * Test single message.
     *
     * @throws MqttException the mqtt exception
     * @throws TimeoutException the timeout exception
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testSingleMessage() throws MqttException, TimeoutException, InterruptedException {
        publishMessageToMqttTopic(TOPIC, PAYLOAD.getBytes());
        List<byte[]> messages = getMessagesFromMqttTopic(TOPIC, 1, Constants.THREAD_SLEEP_TIME_10000);
        assertEquals("Expected payload is different", PAYLOAD, new String(messages.get(0)));
    }

    /**
     * Test multiple messages.
     *
     * @throws MqttException the mqtt exception
     * @throws TimeoutException the timeout exception
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testMultipleMessages() throws MqttException, TimeoutException, InterruptedException {
        publishMessageToMqttTopic(TOPIC, PAYLOAD.getBytes());
        publishMessageToMqttTopic(TOPIC, PAYLOAD.getBytes());
        List<byte[]> messages = getMessagesFromMqttTopic(TOPIC, Constants.TWO, Constants.THREAD_SLEEP_TIME_10000);
        assertEquals("Expected payload is different", PAYLOAD, new String(messages.get(0)));
        assertEquals("Expected payload is different", PAYLOAD, new String(messages.get(1)));
    }
}
