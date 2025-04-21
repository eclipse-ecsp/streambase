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

package org.eclipse.ecsp.analytics.stream.base.mqtt;

import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;


/**
 * class HiveMQEmbeddedMQTTServerTest extends HiveMQTestContainer.
 */
public class HiveMQEmbeddedMQTTServerTest extends HiveMQTestContainer {
    
    /** The Constant MQTT_SERVER. */
    @ClassRule
    public static final MqttServer MQTT_SERVER = new MqttServer();
    
    /** The Constant TOPIC. */
    private static final String TOPIC = "test";
    
    /** The Constant PAYLOAD. */
    private static final String PAYLOAD = "testPayload";
    
    /** The mqtt messages. */
    protected Map<String, List<byte[]>> mqttMessages = new HashMap<>();

    /**
     * Subscribe to topic.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Before
    public void subscribeToTopic() throws InterruptedException {
        this.subscribeHiveMQClientToMqttTopic(TOPIC);
    }

    /**
     * Test single message.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testSingleMessage() throws InterruptedException {
        super.publishToTopic(TOPIC, PAYLOAD.getBytes());
        List<byte[]> messages = getMessagesFromMqttTopic(TOPIC, 1, Constants.THREAD_SLEEP_TIME_10000);
        assertEquals("Expected payload is different", PAYLOAD, new String(messages.get(0)));
    }

    /**
     * Test multiple messages.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testMultipleMessages() throws InterruptedException {
        super.publishToTopic(TOPIC, PAYLOAD.getBytes());
        super.publishToTopic(TOPIC, PAYLOAD.getBytes());
        List<byte[]> messages = getMessagesFromMqttTopic(TOPIC, Constants.TWO, Constants.THREAD_SLEEP_TIME_10000);
        assertEquals("Expected payload is different", PAYLOAD, new String(messages.get(0)));
        assertEquals("Expected payload is different", PAYLOAD, new String(messages.get(1)));
        super.after();
    }

    /**
     * Subscribe hive MQ client to mqtt topic.
     *
     * @param topic the topic
     * @throws InterruptedException the interrupted exception
     */
    private void subscribeHiveMQClientToMqttTopic(String topic) throws InterruptedException {
        Consumer<Mqtt3Publish> publishConsumer = (publish) -> {
            if (mqttMessages.containsKey(topic)) {
                mqttMessages.get(topic).add(publish.getPayloadAsBytes());
            } else {
                List<byte[]> messages = new ArrayList<>();
                messages.add(publish.getPayloadAsBytes());
                mqttMessages.put(topic, messages);
            }
        };
        super.subscribeToTopic(TOPIC, publishConsumer);
    }

    /**
     * Gets the messages from mqtt topic.
     *
     * @param topic the topic
     * @param n the n
     * @param waitTime the wait time
     * @return the messages from mqtt topic
     * @throws InterruptedException the interrupted exception
     */
    protected List<byte[]> getMessagesFromMqttTopic(String topic, int n, int waitTime)
            throws InterruptedException {
        int timeWaited = 0;
        int increment = Constants.THREAD_SLEEP_TIME_2000;
        List<byte[]> messages = new ArrayList<>();
        while ((messages.size() < n) && (timeWaited <= waitTime)) {
            List<byte[]> payloads = mqttMessages.get(topic);
            if (null != payloads) {
                messages.addAll(payloads);
            }
            runAsync(() -> {
            }, delayedExecutor(increment, MILLISECONDS)).join();
            timeWaited = timeWaited + increment;
        }
        return messages;
    }
}
