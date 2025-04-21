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

import com.hivemq.client.internal.mqtt.lifecycle.MqttClientAutoReconnectImpl;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


/**
 * HiveMQTestContainer.
 */
public class HiveMQTestContainer {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(HiveMQTestContainer.class);

    /** The subscribe mqtt client. */
    private Mqtt3AsyncClient subscribeMqttClient;
    
    /** The publish mqtt client. */
    private Mqtt3AsyncClient publishMqttClient;

    /**
     * HiveMQTestContainer().
     */
    public HiveMQTestContainer() {
        try {
            logger.info("MQTT Server started");
            subscribeMqttClient = MqttClient.builder().identifier(UUID.randomUUID().toString())
                    .serverHost("localhost").serverPort(Constants.INT_1883)
                    .useMqttVersion3().automaticReconnect(MqttClientAutoReconnectImpl.DEFAULT)
                    .addConnectedListener(context -> logger.info("connected subscriber"))
                    .addDisconnectedListener(context -> logger.info("disconnected subscriber"))
                    .transportConfig().mqttConnectTimeout(Constants.FIVE, TimeUnit.MINUTES)
                    .socketConnectTimeout(Constants.FIVE, TimeUnit.MINUTES).applyTransportConfig()
                    .buildAsync();
            publishMqttClient = MqttClient.builder().identifier(UUID.randomUUID().toString())
                    .serverHost("localhost").serverPort(Constants.INT_1883).useMqttVersion3()
                    .addConnectedListener(context -> logger.info("connected publisher"))
                    .addDisconnectedListener(context -> logger.info("disconnected publisher"))
                    .transportConfig().mqttConnectTimeout(Constants.FIVE, TimeUnit.MINUTES)
                    .socketConnectTimeout(Constants.FIVE, TimeUnit.MINUTES).applyTransportConfig()
                    .automaticReconnect(MqttClientAutoReconnectImpl.DEFAULT)
                    .buildAsync();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create MQTT client", ex);
        }
    }

    /**
     * After.
     */
    protected void after() {
        if (null != subscribeMqttClient) {
            try {
                subscribeMqttClient.disconnect();
                publishMqttClient.disconnect();
                subscribeMqttClient = null;
                publishMqttClient = null;
            } catch (Exception e) {
                logger.error("Failed to close MQTT Client", e);
            }
        }
        logger.info("MQTT Server stopped");
    }

    /**
     * subscribeToTopic().
     *
     * @param topic topic
     * @param callback callback
     * @throws InterruptedException InterruptedException
     */
    public void subscribeToTopic(String topic, Consumer<Mqtt3Publish> callback) throws InterruptedException {
        if (!subscribeMqttClient.getState().isConnected()) {
            subscribeMqttClient.connectWith().keepAlive(Constants.THREAD_SLEEP_TIME_60000).cleanSession(false).send()
                    .whenComplete((connAck, throwable) -> {
                        if (throwable != null) {
                            logger.info("Failure in subscribe connection", throwable);
                        }
                    });
        }
        Thread.sleep(Constants.THREAD_SLEEP_TIME_1000);
        subscribeMqttClient.subscribeWith()
                .topicFilter(topic)
                .callback(callback)
                .send().whenComplete((connAck, throwable) -> {
                    if (throwable != null) {
                        logger.info("Failure in subscribing client", throwable);
                    } else {
                        logger.info("Subscribed to topic {}", topic);
                    }
                });
    }

    /**
     * publishToTopic().
     *
     * @param topic topic
     * @param payload payload
     * @throws InterruptedException InterruptedException
     */
    public void publishToTopic(String topic, byte[] payload) throws InterruptedException {
        if (!publishMqttClient.getState().isConnected()) {
            publishMqttClient.connect()
                    .whenComplete((connAck, throwable) -> {
                        if (throwable != null) {
                            logger.info("Failure in publishing the connection", throwable);
                        } 
                    });
        }
        
        if (!publishMqttClient.getState().isConnected()) {
            TimeUnit.SECONDS.sleep(Constants.FIVE);
        }
         
        publishMqttClient.publishWith()
                         .topic(topic).payload(payload).send().whenComplete((publish, throwable) -> {
                             if (throwable != null) {
                                 logger.info("Failure in publishing the message", throwable);
                             } else {
                                 logger.info("Published to topic {}", topic);
                             }
                         });
    
        if (!publishMqttClient.getState().isConnected()) {
            TimeUnit.SECONDS.sleep(Constants.FIVE);
        }
        publishMqttClient.publishWith()
                .topic(topic)
                .payload(payload)
                .send()
                .whenComplete((publish, throwable) -> {
                    if (throwable != null) {
                        logger.info("Failure in publishing the message", throwable);
                    } else {
                        logger.info("Published to topic {}", topic);
                    }
                });
    }
}
