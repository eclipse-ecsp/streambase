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

import de.flapdoodle.embed.process.runtime.Network;
import io.moquette.broker.Server;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;


/**
 * class {@link EmbeddedMQTTServer} extends {@link ExternalResource}.
 */
public class EmbeddedMQTTServer extends ExternalResource {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(EmbeddedMQTTServer.class);
    
    /** The embed MQTT server. */
    private Server embedMQTTServer;
    
    /** The subscribe mqtt client. */
    private MqttClient subscribeMqttClient;
    
    /** The publish mqtt client. */
    private MqttClient publishMqttClient;
    
    /** The mqtt connection. */
    private MqttConnectOptions mqttConnection;

    // RTC-155383 - Running Kafka and Zookeeper on dynamic ports to resolve bind
    /** The mqtt port. */
    // address issue in streambase project
    private int mqttPort;

    /** The web socket port. */
    private int webSocketPort;

    /** The ssl port. */
    private int sslPort;

    /**
     * subscribeToTopic().
     */
    public EmbeddedMQTTServer() {
        // RTC-155383 - Running Kafka and Zookeeper on dynamic ports to resolve
        // bind
        // address issue in streambase project
        try {
            mqttPort = Network.getFreeServerPort();
        } catch (IOException e1) {
            throw new RuntimeException("Not able to get available mqtt  port to run embedded mqtt server");
        }

        try {
            webSocketPort = Network.getFreeServerPort();
        } catch (IOException e1) {
            throw new RuntimeException("Not able to get available webSocketPort port to run embedded mqtt server");
        }

        try {
            sslPort = Network.getFreeServerPort();
        } catch (IOException e1) {
            throw new RuntimeException("Not able to get available ssl port to run embedded mqtt server");
        }

        try {
            String mqttConnectionString = getConnectionString();
            MqttDispatcher.overriddenMqttBrokerUrl = mqttConnectionString;
            logger.info("Mqtt connection string:{}", mqttConnectionString);
            subscribeMqttClient = new MqttClient(mqttConnectionString, UUID.randomUUID().toString(), 
                    new MemoryPersistence());
            publishMqttClient = new MqttClient(mqttConnectionString, UUID.randomUUID().toString(), 
                    new MemoryPersistence());
        } catch (MqttException e) {
            throw new RuntimeException("Failed to create MQTT client", e);
        }
        mqttConnection = new MqttConnectOptions();
        mqttConnection.setCleanSession(true);
        mqttConnection.setPassword("simulator16".toCharArray());
        mqttConnection.setUserName("8146ccc47e84ac1e43de623403133d55");
    }

    /**
     * Before.
     *
     * @throws Throwable the throwable
     */
    @Override
    protected void before() throws Throwable {
        super.before();
        if (null != embedMQTTServer) {
            embedMQTTServer.stopServer();
        }
        embedMQTTServer = new Server();
        Properties configProps = new Properties();
        configProps.load(EmbeddedMQTTServer.class.getResourceAsStream("/moquette.conf"));

        // RTC-155383 - Running Kafka and Zookeeper on dynamic ports to
        // resolve bind
        // address issue in streambase project while running test cases
        // setting the "port" property in server to point to new dynamic port
        configProps.setProperty("port", String.valueOf(mqttPort));
        configProps.setProperty("websocket_port", String.valueOf(webSocketPort));
        configProps.setProperty("ssl_port", String.valueOf(sslPort));

        embedMQTTServer.startServer(configProps);
        logger.info("MQTT Server started");
    }

    /**
     * After.
     */
    @Override
    protected void after() {
        super.after();
        if (null != embedMQTTServer) {
            embedMQTTServer.stopServer();
        }
        if (null != subscribeMqttClient) {
            try {
                subscribeMqttClient.disconnect();
                subscribeMqttClient.close(true);
                publishMqttClient.disconnect();
                publishMqttClient.close(true);
            } catch (MqttException e) {
                logger.error("Failed to close MQTT Client");
            }
        }
        logger.info("MQTT Server stopped");
    }

    /**
     * subscribeToTopic().
     *
     * @param topic topic
     * @param mqttCallback mqttCallback
     * @throws MqttException MqttException
     */
    public void subscribeToTopic(String topic, MqttCallback mqttCallback) throws MqttException {
        if (!subscribeMqttClient.isConnected()) {
            subscribeMqttClient.connect(mqttConnection);
        }
        subscribeMqttClient.subscribe(topic);
        subscribeMqttClient.setCallback(mqttCallback);
        logger.info("Subscribed to topic {}", topic);
    }

    /**
     * publishToTopic().
     *
     * @param topic topic
     * @param payload payload
     * @throws MqttException MqttException
     */
    public void publishToTopic(String topic, byte[] payload) throws MqttException {
        if (!publishMqttClient.isConnected()) {
            publishMqttClient.connect(mqttConnection);
        }
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(payload);
        publishMqttClient.publish(topic, mqttMessage);
        logger.info("Published to topic {}", topic);
    }

    /**
     * Gets the connection string.
     *
     * @return the connection string
     */
    public String getConnectionString() {
        return "tcp://" + "127.0.0.1" + ":"
                + mqttPort;
    }
}
