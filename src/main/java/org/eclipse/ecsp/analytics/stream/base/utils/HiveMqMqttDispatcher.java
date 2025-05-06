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

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientSslConfig;
import com.hivemq.client.mqtt.datatypes.MqttClientIdentifier;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3ClientBuilder;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import jakarta.annotation.PostConstruct;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamBaseConstant;
import org.eclipse.ecsp.serializer.IngestionSerializerFactory;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * class HiveMqMqttDispatcher extends MqttDispatcher.
 */
@ConditionalOnExpression(
        "${" + PropertyNames.DMA_ENABLED + ":true} and '${"
                + PropertyNames.MQTT_CLIENT + "}'.equalsIgnoreCase('hivemq')")
@Component
@Scope("prototype")
public class HiveMqMqttDispatcher extends MqttDispatcher {
   
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(HiveMqMqttDispatcher.class);
   
    /** The message pay load. */
    byte[] messagePayLoad;
    
    /** The mqtt qos. */
    private MqttQos mqttQos;
    
    /** The mqtt client map. */
    private Map<String, Mqtt3AsyncClient> mqttClientMap;

    /**
     * Inits the.
     */
    @PostConstruct
    private void init() {
        mqttPlatformConfigMap = new ConcurrentHashMap<>();
        mqttClientMap = new ConcurrentHashMap<>();
        logger.info("Property loaded for mqtt broker to platformId mapping {}", mqttBrokerPlatformIdMapping);
        mqttQos = MqttQos.fromCode(mqttQosValue);
        if (mqttQos == null) { 
            logger.info("No QOS value defined against property mqtt.config.qos. Setting to default value");
            mqttQos = Mqtt3Publish.DEFAULT_QOS;
        }
        createMqttConfigForDefaultPlatform();
        createMqttConfigForPlatforms();
        initializeForcedHealthCheckEvent();
        healthMonitor.register(this);
        transformer = IngestionSerializerFactory.getInstance(igniteSerializationImpl);
        logger.info("Initialized HiveMqMqttDispatcher with QOS value for default platform as : {}", mqttQos);
    }

    /**
     * Creates the mqtt client.
     *
     * @param platform the platform
     */
    @Override
    protected void createMqttClient(String platform) {
        createMqtt3AsyncClient(platform);
    }

    /**
     * Creates the mqtt 3 async client.
     *
     * @param platform the platform
     */
    private void createMqtt3AsyncClient(String platform) {
        logger.debug("Fetching HiveMq MqttClient for platformID : {}", platform);
        Mqtt3AsyncClient client = mqttClientMap.get(platform);
        if (client == null || !client.getState().isConnected()) {
            logger.warn("Initializing HiveMQ MqttClient because it is null or not connected for platformID : {}", 
                    platform);
            Optional<Mqtt3AsyncClient> cl = getConnection(platform);
            if (!cl.isPresent()) {
                logger.error("Unable to establish connection to Mqtt broker with HiveMQ client for platformID : {}", 
                        platform);
                healthy = false;
                errorCounter.incErrorCounter(Optional.ofNullable(taskId), Exception.class);
            } else {
                client = cl.get();
                mqttClientMap.put(platform, client);
                healthy = true;
            }
        } else {
            logger.debug("HiveMQ Client with ID : {} is already connected. Need not reconnect for platformID : {}",
                    client.getConfig().getClientIdentifier(), platform);
        }
    }

    /**
     * Gets the connection.
     *
     * @param platform the platform
     * @return the connection
     */
    private Optional<Mqtt3AsyncClient> getConnection(String platform) {
        try {
            for (int i = 0; i < retryCount; i++) {
                Optional<Mqtt3AsyncClient> client = getMqttClient(platform);
                if (client.isPresent()) {
                    return client;
                }
                logger.debug("Retrying connecting to MQTT broker with HiveMQ client for platformID : {}. "
                        + "Current retry count : {}", platform, i);
                Thread.sleep(retryInterval);
            }
        } catch (InterruptedException e) {
            logger.error("Error occurred when sleeping during retry for creating connection to MQTT broker", e);
            Thread.currentThread().interrupt();
        }
        return Optional.empty();
    }
    
    /**
     * Get the Mqtt3ClientAsyncClient instance for the given platformId.

     * @param platform The platform identifier.
     * @return The instance of Mqtt3AsyncClient
     */
    public synchronized Optional<Mqtt3AsyncClient> getMqttClient(String platform) {
        Mqtt3AsyncClient client = null;
        logger.info("Creating HiveMQ MQTT client for platformID : {}", platform);
        Optional<MqttConfig> mqttConfigOpt = getMqttConfig(platform);
        if (!mqttConfigOpt.isPresent()) {
            logger.error("No MQTT config found for platformID : {}. HiveMQ MQTT client cannot be created.", platform);
            return Optional.ofNullable(client);
        }

        MqttConfig mqttConfig = mqttConfigOpt.get();
        try {
            String brokerUrl = mqttConfig.getBrokerUrl();
            int brokerPort = mqttConfig.getMqttBrokerPort();
            int mqttTimeoutInMillis = mqttConfig.getMqttTimeoutInMillis();
            String userName = mqttConfig.getMqttUserName();
            String password = mqttConfig.getMqttUserPassword();
            String mqttClientId = podName + Constants.HYPHEN + UUID.randomUUID();
            Mqtt3ClientBuilder mqtt3ClientBuilder = MqttClient.builder().identifier(mqttClientId)
                .serverHost(brokerUrl).serverPort(brokerPort)
                .useMqttVersion3();
            mqtt3ClientBuilder = checkAndApplySslConfig(mqtt3ClientBuilder, mqttConfig, platform);
            client = mqtt3ClientBuilder.transportConfig().mqttConnectTimeout(mqttTimeoutInMillis, TimeUnit.MILLISECONDS)
                .applyTransportConfig()
                .addConnectedListener(context -> logger.debug("HiveMq client with id {} is connected "
                        + "for platformID {}", mqttClientId, platform))
                .addDisconnectedListener(context -> logger.debug("HiveMq client with id {} is disconnected "
                        + "for platformID {}", mqttClientId, platform))
                .buildAsync();
            client.connectWith().keepAlive(keepAliveInterval).cleanSession(cleanSession)
                .simpleAuth().username(userName).password(password.getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .send()
                .whenComplete((connAck, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failure in HiveMQ client creation for platformID : {}. Exception is {}", 
                                platform, throwable);
                    } else {
                        logger.info("HiveMQ Mqtt Client with id {} is created successfully for platformID : {}", 
                                    mqttClientId, platform);
                    }
                });
        } catch (Exception e) {
            logger.error("HiveMQ MQTT client could not connect for platformID : {}. Exception while creating "
                    + "HiveMQ Mqtt client " + "and the error msg is : {}", platform, e);
        }
        return Optional.ofNullable(client);
    }

    /**
     * Check and apply ssl config.
     *
     * @param mqtt3ClientBuilder the mqtt 3 client builder
     * @param mqttConfig the mqtt config
     * @param platform the platform
     * @return the mqtt 3 client builder
     */
    Mqtt3ClientBuilder checkAndApplySslConfig(Mqtt3ClientBuilder mqtt3ClientBuilder, MqttConfig mqttConfig, 
            String platform) {
        if (StreamBaseConstant.DMA_ONE_WAY_TLS_AUTH_MECHANISM
                .equalsIgnoreCase(mqttConfig.getMqttClientAuthMechanism())) {
            return mqtt3ClientBuilder
                .sslConfig(MqttClientSslConfig.builder()
                               .trustManagerFactory(DMATLSFactory.getTrustManagerFactory(platform, mqttConfig))
                               .build());
        }
        return mqtt3ClientBuilder;
    }

    /**
     * Publish message to mqtt topic.
     *
     * @param mqttTopicName the mqtt topic name
     * @param isRetainedMessage the is retained message
     * @param platform the platform
     */
    @Override
    protected void publishMessageToMqttTopic(String mqttTopicName, boolean isRetainedMessage, String platform) {
        Mqtt3AsyncClient client = mqttClientMap.get(platform);
        if (null == client) {
            throw new NoMqttClientFoundException("Unable to publish message to topic : " + mqttTopicName + ". "
                    + "No MQTT client found against platformID : " + platform);
        }

        logger.debug("Publishing the event via HiveMQ client to the mqtt topic : {} ,with retained flag as : {} ,"
                + "for platformID : {}", mqttTopicName, isRetainedMessage, platform);
        Optional<MqttConfig> mqttConfigOpt = getMqttConfig(platform);
        MqttQos qos = (mqttConfigOpt.isPresent() ? MqttQos.fromCode(mqttConfigOpt.get().getMqttQosValue()) : mqttQos);
        client.publishWith().topic(mqttTopicName)
                .payload(messagePayLoad)
                .qos(qos)
                .retain(isRetainedMessage)
                .send();
    }

    /**
     * Sets the mqtt message payload.
     *
     * @param payload the new mqtt message payload
     */
    @Override
    protected void setMqttMessagePayload(byte[] payload) {
        messagePayLoad = payload;
    }

    /**
     * Close mqtt connections.
     */
    protected void closeMqttConnections() {
        for (Map.Entry<String, Mqtt3AsyncClient> entry : mqttClientMap.entrySet()) {
            closeMqttConnection(entry.getKey());
        }
    }

    /**
     * Close mqtt connection.
     *
     * @param platform the platform
     */
    @Override
    protected void closeMqttConnection(String platform) {
        Mqtt3AsyncClient client;
        MqttClientIdentifier mqttClientId = null;
        if (mqttClientMap.containsKey(platform)) {
            client = mqttClientMap.get(platform);
        } else {
            logger.info("No MQTT client found to be closed against platformID : {} in mqttClientMap", platform);
            return;
        }
        try {
            if (client != null && client.getState().isConnected()) {
                mqttClientId = client.getConfig().getClientIdentifier().orElse(null);
                client.disconnect();
                client = null;
            }
        } catch (Exception e1) {
            logger.error("Unable to disconnect the HiveMQ client for platformID : {} with clientID {} and "
                    + "error msg is {}.", platform, mqttClientId, e1);
        } finally {
            if (client == null || !client.getState().isConnected()) {
                logger.info("HiveMQ Client with ID: {} , for platformID : {} disconnected successfully", 
                        mqttClientId, platform);
            } else {
                logger.info("HiveMQ Client is still connected with client id {} , resetting reference to null.",
                        client.getConfig().getClientIdentifier());
                client = null;
            }
        }
    }

    /**
     * Sets the mqtt client map.
     *
     * @param mqttClientMap the mqtt client map
     */
    // setter for unit test cases
    void setMqttClientMap(Map<String, Mqtt3AsyncClient> mqttClientMap) {
        this.mqttClientMap = mqttClientMap;
    }
}
