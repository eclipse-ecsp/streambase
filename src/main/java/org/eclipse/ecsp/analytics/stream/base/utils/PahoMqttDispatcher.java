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

import jakarta.annotation.PostConstruct;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamBaseConstant;
import org.eclipse.ecsp.analytics.stream.base.exception.DeviceMessagingMqttClientTrustStoreException;
import org.eclipse.ecsp.serializer.IngestionSerializerFactory;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;



/**
 * class PahoMqttDispatcher extends MqttDispatcher.
 */
@ConditionalOnExpression(
        "${" + PropertyNames.DMA_ENABLED + ":true} and !('${"
                + PropertyNames.MQTT_CLIENT + "}'.equalsIgnoreCase('hivemq') )"
)
@Component
@Scope("prototype")
public class PahoMqttDispatcher extends MqttDispatcher {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(PahoMqttDispatcher.class);
    
    /** The mqtt message. */
    private MqttMessage mqttMessage;

    /** The mqtt client map. */
    private Map<String, MqttClient> mqttClientMap;

    /** The mqtt conn opts. */
    protected Map<String, MqttConnectOptions> mqttConnOpts;
    
    /** The Constant TLS_V1_2. */
    private static final String TLS_V1_2 = "TLSv1.2";

    /**
     * Inits the.
     */
    @PostConstruct
    private void init() {
        mqttPlatformConfigMap = new ConcurrentHashMap<>();
        mqttConnOpts = new ConcurrentHashMap<>();
        mqttClientMap = new ConcurrentHashMap<>();
        logger.info("Property loaded for mqtt broker to platformId mapping {}", mqttBrokerPlatformIdMapping);
        createMqttConfigForDefaultPlatform();
        createMqttConfigForPlatforms();
        initializeForcedHealthCheckEvent();
        healthMonitor.register(this);
        transformer = IngestionSerializerFactory.getInstance(igniteSerializationImpl);
    }

    /**
     * Creates the mqtt client.
     *
     * @param platform the platform
     */
    protected void createMqttClient(String platform) {
        createPahoMqttClient(platform);
    }
    
    /**
     * Creates the paho mqtt client.
     *
     * @param platform the platform
     */
    private void createPahoMqttClient(String platform) {
        logger.debug("Fetching Paho MqttClient for platformID : {}", platform);
        MqttClient client = mqttClientMap.get(platform);
        if (client == null || !client.isConnected()) {
            logger.warn("Initializing Paho MqttClient because it is null or not connected for platformID : {}", 
                    platform);
            Optional<MqttClient> cl = getConnection(platform);
            if (!cl.isPresent()) {
                logger.error("Unable to establish connection to Mqtt broker with PAHO client for platformID : {}", 
                        platform);
                healthy = false;
                errorCounter.incErrorCounter(Optional.ofNullable(taskId), MqttException.class);
            } else {
                client = cl.get();
                mqttClientMap.put(platform, client);
                healthy = true;
            }
        } else {
            logger.debug("Paho Client with ID : {} is already connected. "
                    + "Need not reconnect for platformID : {}", client.getClientId(), platform);
        }
    }

    /**
     * Publish message to mqtt topic.
     *
     * @param mqttTopicName the mqtt topic name
     * @param isRetainedMessage the is retained message
     * @param platform the platform
     * @throws MqttException the mqtt exception
     */
    @Override
    protected void publishMessageToMqttTopic(String mqttTopicName, boolean isRetainedMessage, String platform) 
            throws MqttException {
        MqttClient client = mqttClientMap.get(platform);
        if (null == client) {
            throw new NoMqttClientFoundException("Unable to publish message to topic : " + mqttTopicName
                    + ". No MQTT client found against platformID : " + platform);
        }
        Optional<MqttConfig> mqttConfigOpt = getMqttConfig(platform);
        int qos = (mqttConfigOpt.isPresent() ? mqttConfigOpt.get().getMqttQosValue() : mqttQosValue);
        logger.debug("Publishing event via PAHO client to the mqtt topic : {}, with retained flag as {}, "
                + "platformId {}, clientID {}", mqttTopicName, isRetainedMessage, platform, client.getClientId());
        mqttMessage.setRetained(isRetainedMessage);
        mqttMessage.setQos(qos);
        client.publish(mqttTopicName, mqttMessage);
    }

    /**
     * Sets the mqtt message payload.
     *
     * @param payload the new mqtt message payload
     */
    @Override
    protected void setMqttMessagePayload(byte[] payload) {
        mqttMessage = new MqttMessage();
        mqttMessage.setPayload(payload);
    }

    /**
     * Key is vehicleID and value is event that needs to be send to Mqtt topic.
     *
     * @param platform the platform
     * @return the connection
     */

    private Optional<MqttClient> getConnection(String platform) {
        try {
            for (int i = 0; i < retryCount; i++) {
                Optional<MqttClient> client = getMqttClient(platform);
                if (client.isPresent()) {
                    return client;
                }
                logger.debug("Retrying connecting to MQTT broker with PAHO client for platformID : {}. "
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
     * Method to fetch the mqtt client.
     *
     * @param platform the platform
     * @return MqttClient
     */
    public synchronized Optional<MqttClient> getMqttClient(String platform) {
        MqttClient client = null;
        logger.info("Creating Paho MQTT client for platformID : {}", platform);
        Optional<MqttConnectOptions> connectOptions = getMqttConnectOptions(platform);
        Optional<MqttConfig> mqttConfig = getMqttConfig(platform);
        if (connectOptions.isPresent() && mqttConfig.isPresent()) {
            try {
                String mqttClientId = podName + Constants.HYPHEN + UUID.randomUUID().toString();
                client = new MqttClient(mqttConfig.get().getBrokerUrl(), mqttClientId, null);
                client.setTimeToWait(mqttConfig.get().getMqttTimeoutInMillis());
                client.connect(connectOptions.get());
                logger.info("Paho MQTT client creation is successful with client ID : {}, for platformID : {}",
                        client.getClientId(), platform);
            } catch (MqttException e) {
                logger.error("Paho MQTT client could not connect for platformID : {}. "
                        + "Exception while creating Paho Mqtt client and the error msg is : {}", platform, e);
            }
        } else {
            logger.error("Error attempting to create connection with Paho MQTT client. "
                    + "No ConnectOptions or MqttConfig found for platformID : {}", platform);
        }
        return Optional.ofNullable(client);

    }

    /**
     * Sets the mqtt connection opt.
     *
     * @param mqttPlatformId the mqtt platform id
     * @param mqttConfig the mqtt config
     */
    @Override
    protected void setMqttConnectionOpt(String mqttPlatformId, MqttConfig mqttConfig) {
        MqttConnectOptions connOpt = new MqttConnectOptions();
        logger.debug("Creating MQTT connection options for MQTT broker against platformId {}",  mqttPlatformId);
        connOpt.setUserName(mqttConfig.getMqttUserName());
        connOpt.setPassword(mqttConfig.getMqttUserPassword().toCharArray());
        connOpt.setServerURIs(new String[]{mqttConfig.getBrokerUrl()});
        connOpt.setCleanSession(mqttConfig.isCleanSession());
        connOpt.setKeepAliveInterval(mqttConfig.getKeepAliveInterval());
        connOpt.setMaxInflight(mqttConfig.getMaxInflight());
        if (StreamBaseConstant.DMA_ONE_WAY_TLS_AUTH_MECHANISM
                .equalsIgnoreCase(mqttConfig.getMqttClientAuthMechanism())) {
            connOpt.setSocketFactory(getSocketFactory(mqttPlatformId, mqttConfig));
        }
        mqttConnOpts.put(mqttPlatformId, connOpt);
    }

    /**
     * Getting Mqtt connection option object to create MQTT client. Always use
     * this method
     *
     * @param platform the platform
     * @return MqttConnectOptions instance.
     */
    protected Optional<MqttConnectOptions> getMqttConnectOptions(String platform) {
        return Optional.ofNullable(mqttConnOpts.get(platform));
    }

    /**
     * Method to create SSLSocketFactory from Truststore details.
     *
     * @param platformId the platform id
     * @param mqttConfig the mqtt config
     * @return the socket factory
     */
    private static SSLSocketFactory getSocketFactory(String platformId, MqttConfig mqttConfig) {
        try {
            TrustManagerFactory trustManagerFactory = DMATLSFactory.getTrustManagerFactory(platformId, mqttConfig);
            SSLContext sslContext = SSLContext.getInstance(TLS_V1_2);
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            return sslContext.getSocketFactory();
        } catch (NoSuchAlgorithmException  | KeyManagementException exception) {
            throw new DeviceMessagingMqttClientTrustStoreException(
                "Error encountered either while loading the TrustStore: " + mqttConfig.getMqttServiceTrustStorePath()
                    + " or in initializing the TrustManagerFactory. Exception is: " + exception,
                exception.getCause());
        }
    }

    /**
     * Close mqtt connections.
     */
    @Override
    protected void closeMqttConnections() {
        for (Map.Entry<String, MqttClient> entry : mqttClientMap.entrySet()) {  
            closeMqttConnection(entry.getKey());
        }
    }
    
    /**
     * Close mqtt connection.
     *
     * @param platform the platform
     */
    @SuppressWarnings("resource")
    @Override
    protected void closeMqttConnection(String platform) {
        MqttClient client;
        String mqttClientId = null;
        if (mqttClientMap.containsKey(platform)) {
            client = mqttClientMap.get(platform);
        } else {
            logger.info("No MQTT client found to be closed against platformID : {} in mqttClientMap", platform);
            return;
        }
        try {
            if (client != null && client.isConnected()) {
                mqttClientId = client.getClientId();
                client.disconnect();
                client.close();
                client = null;
            }
        } catch (MqttException mqttException) {
            logger.error("Unable to close the Paho client with id {}, for platformID {} and error msg is {}. "
                    + "Trying to disconnect forcibly! ", client.getClientId(), platform, mqttException);
            try {
                client.disconnectForcibly();
                client.close();
                client = null;
            } catch (MqttException exception) {
                logger.error("Exception while disconnecting Paho client with id {} forcibly: {} ", 
                        client.getClientId(), exception);
            }
        } finally {
            if (client == null || !client.isConnected()) {
                logger.info("Paho Client with ID : {} disconnected successfully, for platformID : {}.", 
                        mqttClientId, platform);
            } else {
                logger.info("Unable to disconnect Paho Client with id {} , for platformID : {}. "
                        + "Resetting reference to null.", mqttClientId, platform);
                client = null;
            }
            mqttClientMap.put(platform, null);
        }
    }

    /**
     * Sets the mqtt client map.
     *
     * @param mqttClientMap the mqtt client map
     */
    // setter for unit test cases
    void setMqttClientMap(Map<String, MqttClient> mqttClientMap) {
        this.mqttClientMap = mqttClientMap;
    }
}
