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
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamBaseConstant;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.platform.MqttTopicNameGenerator;
import org.eclipse.ecsp.domain.AbstractBlobEventData.Encoding;
import org.eclipse.ecsp.domain.BlobDataV1_0;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.IgniteEventSource;
import org.eclipse.ecsp.entities.IgniteBlobEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageErrorCode;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.serializer.IngestionSerializer;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageHandler;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.ecsp.utils.metrics.IgniteErrorCounter;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Class that dispatches data to Mqtt topic.
 */
@Component
@Scope("prototype")
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public abstract class MqttDispatcher implements Dispatcher<IgniteKey<?>, DeviceMessage> {

    /** The Constant THRESHOLD. */
    protected static final int THRESHOLD = (Integer.MAX_VALUE - 2);
    // RTC-155383 - Running Kafka and Zookeeper on dynamic ports to resolve bind
    // address issue in streambase project while running test cases
    /** The overridden mqtt broker url. */
    // This url will be used while running test cases
    protected static String overriddenMqttBrokerUrl = null;
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(MqttDispatcher.class);

    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /** The health monitor. */
    @Autowired
    protected MqttHealthMonitor healthMonitor;
    
    /** The broker url. */
    // mqtt broker url
    @Value("${" + PropertyNames.MQTT_BROKER_URL + "}")
    protected String brokerUrl;
    
    /** The mqtt qos value. */
    // QoS = Quality of Service
    @Value("${" + PropertyNames.MQTT_CONFIG_QOS + "}")
    protected int mqttQosValue;

    /** The mqtt client auth mechanism. */
    //client auth mechanism
    @Value("${" + PropertyNames.MQTT_CLIENT_AUTH_MECHANISM + "}")
    protected String mqttClientAuthMechanism;

    /** The user name. */
    // mqtt user name
    @Value("${" + PropertyNames.MQTT_USER_NAME + "}")
    protected String userName;
    
    /** The password. */
    // mqtt password
    @Value("${" + PropertyNames.MQTT_USER_PASSWORD + "}")
    protected String password;
    
    /** The max inflight. */
    // mqtt maxInflight
    @Value("${" + PropertyNames.MQTT_MAX_INFLIGHT + "}")
    protected int maxInflight;
    
    /** The retry count. */
    @Value("${" + PropertyNames.MQTT_CONNECTION_RETRY_COUNT + ":3}")
    protected int retryCount;
    
    /** The mqtt timeout in millis. */
    @Value("${" + PropertyNames.MQTT_TIMEOUT_IN_MILLIS + ":60000}")
    protected int mqttTimeoutInMillis;
    
    /** The retry interval. */
    @Value("${" + PropertyNames.MQTT_CONNECTION_RETRY_INTERVAL + ":1000}")
    protected long retryInterval;
    
    /** The ignite serialization impl. */
    @Value("${" + PropertyNames.INGESTION_SERIALIZER_CLASS
            + ":org.eclipse.ecsp.serializer.IngestionSerializerFstImpl}")
    protected String igniteSerializationImpl;
    
    /** The wrap dispatch event. */
    // Below properties are for tracing
    @Value("${" + PropertyNames.WRAP_DISPATCH_EVENT + ":false}")
    protected boolean wrapDispatchEvent;
    
    /** The event wrap frequency. */
    @Value("${" + PropertyNames.EVENT_WRAP_FREQUENCY + ":10}")
    protected int eventWrapFrequency;
    
    /** The mqtt service trust store path. */
    @Value("${" + PropertyNames.MQTT_SERVICE_TRUSTSTORE_PATH + "}")
    protected String mqttServiceTrustStorePath;

    /** The mqtt service trust store password. */
    @Value("${" + PropertyNames.MQTT_SERVICE_TRUSTSTORE_PASSWORD + "}")
    protected String mqttServiceTrustStorePassword;

    /** The mqtt service trust store type. */
    @Value("${" + PropertyNames.MQTT_SERVICE_TRUSTSTORE_TYPE + "}")
    protected String mqttServiceTrustStoreType;

    /** The pod name. */
    /*
     * Below property's value will be used to set the mqtt client ID connecting
     * to HiveMQ. So that while debugging any issue, we get to know which pod
     * connected to or disconnected from HiveMQ.
     *
     * RDNG: 170796
     */
    @Value("${HOSTNAME:localhost}")
    protected String podName;
    
    /** The mqtt broker port. */
    @Value("${" + PropertyNames.MQTT_BROKER_PORT + ":1883}")
    protected int mqttBrokerPort;
    
    /** The keep alive interval. */
    @Value("${" + PropertyNames.MQTT_KEEP_ALIVE_INTERVAL + ":120}")
    protected int keepAliveInterval;
    
    /** The event dispatch counter. */
    protected AtomicInteger eventDispatchCounter = new AtomicInteger(1);

    /** The mqtt topic name service impl. */
    @Value("${" + PropertyNames.MQTT_TOPIC_GENERATOR_SERVICE_IMPL_CLASS_NAME + ":" 
            + PropertyNames.DEFAULT_TOPIC_NAME_GENERATOR_IMPL + "}")
    private String mqttTopicNameServiceImpl;

    /** The device message utils. */
    @Autowired
    private DeviceMessageUtils deviceMessageUtils;

    /** The mqtt topic name generator. */
    @Autowired
    private MqttTopicNameGenerator mqttTopicNameGenerator;
    
    /** The healthy. */
    protected volatile boolean healthy;
    
    /** The transformer. */
    protected IngestionSerializer transformer;
    
    /** The global broadcast retention topic list. */
    @Value("#{'${" + PropertyNames.MQTT_GLOBAL_BROADCAST_RETENTION_TOPICS + "}'.split(',')}")
    protected List<String> globalBroadcastRetentionTopicList;
    
    /** The clean session. */
    @Value("${" + PropertyNames.MQTT_CLEAN_SESSION + ":false}")
    protected boolean cleanSession;
    
    /** The error counter. */
    @Autowired
    protected IgniteErrorCounter errorCounter;
    
    /** The task id. */
    protected String taskId;
    
    /** The mqtt broker platform id mapping. */
    @Value("#{${mqtt.broker.platformId.mapping: {:} }}")
    protected Map<String, String> mqttBrokerPlatformIdMapping;
    
    /** The mqtt platform config map. */
    protected Map<String, MqttConfig> mqttPlatformConfigMap;

    /** The environment. */
    @Autowired
    private Environment environment;
    
    /** The dma post dispatch handler. */
    private DeviceMessageHandler dmaPostDispatchHandler;
    
    /** The forced check value. */
    private DeviceMessage forcedCheckValue;
    
    /** The forced check key. */
    private IgniteStringKey forcedCheckKey;

    /**
     * Method to verify if all the required properties are valid, and not null
     * or empty.
     */
    @PostConstruct
    private void verifyAndDumpMqttProperties() {
        ObjectUtils.requireNonEmpty(brokerUrl, "Broker url shouldn't be null or empty.");
        ObjectUtils.requireNonNegative(mqttQosValue, "Mqtt Qos (Quality of Service) value cannot be negative.");
        ObjectUtils.requireNonEmpty(userName, "Username shouldn't be null or empty.");
        ObjectUtils.requireNonEmpty(password, "Password shouldn't be null or empty.");
        
        if (StreamBaseConstant.DMA_ONE_WAY_TLS_AUTH_MECHANISM.equalsIgnoreCase(mqttClientAuthMechanism)) {
            ObjectUtils.requireNonEmpty(mqttServiceTrustStorePath, Constants.MQTT_CLIENT_AS_ONE_WAY_TLS
                + " mqttServiceTrustStorePath" + Constants.CANNOT_BE_NULL_ERROR_MSG);
            ObjectUtils.requireNonEmpty(mqttServiceTrustStorePassword, Constants.MQTT_CLIENT_AS_ONE_WAY_TLS
                + " mqttServiceTrustStorePassword" + Constants.CANNOT_BE_NULL_ERROR_MSG);
            ObjectUtils.requireNonEmpty(mqttServiceTrustStoreType, Constants.MQTT_CLIENT_AS_ONE_WAY_TLS
                + " mqttServiceTrustStoreType" + Constants.CANNOT_BE_NULL_ERROR_MSG);
        }

        validateEventWrapFrequency();
        // RTC-155383 - Running Kafka and Zookeeper on dynamic ports to resolve
        // bind address issue in streambase project while running test cases
        if (null != overriddenMqttBrokerUrl) {
            brokerUrl = overriddenMqttBrokerUrl;
        }
        logger.debug("Mqtt clean Session value: {}", cleanSession);
        logger.info("Mqtt Broker url:{}, QoS:{}, userName:{}, mqttClientAuthMechanism:{}", 
                brokerUrl, mqttQosValue, userName, mqttClientAuthMechanism);
        logger.info("Global Broadcast Topic List configured for retaining messages : {}", 
                globalBroadcastRetentionTopicList);
    }

    /**
     * Validate event wrap frequency.
     */
    void validateEventWrapFrequency() {
        if (eventWrapFrequency < 1) {
            throw new IllegalArgumentException("Value for property event.wrap.frequency should be greater than 0");
        }
    }

    /**
     * Dispatch.
     *
     * @param key the key
     * @param entity the entity
     */
    @Override
    public void dispatch(IgniteKey key, DeviceMessage entity) {
        if (invalidKeyOrValue(key, entity)) {
            return;
        }
        String platform = (StringUtils.isBlank(entity.getEvent().getPlatformId()) ? PropertyNames.DEFAULT_PLATFORMID
                : entity.getEvent().getPlatformId());
        logger.debug("Dispatching key:{} and value:{} to MQTT with platformID : {}", 
                key.toString(), entity.toString(), platform);
        createMqttClient(platform);
        DeviceMessageHeader header = entity.getDeviceMessageHeader();
        Optional<String> mqttTopicNameOpt = mqttTopicNameGenerator.getMqttTopicName(key, header, 
                entity.getEvent().getEventId());
        if (!mqttTopicNameOpt.isPresent()) {
            logger.error("Received Empty topic name. Not proceeding further.");
            return;
        }
        String mqttTopicName = mqttTopicNameOpt.get();
        logger.info("Mqtt Topic Name fetched from " + mqttTopicNameServiceImpl + " class is " + mqttTopicName);
        
        /*
         * DataPlatform 101-HCP-12088, All the events going to Mqtt are
         * whitelisted and Mqtt Topic is vehicleId/eventId
         *
         * Later per vehicleID we will see whether this vehicleID has subscribed
         * to this event or not
         */
        byte[] payLoad = entity.getMessage();
        if (wrapDispatchEvent && (eventDispatchCounter.getAndIncrement() % eventWrapFrequency == 0)) {
            setMqttMessagePayload(transformToIgniteBlobEvent(payLoad, header.getTargetDeviceId(), 
                    header.getVehicleId(), header.getRequestId()));
        } else {
            setMqttMessagePayload(payLoad);
        }
        eventDispatchCounter.compareAndSet(THRESHOLD, 0);
        boolean isRetainedMessage = (null != globalBroadcastRetentionTopicList) 
                && !globalBroadcastRetentionTopicList.isEmpty() 
                && (header.isGlobalTopicNameProvided()) && (globalBroadcastRetentionTopicList.contains(mqttTopicName));
        try {
            publishMessageToMqttTopic(mqttTopicName, isRetainedMessage, platform);
            logger.info("Successfully published the event : {}, to the mqtt topic : {}, "
                    + "with retained flag as {}, for platformID {}", entity.getEvent(), 
                    mqttTopicName, isRetainedMessage, platform);
            healthy = true;
            if (null != dmaPostDispatchHandler) {
                dmaPostDispatchHandler.handle(key, entity);
            }
        } catch (Exception e) {
            if (header.isGlobalTopicNameProvided()) {
                DeviceMessageFailureEventDataV1_0 failEventData = new DeviceMessageFailureEventDataV1_0();
                failEventData.setFailedIgniteEvent(entity.getEvent());
                failEventData.setErrorCode(DeviceMessageErrorCode.MQTT_DISPATCH_FAILED);
                deviceMessageUtils.postFailureEvent(failEventData, key, spc, entity.getFeedBackTopic());
            }
            errorCounter.incErrorCounter(Optional.ofNullable(taskId), e.getClass());
            logger.error("Unable to push the event:{} to the mqtt topic:{} for platform:{}, with exception: {}",
                    entity.toString(), mqttTopicName, platform, e);
            /*
             * DataPlatform 101-HCP-12088, We will not do any retry if
             * exception is thrown.
             *
             * Future: We will retry sending the event for a configurable amount
             * of time before bailing out
             */
            healthy = false;
            closeMqttConnection(platform);
        }
    }

    /**
     * Invalid key or value.
     *
     * @param key the key
     * @param entity the entity
     * @return true, if successful
     */
    private boolean invalidKeyOrValue(IgniteKey<?> key, DeviceMessage entity) {
        if (null == key) {
            logger.error("Null key received. Data will not be processed further.");
            return true;
        }
        if (null == entity) {
            logger.error("Null value received. Data will not be processed further .");
            return true;
        }
        return false;
    }

    /**
     * Publish message to mqtt topic.
     *
     * @param mqttTopicName the mqtt topic name
     * @param isRetainedMessage the is retained message
     * @param platform the platform
     * @throws MqttException the mqtt exception
     */
    protected abstract void publishMessageToMqttTopic(String mqttTopicName,
            boolean isRetainedMessage, String platform) throws MqttException;

    /**
     * Sets the mqtt message payload.
     *
     * @param payload the new mqtt message payload
     */
    protected abstract void setMqttMessagePayload(byte[] payload);

    /**
     * Creates the mqtt client.
     *
     * @param platform the platform
     */
    protected abstract void createMqttClient(String platform);

    /**
     * Close mqtt connections.
     */
    protected abstract void closeMqttConnections();

    /**
     * Close mqtt connection.
     *
     * @param platform the platform
     */
    protected abstract void closeMqttConnection(String platform);

    /**
     * Close.
     */
    public void close() {
        closeMqttConnections();
    }

    /**
     * Transform to ignite blob event.
     *
     * @param payload the payload
     * @param deviceId the device id
     * @param vehicleId the vehicle id
     * @param requestId the request id
     * @return the byte[]
     */
    protected byte[] transformToIgniteBlobEvent(byte[] payload, String deviceId,
            String vehicleId, String requestId) {
        IgniteBlobEvent igniteBlobEvent = new IgniteBlobEvent();
        igniteBlobEvent.setTimestamp(System.currentTimeMillis());
        igniteBlobEvent.setSourceDeviceId(deviceId);
        igniteBlobEvent.setVehicleId(vehicleId);
        igniteBlobEvent.setVersion(org.eclipse.ecsp.domain.Version.V1_0);
        igniteBlobEvent.setEventId(EventID.BLOBDATA);
        igniteBlobEvent.setRequestId(requestId);
        BlobDataV1_0 blobData = new BlobDataV1_0();
        blobData.setEventSource(IgniteEventSource.IGNITE);
        blobData.setEncoding(Encoding.JSON);
        blobData.setPayload(payload);
        igniteBlobEvent.setEventData(blobData);
        return transformer.serialize(igniteBlobEvent);
    }

    /**
     * Sets the up.
     *
     * @param taskId the new up
     */
    public void setup(String taskId) {
        this.taskId = taskId;
    }

    /**
     * isHealthy().
     *
     * @param forceHealthCheck
     *         forceHealthCheck
     * @return boolean
     */
    public boolean isHealthy(boolean forceHealthCheck) {

        if (forceHealthCheck) {
            dispatch(forcedCheckKey, forcedCheckValue);
        }
        return healthy;
    }

    /**
     * initializeForcedHealthCheckEvent().
     */
    public void initializeForcedHealthCheckEvent() {
        forcedCheckKey = new IgniteStringKey();
        forcedCheckKey.setKey(Constants.FORCED_HEALTH_CHECK_DEVICE_ID);
        ForcedHealthCheckEvent forcedHealthCheckEvent = new ForcedHealthCheckEvent();
        forcedHealthCheckEvent.setDevMsgTopicSuffix(Constants.FORCED_HEALTH_DEFAULT_TEST_TOPIC_NAME);
        forcedHealthCheckEvent.setTargetDeviceId(Constants.FORCED_HEALTH_CHECK_DEVICE_ID);
        forcedCheckValue = new DeviceMessage();
        forcedCheckValue.setMessage("forcedHealthCheckDummyMsg".getBytes());
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withTargetDeviceId(Constants.FORCED_HEALTH_CHECK_DEVICE_ID);
        header.withDevMsgTopicSuffix(Constants.FORCED_HEALTH_DEFAULT_TEST_TOPIC_NAME);
        forcedCheckValue.setEvent(forcedHealthCheckEvent);
        forcedCheckValue.setDeviceMessageHeader(header);
    }
    
    /**
     * Sets the mqtt config for default platform id.
     *
     * @param mqttConfig the new mqtt config for default platform id
     */
    private void setMqttConfigForDefaultPlatformId(MqttConfig mqttConfig) {
        mqttConfig.setMqttUserName(userName);
        mqttConfig.setMqttUserPassword(password);
        mqttConfig.setBrokerUrl(brokerUrl);
        mqttConfig.setMqttBrokerPort(mqttBrokerPort);
        mqttConfig.setMqttQosValue(mqttQosValue);
        mqttConfig.setMaxInflight(maxInflight);
        mqttConfig.setMqttTimeoutInMillis(mqttTimeoutInMillis);
        mqttConfig.setKeepAliveInterval(keepAliveInterval);
        mqttConfig.setCleanSession(cleanSession);
        mqttConfig.setMqttClientAuthMechanism(mqttClientAuthMechanism);
        if (StreamBaseConstant.DMA_ONE_WAY_TLS_AUTH_MECHANISM.equalsIgnoreCase(mqttClientAuthMechanism)) {
            mqttConfig.setMqttServiceTrustStorePath(mqttServiceTrustStorePath);
            mqttConfig.setMqttServiceTrustStorePassword(mqttServiceTrustStorePassword);
            mqttConfig.setMqttServiceTrustStoreType(mqttServiceTrustStoreType);
        }
    }
    
    /**
     * Validate ssl configs.
     *
     * @param mqttServiceTrustStorePasswordPlatform the mqtt service trust store password platform
     * @param mqttServiceTrustStorePathPlatform the mqtt service trust store path platform
     * @param mqttServiceTrustStoreTypePlatform the mqtt service trust store type platform
     * @param platform the platform
     */
    private void validateSslConfigs(String mqttServiceTrustStorePasswordPlatform, 
            String mqttServiceTrustStorePathPlatform, String mqttServiceTrustStoreTypePlatform, 
            String platform) {
        ObjectUtils.requireNonEmpty(mqttServiceTrustStorePasswordPlatform,
                "MQTT Truststore password for a given platformID: " + platform 
                + Constants.CANNOT_BE_NULL_ERROR_MSG);
        ObjectUtils.requireNonEmpty(mqttServiceTrustStorePathPlatform,
                "MQTT Truststore path for a given platformID: " + platform 
                + Constants.CANNOT_BE_NULL_ERROR_MSG);
        ObjectUtils.requireNonEmpty(mqttServiceTrustStoreTypePlatform,
                "MQTT TrustStore type for a given platformID: " + platform 
                + Constants.CANNOT_BE_NULL_ERROR_MSG);
    }
    
    /**
     * Validate mqtt configs.
     *
     * @param platform the platform
     * @param mqttBrokerUrl the mqtt broker url
     * @param mqttUserName the mqtt user name
     * @param mqttPass the mqtt pass
     */
    private void validateMqttConfigs(String platform, String mqttBrokerUrl, String mqttUserName, String mqttPass) {
        ObjectUtils.requireNonEmpty(mqttBrokerUrl,
                "MQTT broker url for a given platformID: " + platform + Constants.CANNOT_BE_NULL_ERROR_MSG);
        ObjectUtils.requireNonEmpty(mqttUserName,
                "MQTT username for a given platformID: " + platform + Constants.CANNOT_BE_NULL_ERROR_MSG);
        ObjectUtils.requireNonEmpty(mqttPass,
                "MQTT password for a given platformID: " + platform + Constants.CANNOT_BE_NULL_ERROR_MSG);
    }
    
    /**
     * Sets the mqtt properties.
     *
     * @param mqttConfig the mqtt config
     * @param mqttUserName the mqtt user name
     * @param mqttPass the mqtt pass
     * @param mqttBrokerUrl the mqtt broker url
     */
    private void setMqttProperties(MqttConfig mqttConfig, String mqttUserName, String mqttPass, String mqttBrokerUrl) {
        mqttConfig.setMqttUserName(mqttUserName);
        mqttConfig.setMqttUserPassword(mqttPass);
        mqttConfig.setBrokerUrl(mqttBrokerUrl);
    }
    
    /**
     * Gets the mqtt config for broker.
     *
     * @param platform the platform
     * @param mqttBroker the mqtt broker
     * @return the mqtt config for broker
     */
    protected MqttConfig getMqttConfigForBroker(String platform, String mqttBroker) {
        MqttConfig mqttConfig = new MqttConfig();
        if (StringUtils.equalsIgnoreCase(platform, PropertyNames.DEFAULT_PLATFORMID)) {
            setMqttConfigForDefaultPlatformId(mqttConfig);
        } else {
            String mqttBrokerUrl = environment.getProperty(mqttBroker + PropertyNames.MQTT_BROKER_URL_SUFFIX);
            String mqttUserName = environment.getProperty(mqttBroker + PropertyNames.MQTT_USER_NAME_SUFFIX);
            String mqttPass = environment.getProperty(mqttBroker + PropertyNames.MQTT_USER_PASSWORD_SUFFIX);
            String mqttClientAuthMechanismPlatform = environment.getProperty(mqttBroker 
                    + PropertyNames.MQTT_CLIENT_AUTH_MECHANISM_SUFFIX, String.class);
            mqttConfig.setMqttClientAuthMechanism(mqttClientAuthMechanismPlatform);
            String mqttServiceTrustStorePasswordPlatform = null;
            String mqttServiceTrustStorePathPlatform = null;
            String mqttServiceTrustStoreTypePlatform = null;

            try {
                if (StreamBaseConstant.DMA_ONE_WAY_TLS_AUTH_MECHANISM
                        .equalsIgnoreCase(mqttClientAuthMechanismPlatform)) {
                    mqttServiceTrustStorePasswordPlatform = environment.getProperty(mqttBroker 
                            + PropertyNames.MQTT_SERVICE_TRUSTSTORE_PASSWORD_SUFFIX, String.class);
                    mqttServiceTrustStorePathPlatform = environment.getProperty(mqttBroker 
                            + PropertyNames.MQTT_SERVICE_TRUSTSTORE_PATH_SUFFIX, String.class);
                    mqttServiceTrustStoreTypePlatform = environment.getProperty(mqttBroker 
                            + PropertyNames.MQTT_SERVICE_TRUSTSTORE_TYPE_SUFFIX, String.class);
                    validateSslConfigs(mqttServiceTrustStorePasswordPlatform, mqttServiceTrustStorePathPlatform,
                            mqttServiceTrustStoreTypePlatform, platform);
                }
                validateMqttConfigs(platform, mqttBrokerUrl, mqttUserName, mqttPass);
            } catch (Exception e) {
                logger.error("Error occurred when verifying properties for platformID : " + platform, e);
                return null;
            }

            final Integer mqttBrokerPortPlatform = 
                    environment.getProperty(mqttBroker + PropertyNames.MQTT_BROKER_PORT_SUFFIX, Integer.class);
            final int mqttQosValuePlatform = environment.getProperty(mqttBroker + PropertyNames.MQTT_CONFIG_QOS_SUFFIX, 
                    Integer.class, this.mqttQosValue);
            final int mqttMaxInflight = environment.getProperty(mqttBroker + PropertyNames.MQTT_MAX_INFLIGHT_SUFFIX, 
                    Integer.class, this.maxInflight);
            final int mqttTimeoutInMilli = environment.getProperty(mqttBroker 
                    + PropertyNames.MQTT_TIMEOUT_IN_MILLIS_SUFFIX, Integer.class, this.mqttTimeoutInMillis);
            final int mqttKeepAliveInterval = environment.getProperty(mqttBroker 
                    + PropertyNames.MQTT_KEEP_ALIVE_INTERVAL_SUFFIX, Integer.class, this.keepAliveInterval);
            final boolean mqttCleanSession = environment.getProperty(mqttBroker 
                    + PropertyNames.MQTT_CLEAN_SESSION_SUFFIX, Boolean.class, this.cleanSession);
            setMqttProperties(mqttConfig, mqttUserName, mqttPass, mqttBrokerUrl);
            if (mqttBrokerPortPlatform != null) {
                mqttConfig.setMqttBrokerPort(mqttBrokerPortPlatform.intValue());
            }
            mqttConfig.setMqttServiceTrustStorePassword(mqttServiceTrustStorePasswordPlatform);
            mqttConfig.setMqttServiceTrustStorePath(mqttServiceTrustStorePathPlatform);
            mqttConfig.setMqttServiceTrustStoreType(mqttServiceTrustStoreTypePlatform);
            mqttConfig.setMqttQosValue(mqttQosValuePlatform);
            mqttConfig.setMaxInflight(mqttMaxInflight);
            mqttConfig.setMqttTimeoutInMillis(mqttTimeoutInMilli);
            mqttConfig.setKeepAliveInterval(mqttKeepAliveInterval);
            mqttConfig.setCleanSession(mqttCleanSession);
        }
        logger.info("Mqtt config created for platformId {} - {}", platform, mqttConfig);
        mqttPlatformConfigMap.put(platform, mqttConfig);
        return mqttConfig;
    }

    /**
     * Sets the mqtt connection opt.
     *
     * @param mqttPlatformId the mqtt platform id
     * @param mqttConfig the mqtt config
     */
    protected void setMqttConnectionOpt(String mqttPlatformId, MqttConfig mqttConfig) {
        //
    }

    /**
     * Creates the mqtt config for platforms.
     */
    protected void createMqttConfigForPlatforms() {
        if (!mqttBrokerPlatformIdMapping.isEmpty()) {
            for (Map.Entry<String, String> entry : mqttBrokerPlatformIdMapping.entrySet()) {
                String platform = entry.getKey();
                String mqttBroker = entry.getValue();
                logger.info("Processing properties for mqtt broker {}, against platformID {}", mqttBroker, platform);
                if (StringUtils.isEmpty(platform) || StringUtils.isEmpty(mqttBroker)) {
                    logger.info("MQTT broker or platformID not set in mqtt.broker.platformId.mapping "
                            + "Client will not be created for mqtt broker : {} , platform : {}.", mqttBroker, platform);
                    continue;
                }
                MqttConfig mqttConfig = getMqttConfigForBroker(platform, mqttBroker);
                if (null != mqttConfig) {
                    setMqttConnectionOpt(platform, mqttConfig);
                }
            }
        }
    }

    /**
     * Creates the mqtt config for default platform.
     */
    protected void createMqttConfigForDefaultPlatform() {
        MqttConfig mqttConfig = getMqttConfigForBroker(PropertyNames.DEFAULT_PLATFORMID, null);
        if (mqttConfig != null) {
            setMqttConnectionOpt(PropertyNames.DEFAULT_PLATFORMID, mqttConfig);
        }
    }

    /**
     * Getting Mqtt connection option object to create Mqtt client. Always use
     * this method
     *
     * @param platform the platform
     * @return MqttConfig instance with all the configurations set for the Mqtt Client.
     */
    public Optional<MqttConfig> getMqttConfig(String platform) {
        return Optional.ofNullable(mqttPlatformConfigMap.get(platform));
    }

    /**
     * Sets the next handler.
     *
     * @param dmaPostDispatchHandler the new next handler
     */
    public void setNextHandler(DeviceMessageHandler dmaPostDispatchHandler) {
        this.dmaPostDispatchHandler = dmaPostDispatchHandler;
    }

    /**
     * Sets the stream processing context.
     *
     * @param ctx the ctx
     */
    public void setStreamProcessingContext(StreamProcessingContext<IgniteKey<?>, IgniteEvent> ctx) {
        this.spc = ctx;
    }

    /**
     * Sets the event wrap frequency.
     *
     * @param eventWrapFrequency the new event wrap frequency
     */
    // Below setters are for testing
    void setEventWrapFrequency(int eventWrapFrequency) {
        this.eventWrapFrequency = eventWrapFrequency;
    }

    /**
     * Sets the wrap dispatch event.
     *
     * @param wrapDispatchEvent the new wrap dispatch event
     */
    void setWrapDispatchEvent(boolean wrapDispatchEvent) {
        this.wrapDispatchEvent = wrapDispatchEvent;
    }

    /**
     * Sets the transformer.
     *
     * @param transformer the new transformer
     */
    void setTransformer(IngestionSerializer transformer) {
        this.transformer = transformer;
    }

    /**
     * Sets the global broadcast retention topic list.
     *
     * @param globalBroadcastRetentionTopicList the new global broadcast retention topic list
     */
    void setGlobalBroadcastRetentionTopicList(List<String> globalBroadcastRetentionTopicList) {
        this.globalBroadcastRetentionTopicList = globalBroadcastRetentionTopicList;
    }
}
