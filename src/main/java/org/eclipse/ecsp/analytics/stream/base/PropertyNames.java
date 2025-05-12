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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.eclipse.ecsp.analytics.stream.base.exception.PropertyNotFoundException;

import java.util.HashMap;
import java.util.Map;

/**
 * PropertyNames: Constant File. A list of all the properties exposed by stream-base library.
 */
public class PropertyNames {

    /**
     * Instantiates a new property names.
     */
    private PropertyNames() {
        // empty private constructor for utility class
    }
    
    /** The Constant DMA_CONFIG_RESOLVER_CLASS. */
    public static final String DMA_CONFIG_RESOLVER_CLASS = "dma.config.resolver.class";
    
    /** The Constant ADMIN. */
    private static final String ADMIN = "admin";
    
    /** The Constant SOURCE_TOPIC_NAME2. */
    // source.topic.name2 is from CFMS
    public static final String SOURCE_TOPIC_NAME2 = "source.topic.name2";
    
    /** The Constant SOURCE_TOPIC_NAME. */
    public static final String SOURCE_TOPIC_NAME = "source.topic.name";
    
    /** The Constant DMA_NOTIFICATION_TOPIC_NAME. */
    public static final String DMA_NOTIFICATION_TOPIC_NAME = "dma.notification.topic.name";
    
    /** The Constant DISCOVERY_SERVICE_IMPL. */
    public static final String DISCOVERY_SERVICE_IMPL = "discovery.impl.class.fqn";
    
    /** The Constant LAUNCHER_IMPL. */
    public static final String LAUNCHER_IMPL = "launcher.impl.class.fqn";
    
    /** The Constant EVENT_TRANSFORMER_CLASSES. */
    public static final String EVENT_TRANSFORMER_CLASSES = "event.transformer.classes";
    
    /** The Constant IGNITE_KEY_TRANSFORMER. */
    public static final String IGNITE_KEY_TRANSFORMER = "ignite.key.transformer.class";
    
    /** The Constant DEVICE_MESSAGING_EVENT_TRANSFORMER. */
    public static final String DEVICE_MESSAGING_EVENT_TRANSFORMER = "device.messaging.event.transformer.class";
    
    /** The Constant DEVICE_MESSAGE_FEEDBACK_TOPIC. */
    public static final String DEVICE_MESSAGE_FEEDBACK_TOPIC = "device.message.feedback.topic";
    
    /** The Constant INGESTION_SERIALIZER_CLASS. */
    public static final String INGESTION_SERIALIZER_CLASS = "ingestion.serializer.class";
    
    /** The Constant SHUTDOWN_HOOK_WAIT_MS. */
    public static final String SHUTDOWN_HOOK_WAIT_MS = "shutdown.hook.wait.ms";
    
    /** The Constant APPLICATION_ID. */
    public static final String APPLICATION_ID = StreamsConfig.APPLICATION_ID_CONFIG;
    
    /** The Constant NUM_STREAM_THREADS. */
    public static final String NUM_STREAM_THREADS = StreamsConfig.NUM_STREAM_THREADS_CONFIG;
    
    /** The Constant CLIENT_ID. */
    public static final String CLIENT_ID = ProducerConfig.CLIENT_ID_CONFIG;
    
    /** The Constant ZOOKEEPER_CONNECT. */
    // RTC-141484 - Kafka version upgrade from 1.0.0. to 2.2.0 changes
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    
    /** The Constant BOOTSTRAP_SERVERS. */
    public static final String BOOTSTRAP_SERVERS = StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
    
    /** The Constant REPLICATION_FACTOR. */
    public static final String REPLICATION_FACTOR = StreamsConfig.REPLICATION_FACTOR_CONFIG;
    
    /** The Constant AUTO_OFFSET_RESET_CONFIG. */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
    
    /** The Constant APPLICATION_OFFSET_RESET. */
    public static final String APPLICATION_OFFSET_RESET = "application.offset.reset";
    
    /** The Constant APPLICATION_RESET_TOPICS. */
    public static final String APPLICATION_RESET_TOPICS = "application.reset.topics";
    
    /** The Constant TENANT. */
    public static final String TENANT = "tenant";
    
    /** The Constant ENV. */
    public static final String ENV = "env";
    
    /** The Constant STATE_STORE_TYPE. */
    public static final String STATE_STORE_TYPE = "state.store.type";
    
    /** The Constant SHARED_DATA_SOURCE_IMPL. */
    public static final String SHARED_DATA_SOURCE_IMPL = "shared.data.source.impl.class";
    
    /** The Constant SHARED_DATA_SOURCE_URL. */
    public static final String SHARED_DATA_SOURCE_URL = "shared.data.source.url";
    
    /** The Constant SHARED_DATA_SOURCE_NAME. */
    public static final String SHARED_DATA_SOURCE_NAME = "shared.data.source.name";
    
    /** The Constant SHARED_DATA_SOURCE_CONNECTION_PER_HOST. */
    public static final String SHARED_DATA_SOURCE_CONNECTION_PER_HOST = "shared.data.source.connection.per.host";
    
    /** The Constant SHARED_DATA_SOURCE_CONNECTION_TIMEOUT. */
    public static final String SHARED_DATA_SOURCE_CONNECTION_TIMEOUT = "shared.data.source.connection.timeout";
    
    /** The Constant SHARED_DATA_SOURCE_CONNECTION_SOCKET_TIMEOUT. */
    public static final String SHARED_DATA_SOURCE_CONNECTION_SOCKET_TIMEOUT =
            "shared.data.source.connection.socket.timeout";
    
    /** The Constant SHARED_TOPICS. */
    public static final String SHARED_TOPICS = "shared.topics";

    /** The Constant MONGODB_URL. */
    // DB Connection constant --> start (FROM CFMS)
    public static final String MONGODB_URL = "db.url";
    
    /** The Constant MONGODB_PORT. */
    public static final String MONGODB_PORT = "db.port";
    
    /** The Constant MONGODB_AUTH_USERNAME. */
    public static final String MONGODB_AUTH_USERNAME = "db.auth.username";
    
    /** The Constant MONGODB_AUTH_PSWD. */
    public static final String MONGODB_AUTH_PSWD = "db.auth.password";
    
    /** The Constant MONGODB_AUTH_DB. */
    public static final String MONGODB_AUTH_DB = "db.auth.db";
    
    /** The Constant MONGODB_DBNAME. */
    public static final String MONGODB_DBNAME = "db.name";
    
    /** The Constant MONGODB_POOL_MAX_SIZE. */
    public static final String MONGODB_POOL_MAX_SIZE = "db.pool.max.size";
    
    /** The Constant MONGO_CLIENT_MAX_WAIT_TIME_MS. */
    public static final String MONGO_CLIENT_MAX_WAIT_TIME_MS = "db.client.max.wait.time.ms";
    
    /** The Constant MONGO_CLIENT_CONNECTION_TIMEOUT_MS. */
    public static final String MONGO_CLIENT_CONNECTION_TIMEOUT_MS = "db.client.connection.timeout.ms";
    
    /** The Constant MONGO_CLIENT_SOCKET_TIMEOUT_MS. */
    public static final String MONGO_CLIENT_SOCKET_TIMEOUT_MS = "db.client.socket,timeout.ms";
    
    /** The Constant MONGO_MAX_CONNECTIONS. */
    public static final String MONGO_MAX_CONNECTIONS = "db.client.max.connections";
    
    /** The Constant DEFAULT_MONGODB_URL. */
    private static final String DEFAULT_MONGODB_URL = "localhost";
    
    /** The Constant DEFAULT_MONGODB_PORT. */
    private static final String DEFAULT_MONGODB_PORT = "12345";
    
    /** The Constant DEFAULT_MONGODB_AUTH_USERNAME. */
    private static final String DEFAULT_MONGODB_AUTH_USERNAME = ADMIN;
    
    /** The Constant DEFAULT_MONGODB_AUTH_PSWD. */
    private static final String DEFAULT_MONGODB_AUTH_PSWD = "password";
    
    /** The Constant DEFAULT_MONGODB_AUTH_DB. */
    private static final String DEFAULT_MONGODB_AUTH_DB = ADMIN;
    
    /** The Constant DEFAULT_MONGODB_DBNAME. */
    private static final String DEFAULT_MONGODB_DBNAME = ADMIN;
    
    /** The Constant DEFAULT_MAX_WAIT_TIME_MS. */
    private static final String DEFAULT_MAX_WAIT_TIME_MS = "30000";
    
    /** The Constant DEFAULT_CONNECT_TIMEOUT_MS. */
    private static final String DEFAULT_CONNECT_TIMEOUT_MS = "20000";
    
    /** The Constant DEFAULT_SOCKET_TIMEOUT_MS. */
    private static final String DEFAULT_SOCKET_TIMEOUT_MS = "60000";
    
    /** The Constant DEFAULT_MAX_CONNECTION. */
    private static final String DEFAULT_MAX_CONNECTION = "50";
    
    /** The Constant KAFKA_PARTITIONER. */
    // DB Connection constant --> end
    public static final String KAFKA_PARTITIONER = "kafka.partitioner";
    
    /** The Constant KAFKA_REPLACE_CLASSLOADER. */
    public static final String KAFKA_REPLACE_CLASSLOADER = "kafka.replace.classloader";
    
    /** The Constant KAFKA_DEVICE_EVENTS_ASYNC_PUTS. */
    public static final String KAFKA_DEVICE_EVENTS_ASYNC_PUTS = "kafka.device.events.sync.puts";

    /** The Constant REDIS_MODE. */
    public static final String REDIS_MODE = "redis.mode";
    
    /** The Constant REDIS_SINGLE_ENDPOINT. */
    public static final String REDIS_SINGLE_ENDPOINT = "redis.single.endpoint";
    
    /** The Constant REDIS_REPLICA_ENDPOINTS. */
    public static final String REDIS_REPLICA_ENDPOINTS = "redis.replica.endpoints";
    
    /** The Constant REDIS_CLUSTER_ENDPOINTS. */
    public static final String REDIS_CLUSTER_ENDPOINTS = "redis.cluster.endpoints";
    
    /** The Constant REDIS_SENTINEL_ENDPOINTS. */
    public static final String REDIS_SENTINEL_ENDPOINTS = "redis.sentinel.endpoints";
    
    /** The Constant REDIS_MASTER_NAME. */
    public static final String REDIS_MASTER_NAME = "redis.master.name";
    
    /** The Constant REDIS_MASTER_POOL_MAX. */
    public static final String REDIS_MASTER_POOL_MAX = "redis.master.pool.max.size";
    
    /** The Constant REDIS_MASTER_IDLE_MIN. */
    public static final String REDIS_MASTER_IDLE_MIN = "redis.master.idle.min";
    
    /** The Constant REDIS_SLAVE_POOL_MAX. */
    public static final String REDIS_SLAVE_POOL_MAX = "redis.slave.pool.max.size";
    
    /** The Constant REDIS_SLAVE_IDLE_MIN. */
    public static final String REDIS_SLAVE_IDLE_MIN = "redis.slave.idle.min";
    
    /** The Constant REDIS_SCAN_INTERVAL. */
    public static final String REDIS_SCAN_INTERVAL = "redis.scan.interval";
    
    /** The Constant REDIS_BATCH_SIZE. */
    public static final String REDIS_BATCH_SIZE = "redis.batch.size";
    
    /** The Constant REDIS_DATABASE. */
    public static final String REDIS_DATABASE = "redis.database";
    
    /** The Constant REDIS_MAX_POOL. */
    public static final String REDIS_MAX_POOL = "redis.max.pool.size";
    
    /** The Constant REDIS_MAX_IDLE. */
    public static final String REDIS_MAX_IDLE = "redis.max.idle";
    
    /** The Constant REDIS_MIN_IDLE. */
    public static final String REDIS_MIN_IDLE = "redis.min.idle";
    
    /** The Constant REDIS_READ_TIMEOUT. */
    public static final String REDIS_READ_TIMEOUT = "redis.read.timeout";
    
    /** The Constant REDIS_RETRY_INTERVAL. */
    public static final String REDIS_RETRY_INTERVAL = "redis.retry.interval";
    
    /** The Constant REDIS_RETRY_ATTEMPTS. */
    public static final String REDIS_RETRY_ATTEMPTS = "redis.retry.attempts";
    
    /** The Constant REDIS_DATA_SERIALIZE. */
    public static final String REDIS_DATA_SERIALIZE = "redis.data.serialize";
    
    /** The Constant REDIS_READ_MODE. */
    public static final String REDIS_READ_MODE = "redis.read.mode";
    
    /** The Constant KAFKA_REBALANCE_TIME_MINS. */
    public static final String KAFKA_REBALANCE_TIME_MINS = "kafka.rebalance.time.mins";
    
    /** The Constant KAFKA_CLOSE_TIMEOUT_SECS. */
    public static final String KAFKA_CLOSE_TIMEOUT_SECS = "kafka.close.timeout.secs";

    /** The Constant KAFKA_SSL_ENABLE. */
    public static final String KAFKA_SSL_ENABLE = "kafka.ssl.enable";
    
    /** The Constant KAFKA_ONE_WAY_TLS_ENABLE. */
    public static final String KAFKA_ONE_WAY_TLS_ENABLE = "kafka.one.way.tls.enable";
    
    /** The Constant KAFKA_SASL_MECHANISM. */
    public static final String KAFKA_SASL_MECHANISM = "kafka.sasl.mechanism";
    
    /** The Constant KAFKA_SASL_JAAS_CONFIG. */
    public static final String KAFKA_SASL_JAAS_CONFIG = "kafka.sasl.jaas.config";
    
    /** The Constant KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM. */
    public static final String KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = 
            "kafka.ssl.endpoint.identification.algorithm";
    
    /** The Constant KAFKA_CLIENT_KEYSTORE. */
    public static final String KAFKA_CLIENT_KEYSTORE = "kafka.client.keystore";
    
    /** The Constant KAFKA_CLIENT_KEYSTORE_PASSWORD. */
    public static final String KAFKA_CLIENT_KEYSTORE_PASSWORD = "kafka.client.keystore.password";
    
    /** The Constant KAFKA_CLIENT_KEY_PASSWORD. */
    public static final String KAFKA_CLIENT_KEY_PASSWORD = "kafka.client.key.password";
    
    /** The Constant KAFKA_CLIENT_TRUSTSTORE. */
    public static final String KAFKA_CLIENT_TRUSTSTORE = "kafka.client.truststore";
    
    /** The Constant KAFKA_CLIENT_TRUSTSTORE_PASSWORD. */
    public static final String KAFKA_CLIENT_TRUSTSTORE_PASSWORD = "kafka.client.truststore.password";
    
    /** The Constant KAFKA_SSL_CLIENT_AUTH. */
    public static final String KAFKA_SSL_CLIENT_AUTH = "kafka.ssl.client.auth";
    
    /** The Constant KAFKA_MAX_REQUEST_SIZE. */
    public static final String KAFKA_MAX_REQUEST_SIZE = "kafka.max.request.size";
    
    /** The Constant KAFKA_ACKS_CONFIG. */
    public static final String KAFKA_ACKS_CONFIG = "kafka.acks.config";
    
    /** The Constant KAFKA_RETRIES_CONFIG. */
    public static final String KAFKA_RETRIES_CONFIG = "kafka.retries.config";
    
    /** The Constant KAFKA_BATCH_SIZE_CONFIG. */
    public static final String KAFKA_BATCH_SIZE_CONFIG = "kafka.batch.size.config";
    
    /** The Constant KAFKA_LINGER_MS_CONFIG. */
    public static final String KAFKA_LINGER_MS_CONFIG = "kafka.linger.ms.config";
    
    /** The Constant KAFKA_BUFFER_MEMORY_CONFIG. */
    public static final String KAFKA_BUFFER_MEMORY_CONFIG = "kafka.buffer.memory.config";
    
    /** The Constant KAFKA_REQUEST_TIMEOUT_MS_CONFIG. */
    public static final String KAFKA_REQUEST_TIMEOUT_MS_CONFIG = "kafka.request.timeout.ms.config";
    
    /** The Constant KAFKA_DELIVERY_TIMEOUT_MS_CONFIG. */
    public static final String KAFKA_DELIVERY_TIMEOUT_MS_CONFIG = "kafka.delivery.timeout.ms.config";
    
    /** The Constant KAFKA_COMPRESSION_TYPE_CONFIG. */
    public static final String KAFKA_COMPRESSION_TYPE_CONFIG = "kafka.compression.type.config";

    /** The Constant KAFKA_CONSUMER_TOPIC. */
    public static final String KAFKA_CONSUMER_TOPIC = "kafka.consumer.topic";
    
    /** The Constant KAFKA_CONSUMER_POLL. */
    public static final String KAFKA_CONSUMER_POLL = "kafka.consumer.poll";

    /** The Constant LOG_COUNTS. */
    public static final String LOG_COUNTS = "log.counts";
    
    /** The Constant LOG_COUNTS_MINUTES. */
    public static final String LOG_COUNTS_MINUTES = "log.counts.minutes";
    
    /** The Constant LOG_PER_PDID. */
    public static final String LOG_PER_PDID = "log.per.pdid";

    /** The Constant KINESIS_ACCESS_KEY. */
    public static final String KINESIS_ACCESS_KEY = "kinesis.accessKey";
    
    /** The Constant KINESIS_SECRET_KEY. */
    public static final String KINESIS_SECRET_KEY = "kinesis.secretAccessKey";
    
    /** The Constant KINESIS_RETRY_ATTEMPTS. */
    public static final String KINESIS_RETRY_ATTEMPTS = "kinesis.retry.attempts";
    
    /** The Constant KINESIS_REGION. */
    public static final String KINESIS_REGION = "kinesis.region";

    /** The Constant KCL_ACCESS_KEY. */
    public static final String KCL_ACCESS_KEY = "kcl.access.key";
    
    /** The Constant KCL_SECRET_KEY. */
    public static final String KCL_SECRET_KEY = "kcl.secret.key";
    
    /** The Constant KCL_WORKER_NUMBER_OF_THREADS. */
    public static final String KCL_WORKER_NUMBER_OF_THREADS = "kcl.worker.number.of.threads";
    
    /** The Constant KCL_WORKER_KEEP_ALIVE_TIME. */
    public static final String KCL_WORKER_KEEP_ALIVE_TIME = "kcl.worker.keep.alive.time";
    
    /** The Constant KCL_STREAM_POSITION. */
    public static final String KCL_STREAM_POSITION = "kcl.stream.position";
    
    /** The Constant KCL_BACKOFF_TIME_MILLIS. */
    public static final String KCL_BACKOFF_TIME_MILLIS = "kcl.backoff.time.in.millis";
    
    /** The Constant KCL_NUM_RETRIES. */
    public static final String KCL_NUM_RETRIES = "kcl.num.retries";

    /** The Constant PRE_PROCESSORS. */
    public static final String PRE_PROCESSORS = "pre.processors";
    
    /** The Constant SERVICE_STREAM_PROCESSORS. */
    public static final String SERVICE_STREAM_PROCESSORS = "service.stream.processors";
    
    /** The Constant SERVICE_NAME. */
    public static final String SERVICE_NAME = "service.name";
    
    /** The Constant POST_PROCESSORS. */
    public static final String POST_PROCESSORS = "post.processors";
    
    /** The Constant APPLICATION_PROPERTIES. */
    public static final String APPLICATION_PROPERTIES = "/application.properties";
    
    /** The Constant SEQUENCE_BLOCK_MAXVALUE. */
    public static final String SEQUENCE_BLOCK_MAXVALUE = "sequence.block.config.maxvalue";
    
    /** The Constant IGNITE_PLATFORM_SERVICE_IMPL_CLASS_NAME. */
    public static final String IGNITE_PLATFORM_SERVICE_IMPL_CLASS_NAME = "ignite.platform.service.impl.class.name";
    
    /** The Constant VEHICLE_PROFILE_VIN_URL. */
    public static final String VEHICLE_PROFILE_VIN_URL = "http.vp.vin.url";
    
    /** The Constant VEHICLE_PROFILE_PLATFORM_IDS. */
    public static final String VEHICLE_PROFILE_PLATFORM_IDS = "http.vp.platform.ids";
    
    /** The Constant MQTT_TOPIC_GENERATOR_SERVICE_IMPL_CLASS_NAME. */
    public static final String MQTT_TOPIC_GENERATOR_SERVICE_IMPL_CLASS_NAME = 
            "mqtt.topic.name.generator.impl.class.name";
    
    /** The Constant DEFAULT_TOPIC_NAME_GENERATOR_IMPL. */
    public static final String DEFAULT_TOPIC_NAME_GENERATOR_IMPL = 
            "org.eclipse.ecsp.analytics.stream.base.utils.DefaultMqttTopicNameGeneratorImpl";
    /*
     * MQTT propreties required for DeviceMessagingAgent stream processor
     */

    // MQTT_SHORT_CIRCUIT property used to identify whether we would like to
    // directly send the event to MQTT or Redis if true, DevieMessaging Agent
    // stream processor will directly push the data to mqtt.

    /** The Constant EVENT_WRAP_FREQUENCY. */
    public static final String EVENT_WRAP_FREQUENCY = "event.wrap.frequency";
    
    /** The Constant MQTT_SHORT_CIRCUIT. */
    public static final String MQTT_SHORT_CIRCUIT = "mqtt.short.circuit";
    
    /** The Constant MQTT_BROKER_URL. */
    public static final String MQTT_BROKER_URL = "mqtt.broker.url";
    
    /** The Constant MQTT_BROKER_PORT. */
    public static final String MQTT_BROKER_PORT = "mqtt.broker.port";
    
    /** The Constant MQTT_TOPIC_NAME. */
    public static final String MQTT_TOPIC_NAME = "mqtt.topic.name";
    
    /** The Constant MQTT_TOPIC_SEPARATOR. */
    public static final String MQTT_TOPIC_SEPARATOR = "mqtt.topic.separator";
    
    /** The Constant MQTT_CONFIG_QOS. */
    public static final String MQTT_CONFIG_QOS = "mqtt.config.qos";
    
    /** The Constant MQTT_USER_NAME. */
    public static final String MQTT_USER_NAME = "mqtt.user.name";
    
    /** The Constant MQTT_CLIENT_AUTH_MECHANISM. */
    public static final String MQTT_CLIENT_AUTH_MECHANISM = "mqtt.client.auth.mechanism";
    
    /** The Constant MQTT_SERVICE_TRUSTSTORE_PATH. */
    public static final String MQTT_SERVICE_TRUSTSTORE_PATH = "mqtt.service.truststore.path";
    
    /** The Constant MQTT_SERVICE_TRUSTSTORE_PASSWORD. */
    public static final String MQTT_SERVICE_TRUSTSTORE_PASSWORD = "mqtt.service.truststore.password";
    
    /** The Constant MQTT_SERVICE_TRUSTSTORE_TYPE. */
    public static final String MQTT_SERVICE_TRUSTSTORE_TYPE = "mqtt.service.truststore.type";
    
    /** The Constant CONNECTIONS_MAX_IDLE_MS. */
    public static final String CONNECTIONS_MAX_IDLE_MS = "connections.max.idle.ms";

    /** The Constant MQTT_USER_PASSWORD. */
    public static final String MQTT_USER_PASSWORD = "mqtt.user.password";
    
    /** The Constant MQTT_MAX_INFLIGHT. */
    public static final String MQTT_MAX_INFLIGHT = "mqtt.max.inflight";
    
    /** The Constant MQTT_SERVICE_TOPIC_NAME. */
    public static final String MQTT_SERVICE_TOPIC_NAME = "mqtt.service.topic.name";
    
    /** The Constant MQTT_GLOBAL_BROADCAST_RETENTION_TOPICS. */
    public static final String MQTT_GLOBAL_BROADCAST_RETENTION_TOPICS = "mqtt.global.broadcast.retention.topics";
    
    /** The Constant MQTT_SERVICE_TOPIC_NAME_PREFIX. */
    public static final String MQTT_SERVICE_TOPIC_NAME_PREFIX = "mqtt.service.topic.name.prefix";
    
    /** The Constant MQTT_TOPIC_TO_DEVICE_INFIX. */
    public static final String MQTT_TOPIC_TO_DEVICE_INFIX = "mqtt.topic.to.device.infix";
    
    /** The Constant MQTT_CONNECTION_RETRY_COUNT. */
    public static final String MQTT_CONNECTION_RETRY_COUNT = "mqtt.conn.retry.count";
    
    /** The Constant MQTT_CONNECTION_RETRY_INTERVAL. */
    public static final String MQTT_CONNECTION_RETRY_INTERVAL = "mqtt.conn.retry.interval";
    
    /** The Constant MQTT_TIMEOUT_IN_MILLIS. */
    public static final String MQTT_TIMEOUT_IN_MILLIS = "mqtt.timeout.in.millis";
    
    /** The Constant MQTT_KEEP_ALIVE_INTERVAL. */
    public static final String MQTT_KEEP_ALIVE_INTERVAL = "mqtt.keep.alive.in.seconds";
    
    /** The Constant MQTT_CLIENT. */
    public static final String MQTT_CLIENT = "mqtt.client";
    
    /** The Constant MQTT_CLEAN_SESSION. */
    public static final String MQTT_CLEAN_SESSION = "mqtt.clean.session";
    
    /** The Constant WRAP_DISPATCH_EVENT. */
    public static final String WRAP_DISPATCH_EVENT = "wrap.dispatch.event";
    
    /** The Constant SHORT_HASHCODE_INDEX. */
    public static final String SHORT_HASHCODE_INDEX = "short_hashcode_index";
    
    /** The Constant HEALTH_MQTT_MONITOR_ENABLED. */
    public static final String HEALTH_MQTT_MONITOR_ENABLED = "health.mqtt.monitor.enabled";
    
    /** The Constant HEALTH_MQTT_MONITOR_RESTART_ON_FAILURE. */
    public static final String HEALTH_MQTT_MONITOR_RESTART_ON_FAILURE = "health.mqtt.monitor.restart.on.failure";
    
    /** The Constant MQTT_BROKER_PLATFORMID_MAPPING. */
    public static final String MQTT_BROKER_PLATFORMID_MAPPING = "mqtt.broker.platformId.mapping";
    
    /** The Constant DEFAULT_PLATFORMID. */
    public static final String DEFAULT_PLATFORMID = "defaultPlatformId";
    
    /** The default property values. */
    private static Map<String, String> defaultPropertyValues = new HashMap<>();

    static {
        defaultPropertyValues.put(MONGODB_URL, DEFAULT_MONGODB_URL);
        defaultPropertyValues.put(MONGODB_PORT, DEFAULT_MONGODB_PORT);
        defaultPropertyValues.put(MONGODB_AUTH_USERNAME, DEFAULT_MONGODB_AUTH_USERNAME);
        defaultPropertyValues.put(MONGODB_AUTH_PSWD, DEFAULT_MONGODB_AUTH_PSWD);
        defaultPropertyValues.put(MONGODB_AUTH_DB, DEFAULT_MONGODB_AUTH_DB);
        defaultPropertyValues.put(MONGODB_DBNAME, DEFAULT_MONGODB_DBNAME);
        defaultPropertyValues.put(MONGO_CLIENT_MAX_WAIT_TIME_MS, DEFAULT_MAX_WAIT_TIME_MS);
        defaultPropertyValues.put(MONGO_CLIENT_CONNECTION_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT_MS);
        defaultPropertyValues.put(MONGO_CLIENT_SOCKET_TIMEOUT_MS, DEFAULT_SOCKET_TIMEOUT_MS);
        defaultPropertyValues.put(MONGO_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTION);

    }

    /**
     * Helper method to retrieve the default values of the property.
     *
     * @param propertyName propertyName
     * @return String
     */
    public static String getDefaultPropertyValue(String propertyName) {
        if (defaultPropertyValues.containsKey(propertyName)) {
            return defaultPropertyValues.get(propertyName);
        }
        throw new PropertyNotFoundException("Property " + propertyName + " doesn't exist in the default list.");
    }

    /** The Constant DMA_KAFKA_CONSUMER_POLL. */
    public static final String DMA_KAFKA_CONSUMER_POLL = "dma.kafka.consumer.poll";
    
    /** The Constant BACKDOOR_KAFKA_CONSUMER_DEFAULT_API_TIMEOUT_MS. */
    //Below property specifies the timeout for committing the offsets.
    public static final String BACKDOOR_KAFKA_CONSUMER_DEFAULT_API_TIMEOUT_MS =
            "backdoor.kafka.consumer.default.api.timeout.ms";
    
    /** The Constant DMA_AUTO_OFFSET_RESET_CONFIG. */
    public static final String DMA_AUTO_OFFSET_RESET_CONFIG =
            "dma.auto.offset.reset";
    
    /** The Constant DMA_EVENT_HEADER_UPDATION_TYPE. */
    public static final String DMA_EVENT_HEADER_UPDATION_TYPE =
            "dma.event.header.updation.type";

    /** The Constant DMA_SERVICE_MAX_RETRY. */
    public static final String DMA_SERVICE_MAX_RETRY =
            "dma.service.max.retry";
    
    /** The Constant DMA_SERVICE_RETRY_INTERVAL_MILLIS. */
    public static final String DMA_SERVICE_RETRY_INTERVAL_MILLIS =
            "dma.service.retry.interval.millis";
    
    /** The Constant DMA_SERVICE_RETRY_MIN_THRESHOLD_MILLIS. */
    public static final String DMA_SERVICE_RETRY_MIN_THRESHOLD_MILLIS =
            "dma.service.retry.min.threshold.millis";
    
    /** The Constant DMA_SERVICE_RETRY_INTERVAL_DIVISOR. */
    public static final String DMA_SERVICE_RETRY_INTERVAL_DIVISOR =
            "dma.service.retry.interval.divisor";
    
    /** The Constant DMA_SHOULDER_TAP_INVOKER_IMPL_CLASS. */
    public static final String DMA_SHOULDER_TAP_INVOKER_IMPL_CLASS =
            "dma.shoulder.tap.invoker.impl.class";
    
    /** The Constant DMA_SHOULDER_TAP_INVOKER_WAM_SEND_SMS_URL. */
    public static final String DMA_SHOULDER_TAP_INVOKER_WAM_SEND_SMS_URL =
            "dma.shoulder.tap.invoker.wam.send.sms.url";
    
    /** The Constant DMA_SHOULDER_TAP_INVOKER_WAM_SMS_TRANSACTION_STATUS_URL. */
    public static final String DMA_SHOULDER_TAP_INVOKER_WAM_SMS_TRANSACTION_STATUS_URL =
            "dma.shoulder.tap.invoker.wam.sms.transaction.status.url";
    
    /** The Constant DMA_SHOULDER_TAP_WAM_SMS_PRIORITY. */
    public static final String DMA_SHOULDER_TAP_WAM_SMS_PRIORITY = "dma.shoulder.tap.wam.sms.priority";
    
    /** The Constant DMA_SHOULDER_TAP_WAM_SMS_VALIDITY_HOURS. */
    public static final String DMA_SHOULDER_TAP_WAM_SMS_VALIDITY_HOURS =
            "dma.shoulder.tap.wam.sms.validity.hours";
    
    /** The Constant DMA_SHOULDER_TAP_WAM_SEND_SMS_SKIP_STATUS_CHECK. */
    public static final String DMA_SHOULDER_TAP_WAM_SEND_SMS_SKIP_STATUS_CHECK =
            "dma.shoulder.tap.wam.send.sms.skip.status.check";
    
    /** The Constant DMA_SHOULDER_TAP_WAM_API_MAX_RETRY_COUNT. */
    public static final String DMA_SHOULDER_TAP_WAM_API_MAX_RETRY_COUNT =
            "dma.shoulder.tap.wam.api.max.retry.count";
    
    /** The Constant DMA_SHOULDER_TAP_WAM_API_MAX_RETRY_INTERVAL_MS. */
    public static final String DMA_SHOULDER_TAP_WAM_API_MAX_RETRY_INTERVAL_MS =
            "dma.shoulder.tap.wam.api.max.retry.interval.ms";
    
    /** The Constant DMA_TTL_EXPIRY_NOTIFICATION_ENABLED. */
    public static final String DMA_TTL_EXPIRY_NOTIFICATION_ENABLED = "dma.ttl.expiry.notification.enabled";
    
    /** The Constant DMA_REMOVE_ON_TTL_EXPIRY_ENABLED. */
    public static final String DMA_REMOVE_ON_TTL_EXPIRY_ENABLED = "dma.remove.on.ttl.expiry.enabled";
    
    /** The Constant DMA_EVENT_CONFIG_PROVIDER_CLASS. */
    public static final String DMA_EVENT_CONFIG_PROVIDER_CLASS = "dma.event.config.provider.class";
    
    /** The Constant DMA_POST_DISPATCH_HANDLER_CLASS. */
    public static final String DMA_POST_DISPATCH_HANDLER_CLASS = "dma.post.dispatch.handler.class";
    
    /**
     * Properties required to make use of the following DMA capabilities:
     * 1. Dispatch to kafka broker. 
     * 2. Retrieve connection status of devices from a third party API
     */
    public static final String DMA_DISPATCHER_ECU_TYPES = "dma.dispatcher.ecu.types";
    
    /** The Constant DMA_CONNECTION_STATUS_RETRIEVER_API_URL. */
    public static final String DMA_CONNECTION_STATUS_RETRIEVER_API_URL =
            "dma.connection.status.retriever.api.url";
    
    /** The Constant DMA_CONNECTION_STATUS_API_MAX_RETRY_COUNT. */
    public static final String DMA_CONNECTION_STATUS_API_MAX_RETRY_COUNT =
            "dma.connection.status.api.max.retry.count";
    
    /** The Constant DMA_CONNECTION_STATUS_API_RETRY_INTERVAL_MS. */
    public static final String DMA_CONNECTION_STATUS_API_RETRY_INTERVAL_MS =
            "dma.connection.status.api.retry.interval.ms";
    
    /** The Constant DMA_CONNECTION_STATUS_PARSER_IMPL. */
    public static final String DMA_CONNECTION_STATUS_PARSER_IMPL = "dma.connection.status.parser.impl";
    
    /** The Constant DMA_CONNECTION_MSG_VALUE_TRANSFORMER. */
    public static final String DMA_CONNECTION_MSG_VALUE_TRANSFORMER =
            "dma.connection.msg.value.transformer";
    
    /** The Constant DMA_CONNECTION_MSG_KEY_TRANSFORMER. */
    public static final String DMA_CONNECTION_MSG_KEY_TRANSFORMER = "dma.connection.msg.key.transformer";
    
    /** The Constant OFFLINE_BUFFER_PER_DEVICE. */
    public static final String OFFLINE_BUFFER_PER_DEVICE = "offline.buffer.per.device";
    
    /** The Constant SHOULDER_TAP_MAX_RETRY. */
    public static final String SHOULDER_TAP_MAX_RETRY = "shoulder.tap.max.retry";
    
    /** The Constant SHOULDER_TAP_RETRY_INTERVAL_MILLIS. */
    public static final String SHOULDER_TAP_RETRY_INTERVAL_MILLIS = "shoulder.tap.retry.interval.millis";
    
    /** The Constant SHOULDER_TAP_RETRY_MIN_THRESHOLD_MILLIS. */
    public static final String SHOULDER_TAP_RETRY_MIN_THRESHOLD_MILLIS = "shoulder.tap.retry.min.threshold.millis";
    
    /** The Constant SHOULDER_TAP_RETRY_INTERVAL_DIVISOR. */
    public static final String SHOULDER_TAP_RETRY_INTERVAL_DIVISOR = "shoulder.tap.retry.interval.divisor";
    
    /** The Constant FILTER_DM_OFFLINE_BUFFER_ENTRIES_IMPL. */
    public static final String FILTER_DM_OFFLINE_BUFFER_ENTRIES_IMPL = "filter.dmoffline.buffer.entry.impl";
    
    /** The Constant DMA_NUM_CACHE_BYPASS_THREADS. */
    public static final String DMA_NUM_CACHE_BYPASS_THREADS = "dma.num.cache.bypass.threads";
    
    /** The Constant CACHE_BYPASS_THREADS_SHUTDOWN_WAIT_TIME. */
    public static final String CACHE_BYPASS_THREADS_SHUTDOWN_WAIT_TIME = "cache.bypass.threads.shutdown.wait.time";
    
    /** The Constant CACHE_BYPASS_QUEUE_INITIAL_CAPACITY. */
    public static final String CACHE_BYPASS_QUEUE_INITIAL_CAPACITY = "cache.bypass.queue.initial.capacity";
    
    /** The Constant DMA_CONNECTION_STATUS_RETRIEVER_IMPL. */
    public static final String DMA_CONNECTION_STATUS_RETRIEVER_IMPL = "dma.connection.status.retriever.impl";
    
    /** The Constant DEFAULT_CONNECTION_STATUS_RETRIEVER_IMPL. */
    public static final String DEFAULT_CONNECTION_STATUS_RETRIEVER_IMPL = 
              "org.eclipse.ecsp.analytics.stream.base.utils.DefaultDeviceConnectionStatusRetriever";

    /** The Constant KAFKA_STREAMS_MAX_FAILURES. */
    //RTC 334625 Configuration for maxFailures and maxTimeInterval to be used to recover the thread
    public static final String KAFKA_STREAMS_MAX_FAILURES = "kafka.streams.max.failures";
    
    /** The Constant KAFKA_STREAMS_MAX_TIME_INTERVAL. */
    public static final String KAFKA_STREAMS_MAX_TIME_INTERVAL = "kafka.streams.max.time.millis";

    /** The Constant HTTP_CONNECTION_TIMEOUT_IN_SEC. */
    // Http client properties
    public static final String HTTP_CONNECTION_TIMEOUT_IN_SEC = "http.connection.timeout.in.sec";
    
    /** The Constant HTTP_READ_TIMEOUT_IN_SEC. */
    public static final String HTTP_READ_TIMEOUT_IN_SEC = "http.read.timeout.in.sec";
    
    /** The Constant HTTP_WRITE_TIMEOUT_IN_SEC. */
    public static final String HTTP_WRITE_TIMEOUT_IN_SEC = "http.write.timeout.in.sec";
    
    /** The Constant HTTP_KEEP_ALIVE_DURATION_IN_SEC. */
    public static final String HTTP_KEEP_ALIVE_DURATION_IN_SEC = "http.keep.alive.duration.in.sec";
    
    /** The Constant HTTP_MAX_IDLE_CONNECTIONS. */
    public static final String HTTP_MAX_IDLE_CONNECTIONS = "http.max.idle.connections";
    
    /** The Constant HTTP_VP_SERVICE_AUTH_HEADER. */
    public static final String HTTP_VP_SERVICE_AUTH_HEADER = "http.vp.auth.header";
    
    /** The Constant HTTP_VP_SERVICE_USER. */
    public static final String HTTP_VP_SERVICE_USER = "http.vp.service.user";
    
    /** The Constant HTTP_VP_SERVICE_PASSWORD. */
    public static final String HTTP_VP_SERVICE_PASSWORD = "http.vp.service.password";

    /** The Constant D2V_MAPPER_IMPL. */
    // Device to Vehicle profile implementation class
    public static final String D2V_MAPPER_IMPL = "device.to.vehicle.mapper.impl";
    
    /** The Constant VEHICLE_PROFILE_URL. */
    // Vehicle profile service URL
    public static final String VEHICLE_PROFILE_URL = "http.vp.url";
    
    /** The Constant VP_RES_ECUS_NODE. */
    public static final String VP_RES_ECUS_NODE = "ecus";
    
    /** The Constant VP_RES_SERVICES_NODE. */
    public static final String VP_RES_SERVICES_NODE = "services";
    
    /** The Constant VP_RES_CLIENT_ID_FIELD. */
    public static final String VP_RES_CLIENT_ID_FIELD = "clientId";
    
    /** The Constant VP_RETRY_INTERVAL_IN_MILLIS. */
    public static final String VP_RETRY_INTERVAL_IN_MILLIS = "http.vp.retry.interval.in.millis";
    
    /** The Constant VP_RETRY_MAX_COUNT. */
    public static final String VP_RETRY_MAX_COUNT = "http.vp.max.retry.count";

    /** The Constant SCHEDULER_AGENT_TOPIC_NAME. */
    public static final String SCHEDULER_AGENT_TOPIC_NAME = "scheduler.agent.topic.name";
    
    /** The Constant START_DEVICE_STATUS_CONSUMER. */
    public static final String START_DEVICE_STATUS_CONSUMER = "start.device.status.consumer";
    
    /** The Constant FETCH_CONNECTION_STATUS_TOPIC_NAME. */
    public static final String FETCH_CONNECTION_STATUS_TOPIC_NAME = "fetch.connection.status.topic.name";

    /** The Constant CONVERT_BACKDOOR_KAFKA_TOPIC_TO_LOWERCASE. */
    public static final String CONVERT_BACKDOOR_KAFKA_TOPIC_TO_LOWERCASE = "convert.backdoor.kafka.topic.tolowercase";

    /** The Constant ENABLE_PROMETHEUS. */
    public static final String ENABLE_PROMETHEUS = "metrics.prometheus.enabled";
    
    /** The Constant NODE_NAME. */
    public static final String NODE_NAME = "NODE_NAME";
    
    /** The Constant PROMETHEUS_AGENT_PORT_KEY. */
    public static final String PROMETHEUS_AGENT_PORT_KEY = "prometheus.agent.port";

    /** The Constant BACKDOOR_KAFKA_MAX_POLL_INTERVAL_MS. */
    public static final String BACKDOOR_KAFKA_MAX_POLL_INTERVAL_MS = "backdoor.kafka.max.poll.interval.ms";
    
    /** The Constant BACKDOOR_KAFKA_REQUEST_TIMEOUT_MS. */
    public static final String BACKDOOR_KAFKA_REQUEST_TIMEOUT_MS = "backdoor.kafka.request.timeout.ms";
    
    /** The Constant BACKDOOR_KAFKA_SESSION_TIMEOUT_MS. */
    public static final String BACKDOOR_KAFKA_SESSION_TIMEOUT_MS = "backdoor.kafka.session.timeout.ms";

    /** The Constant BACKDOOR_KAFKA_MAX_RESTART_ATTEMPTS. */
    public static final String BACKDOOR_KAFKA_MAX_RESTART_ATTEMPTS =
            "backdoor.kafka.max.restart.attempts";
    
    /** The Constant BACKDOOR_KAFKA_ATTEMPTS_RESET_INTERVAL_MIN. */
    public static final String BACKDOOR_KAFKA_ATTEMPTS_RESET_INTERVAL_MIN =
            "backdoor.kafka.restart.reset.interval.min";
    
    /** The Constant BACKDOOR_KAFKA_ENABLE_AUTO_COMMIT. */
    public static final String BACKDOOR_KAFKA_ENABLE_AUTO_COMMIT =
            "backdoor.kafka.enable.auto.commit";
    
    /** The Constant BACKDOOR_KAFKA_OFFSET_PERSISTENCE_DELAY. */
    public static final String BACKDOOR_KAFKA_OFFSET_PERSISTENCE_DELAY =
            "backdoor.kafka.offset.persistence.delay";
    
    /** The Constant KAFKA_STREAMS_OFFSET_PERSISTENCE_DELAY. */
    public static final String KAFKA_STREAMS_OFFSET_PERSISTENCE_DELAY =
            "kafka.streams.offset.persistence.delay";
    
    /** The Constant KAFKA_STREAMS_OFFSET_PERSISTENCE_INIT_DELAY. */
    public static final String KAFKA_STREAMS_OFFSET_PERSISTENCE_INIT_DELAY =
            "kafka.streams.offset.persistence.init.delay";
    
    /** The Constant KAFKA_STREAMS_OFFSET_PERSISTENCE_ENABLED. */
    public static final String KAFKA_STREAMS_OFFSET_PERSISTENCE_ENABLED =
            "kafka.streams.offset.persistence.enabled";

    /** The Constant TRANSFORMER_INJECT_PROPERTY_ENABLE. */
    public static final String TRANSFORMER_INJECT_PROPERTY_ENABLE =
            "transformer.inject.property.enable";

    /**
     * Below flags are used at the time DLQ re-processing.
     */
    public static final String DLQ_MAX_RETRY_COUNT = "dlq.max.retry.count";
    
    /** The Constant DLQ_REPROCESSING_ENABLED. */
    public static final String DLQ_REPROCESSING_ENABLED = "dlq.reprocessing.enabled";

    /** The Constant HEALTH_DEVICE_STATUS_BACKDOOR_MONITOR_ENABLED. */
    public static final String HEALTH_DEVICE_STATUS_BACKDOOR_MONITOR_ENABLED =
            "health.device.status.backdoor.monitor.enabled";
    
    /** The Constant HEALTH_DEVICE_STATUS_BACKDOOR_MONITOR_RESTART_ON_FAILURE. */
    public static final String HEALTH_DEVICE_STATUS_BACKDOOR_MONITOR_RESTART_ON_FAILURE =
            "health.device.status.backdoor.monitor.restart.on.failure";
    /**
     * Below flags are used Kafka topic validator healthcheck.
     */
    public static final String KAFKA_TOPICS_NEEDS_RESTART_ON_FAILURE =
            "health.kafka.topics.monitor.needs.restart.on.failure";
    
    /** The Constant ENABLE_HEALTHCHECK. */
    public static final String ENABLE_HEALTHCHECK = "health.kafka.topics.monitor.enabled";
    
    /** The Constant KAFKA_TOPICS_FILE_PATH. */
    public static final String KAFKA_TOPICS_FILE_PATH = "kafka.topics.file.path";
    
    /** The Constant EXPECTED_MIN_ISR. */
    public static final String EXPECTED_MIN_ISR = "expected.min.isr";

    /**
     * Below flag is used in case of message filter to identify the message
     * duplicates.
     */

    public static final String MSG_FILTER_ENABLED = "message.filter.enabled";
    
    /** The Constant MSG_FILTER_TTL_MS. */
    public static final String MSG_FILTER_TTL_MS = "message.filter.ttl.ms";

    /**
     * Below flag is used to enable and disable DMA/SCHEDULER Component in StreamBase.
     */
    public static final String DMA_ENABLED = "dma.enabled";
    
    /** The Constant SCHEDULER_ENABLED. */
    public static final String SCHEDULER_ENABLED = "scheduler.enabled";

    /**
     * Below flag is used to enabling streaming of event size to kafka for analytics dashboard RTC-301848.
     */
    public static final String KAFKA_DATA_CONSUMPTION_METRICS = "kafka.data.consumption.metrics";
    
    /** The Constant KAFKA_DATA_CONSUMPTION_METRICS_KAFKA_TOPIC. */
    public static final String KAFKA_DATA_CONSUMPTION_METRICS_KAFKA_TOPIC = "data.consumption.metrics.kafka.topic";

    /**
     * CR-1758 property which will hold events that will not be saved to offline buffer in DMA.
     */
    public static final String DMA_EVENTS_SKIP_ONLINE_BUFFER = "dma.events.skip.offline.buffer";
    
    /** The Constant SUB_SERVICES. */
    /*
     * RTC 355420, if a service has multiple mqtt topics or multiple sub-service under itself, then
     * it must let DMA know about all those through below property by assigning comma separated values
     * of names of sub-services.
     */
    public static final String SUB_SERVICES = "sub.services";

    /** The Constant KAFKA_HEADERS_ENABLED. */
    public static final String KAFKA_HEADERS_ENABLED = "kafka.headers.enabled";
    
    /**
     * RDNG 171775 and RTC 503148 Expose RocksDB metrics to Prometheus.
     */
    public static final String ROCKSDB_METRICS_ENABLED = "rocksdb.metrics.enabled";
    
    /** The Constant ROCKSDB_METRICS_LIST. */
    public static final String ROCKSDB_METRICS_LIST = "rocksdb.metrics.list";
    
    /** The Constant ROCKSDB_METRICS_THREAD_INITIAL_DELAY_MS. */
    public static final String ROCKSDB_METRICS_THREAD_INITIAL_DELAY_MS = "rocksdb.metrics.thread.initial.delay.ms";
    
    /** The Constant ROCKSDB_METRICS_THREAD_FREQUENCY_MS. */
    public static final String ROCKSDB_METRICS_THREAD_FREQUENCY_MS = "rocksdb.metrics.thread.frequency.ms";
    
    /**
     * RDNG 171859 and RTC 525171 Report internal cache metrics to Prometheus .
     */
    public static final String INTERNAL_METRICS_ENABLED = "internal.metrics.enabled";

    /**
     * RDNG 171813.
     */
    public static final String KAFKA_TOPIC_NAME_PLATFORM_PREFIXES = "kafka.topic.name.platform.prefixes";
    
    /** The Constant MQTT_BROKER_URL_SUFFIX. */
    public static final String MQTT_BROKER_URL_SUFFIX = ".broker.url";
    
    /** The Constant MQTT_USER_NAME_SUFFIX. */
    public static final String MQTT_USER_NAME_SUFFIX = ".user.name";
    
    /** The Constant MQTT_USER_PASSWORD_SUFFIX. */
    public static final String MQTT_USER_PASSWORD_SUFFIX = ".user.password";
    
    /** The Constant MQTT_BROKER_PORT_SUFFIX. */
    public static final String MQTT_BROKER_PORT_SUFFIX = ".broker.port";
    
    /** The Constant MQTT_CONFIG_QOS_SUFFIX. */
    public static final String MQTT_CONFIG_QOS_SUFFIX = ".config.qos";
    
    /** The Constant MQTT_MAX_INFLIGHT_SUFFIX. */
    public static final String MQTT_MAX_INFLIGHT_SUFFIX = ".max.inflight";
    
    /** The Constant MQTT_TIMEOUT_IN_MILLIS_SUFFIX. */
    public static final String MQTT_TIMEOUT_IN_MILLIS_SUFFIX = ".timeout.in.millis";
    
    /** The Constant MQTT_KEEP_ALIVE_INTERVAL_SUFFIX. */
    public static final String MQTT_KEEP_ALIVE_INTERVAL_SUFFIX = ".keep.alive.in.seconds";
    
    /** The Constant MQTT_CLEAN_SESSION_SUFFIX. */
    public static final String MQTT_CLEAN_SESSION_SUFFIX = ".clean.session";

    /** The Constant MQTT_CLIENT_AUTH_MECHANISM_SUFFIX. */
    public static final String MQTT_CLIENT_AUTH_MECHANISM_SUFFIX = ".client.auth.mechanism";
    
    /** The Constant MQTT_SERVICE_TRUSTSTORE_PATH_SUFFIX. */
    public static final String MQTT_SERVICE_TRUSTSTORE_PATH_SUFFIX = ".service.truststore.path";
    
    /** The Constant MQTT_SERVICE_TRUSTSTORE_PASSWORD_SUFFIX. */
    public static final String MQTT_SERVICE_TRUSTSTORE_PASSWORD_SUFFIX = ".service.truststore.password";
    
    /** The Constant MQTT_SERVICE_TRUSTSTORE_TYPE_SUFFIX. */
    public static final String MQTT_SERVICE_TRUSTSTORE_TYPE_SUFFIX = ".service.truststore.type";
    
    /** The Constant MAX_DECOMPRESS_INPUT_STREAM_SIZE_IN_BYTES. */
    public static final String MAX_DECOMPRESS_INPUT_STREAM_SIZE_IN_BYTES = "max.decompress.input.stream.size.in.bytes";
}

