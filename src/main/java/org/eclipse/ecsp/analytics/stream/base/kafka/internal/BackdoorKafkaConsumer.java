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

package org.eclipse.ecsp.analytics.stream.base.kafka.internal;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams.State;
import org.eclipse.ecsp.analytics.stream.base.KafkaSslConfig;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.exception.BackdoorKafkaConsumerException;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidKeyOrValueException;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.ObjectUtils;
import org.eclipse.ecsp.analytics.stream.base.utils.ThreadUtils;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.healthcheck.HealthMonitor;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.transform.GenericIgniteEventTransformer;
import org.eclipse.ecsp.transform.IgniteKeyTransformer;
import org.eclipse.ecsp.transform.Transformer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.eclipse.ecsp.analytics.stream.base.utils.Constants.CANNOT_BE_EMPTY;

/**
 * BackdoorKafkaConsumer is a Kafka consumer
 * It subscribes to the topic provided in the properties.
 * serviceCallBack instance is provided by the respective SPs for accessing the Kafka consumer records.
 * ConsumerRecords{@code <}byte[], byte[]{@code >} is transformed to
 * IgniteKey and IgniteEvent with the help of transformers instantiated based on the
 * properties that has been set.
 *
 * @author avadakkootko
 */

public abstract class BackdoorKafkaConsumer implements HealthMonitor {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(BackdoorKafkaConsumer.class);
    
    /** The consumer. */
    protected KafkaConsumer<byte[], byte[]> consumer = null;

    /** The value transformer. */
    private GenericIgniteEventTransformer valueTransformer = new GenericIgniteEventTransformer();
    
    /** The key transformer. */
    private IgniteKeyTransformer<?> keyTransformer;
    
    /** The poll. */
    private long poll;
    
    /** The Constant DEFAULT_POLL_VALUE. */
    private static final long DEFAULT_POLL_VALUE = 50L;
    // RTC-155383 - Running Kafka and Zookeeper on dynamic ports to resolve
    /** The Constant OVERRIDDEN_BOOT_STRAP_SERVER. */
    // bind address issue in streambase project while running test cases
    public static final String OVERRIDDEN_BOOT_STRAP_SERVER = null;
    
    /** The closed. */
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    /** The call back map. */
    protected ConcurrentHashMap<Integer, BackdoorKafkaConsumerCallback> callBackMap = new ConcurrentHashMap<>();
    
    /** The persist offset map. */
    private ConcurrentHashMap<String, BackdoorKafkaTopicOffset> persistOffsetMap = new ConcurrentHashMap<>();

    // startedConsumer ensures that for the same service backdoor kafka consumer
    // is not re started multiple times. Earlier BackDoor kafka consumer factory
    /** The started consumer. */
    // used to handle it.
    protected final AtomicBoolean startedConsumer = new AtomicBoolean(false);
    
    /** The healthy. */
    private final AtomicBoolean healthy = new AtomicBoolean(false);
    
    /** The kafka consumer run executor. */
    protected ExecutorService kafkaConsumerRunExecutor;
    
    /** The offsets mgmt executor. */
    protected ScheduledExecutorService offsetsMgmtExecutor = null;

    /** The ignite key transformer impl. */
    @Value("${" + PropertyNames.IGNITE_KEY_TRANSFORMER + ":}")
    private String igniteKeyTransformerImpl;
    
    /** The kafka bootstrap servers. */
    @Value("${" + PropertyNames.BOOTSTRAP_SERVERS + ":}")
    private String kafkaBootstrapServers;
    
    /** The kafka ssl enable. */
    @Value("${" + PropertyNames.KAFKA_SSL_ENABLE + ":false}")
    private boolean kafkaSslEnable;
    
    /** The kafka one way tls enable. */
    @Value("${" + PropertyNames.KAFKA_ONE_WAY_TLS_ENABLE + ":false}")
    private boolean kafkaOneWayTlsEnable;
    
    /** The keystore. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_KEYSTORE + ":}")
    private String keystore;
    
    /** The keystore pwd. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_KEYSTORE_PASSWORD + ":}")
    private String keystorePwd;
    
    /** The key pwd. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_KEY_PASSWORD + ":}")
    private String keyPwd;
    
    /** The truststore. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_TRUSTSTORE + ":}")
    private String truststore;
    
    /** The sasl mechanism. */
    @Value("${" + PropertyNames.KAFKA_SASL_MECHANISM + ":}")
    private String saslMechanism;
    
    /** The sasl jaas config. */
    @Value("${" + PropertyNames.KAFKA_SASL_JAAS_CONFIG + ":}")
    private String saslJaasConfig;
    
    /** The ssl endpoint algo. */
    @Value("${" + PropertyNames.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM + ":}")
    private String sslEndpointAlgo;
    
    /** The truststore pwd. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_TRUSTSTORE_PASSWORD + ":}")
    private String truststorePwd;
    
    /** The ssl client auth. */
    @Value("${" + PropertyNames.KAFKA_SSL_CLIENT_AUTH + ":}")
    private String sslClientAuth;
    
    /** The convert topic to lower case. */
    @Value("${" + PropertyNames.CONVERT_BACKDOOR_KAFKA_TOPIC_TO_LOWERCASE + ":true}")
    private boolean convertTopicToLowerCase;
    
    /** The max poll interval ms. */
    @Value("${" + PropertyNames.BACKDOOR_KAFKA_MAX_POLL_INTERVAL_MS + ":600000}")
    private int maxPollIntervalMs;
    
    /** The request timeout ms. */
    @Value("${" + PropertyNames.BACKDOOR_KAFKA_REQUEST_TIMEOUT_MS + ":605000}")
    private int requestTimeoutMs;
    
    /** The session timeout ms. */
    @Value("${" + PropertyNames.BACKDOOR_KAFKA_SESSION_TIMEOUT_MS + ":250000}")
    private int sessionTimeoutMs;
    
    /** The max restart attempts. */
    @Value("${" + PropertyNames.BACKDOOR_KAFKA_MAX_RESTART_ATTEMPTS + ":5}")
    private int maxRestartAttempts;
    
    /** The restart attempt reset interval min. */
    @Value("${" + PropertyNames.BACKDOOR_KAFKA_ATTEMPTS_RESET_INTERVAL_MIN + ":30}")
    private int restartAttemptResetIntervalMin;
    
    /** The enable auto commit. */
    @Value("${" + PropertyNames.BACKDOOR_KAFKA_ENABLE_AUTO_COMMIT + ":false}")
    private boolean enableAutoCommit;
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The offset persistence delay. */
    @Value("${" + PropertyNames.BACKDOOR_KAFKA_OFFSET_PERSISTENCE_DELAY + ":60000}")
    private int offsetPersistenceDelay;
    
    /** The connection msg value transformer. */
    @Value("${" + PropertyNames.DMA_CONNECTION_MSG_VALUE_TRANSFORMER + ":}")
    private String connectionMsgValueTransformer;
    
    /** The connection msg key transformer. */
    @Value("${" + PropertyNames.DMA_CONNECTION_MSG_KEY_TRANSFORMER + ":}")
    private String connectionMsgKeyTransformer;
    
    /** The default api timeout ms. */
    @Value("${" + PropertyNames.BACKDOOR_KAFKA_CONSUMER_DEFAULT_API_TIMEOUT_MS + ":60000}")
    private int defaultApiTimeoutMs;
    
    /** The connections max idle ms. */
    @Value("${" + PropertyNames.CONNECTIONS_MAX_IDLE_MS + ":-1}")
    private int connectionsMaxIdleMs;
    
    /** The client id. */
    @Value("${" + PropertyNames.CLIENT_ID + ":}")
    private String clientId;
    
    /** The payload value transformer. */
    private Transformer payloadValueTransformer = null;

    /** The payload key transformer. */
    private IgniteKeyTransformer<?> payloadKeyTransformer = null;

    /** The topic offset dao. */
    @Autowired
    private BackdoorKafkaTopicOffsetDAOMongoImpl topicOffsetDao;
    
    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;
    
    /** The kafka ssl config. */
    @Autowired
    private KafkaSslConfig kafkaSslConfig;
    
    /** The kafka consumer props. */
    private Properties kafkaConsumerProps;
    
    /** The name. */
    private String name;

    /** The kafka consumer topic. */
    private String kafkaConsumerTopic;
    
    /** The restart attempts. */
    private AtomicInteger restartAttempts = new AtomicInteger(0);
    
    /** The previous restart. */
    private long previousRestart = 0L;

    /** The kafka admin client. */
    private AdminClient kafkaAdminClient = null;

    /**
     * Gets the kafka consumer props.
     *
     * @return the kafka consumer props
     */
    public Properties getKafkaConsumerProps() {
        return kafkaConsumerProps;
    }

    /**
     * Sets the ignite key transformer impl.
     *
     * @param igniteKeyTransformerImpl the new ignite key transformer impl
     */
    public void setIgniteKeyTransformerImpl(String igniteKeyTransformerImpl) {
        this.igniteKeyTransformerImpl = igniteKeyTransformerImpl;
    }

    /**
     * Sets the kafka bootstrap servers.
     *
     * @param kafkaBootstrapServers the new kafka bootstrap servers
     */
    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public abstract String getName();

    /**
     * Gets the kafka consumer group id.
     *
     * @return the kafka consumer group id
     */
    public abstract String getKafkaConsumerGroupId();

    /**
     * Gets the kafka consumer topic.
     *
     * @return the kafka consumer topic
     */
    public abstract String getKafkaConsumerTopic();

    /**
     * Sets the kafka consumer topic.
     *
     * @param kafkaConsumerTopic the new kafka consumer topic
     */
    protected void setKafkaConsumerTopic(String kafkaConsumerTopic) {
        this.kafkaConsumerTopic = kafkaConsumerTopic;
    }

    /**
     * Gets the poll.
     *
     * @return the poll
     */
    public abstract long getPoll();

    // The reset flag is set to false after the initial reset of offset. So that
    /**
     * Checks if is offsets reset complete.
     *
     * @return true, if is offsets reset complete
     */
    // its not reset for each execution.
    public abstract boolean isOffsetsResetComplete();

    /**
     * Sets the reset offsets.
     *
     * @param reset the new reset offsets
     */
    public abstract void setResetOffsets(boolean reset);

    /**
     * Gets the stream state.
     *
     * @return the stream state
     */
    public abstract State getStreamState();

    /**
     * Sets the stream state.
     *
     * @param newState the new stream state
     */
    public abstract void setStreamState(State newState);

    /**
     * initialize properties.
     */
    @PostConstruct
    public void initializeProperties() {
        kafkaConsumerProps = new Properties();
        if (StringUtils.isEmpty(igniteKeyTransformerImpl)) {
            throw new IllegalArgumentException(PropertyNames.IGNITE_KEY_TRANSFORMER + CANNOT_BE_EMPTY);
        }
        try {
            keyTransformer = (IgniteKeyTransformer) getClass().getClassLoader().loadClass(igniteKeyTransformerImpl)
                    .getDeclaredConstructor().newInstance();
        } catch (InstantiationException | NoSuchMethodException | ClassNotFoundException | IllegalAccessException
                | InvocationTargetException e) {
            throw new IllegalArgumentException(
                    PropertyNames.IGNITE_KEY_TRANSFORMER + " refers to a class that is not available on the classpath");
        }
        if (StringUtils.isNotEmpty(connectionMsgValueTransformer)) {
            payloadValueTransformer = (Transformer) getInstance(connectionMsgValueTransformer);
            logger.info("Class {} loaded as connection status value transformer", connectionMsgValueTransformer);
        }

        if (StringUtils.isNotEmpty(connectionMsgKeyTransformer)) {
            payloadKeyTransformer = (IgniteKeyTransformer) getInstance(connectionMsgKeyTransformer);
            logger.info("Class {} loaded as connection status value transformer", connectionMsgKeyTransformer);
        }

        if (StringUtils.isEmpty(kafkaBootstrapServers)) {
            throw new IllegalArgumentException(PropertyNames.BOOTSTRAP_SERVERS + CANNOT_BE_EMPTY);
        }
        kafkaConsumerProps.setProperty(PropertyNames.BOOTSTRAP_SERVERS, kafkaBootstrapServers);
        kafkaConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().deserializer().getClass().getName());
        kafkaConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().deserializer().getClass().getName());
        kafkaConsumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        kafkaConsumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        kafkaConsumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        kafkaConsumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);
        kafkaConsumerProps.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
        if (StringUtils.isNotBlank(clientId)) {
            kafkaConsumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }
        
        kafkaSslConfig.setSslPropsIfEnabled(kafkaConsumerProps);
        
        if (this.kafkaAdminClient == null) {
            this.kafkaAdminClient = AdminClient.create(kafkaConsumerProps);
        }
    }

    /**
     * Gets the single instance of BackdoorKafkaConsumer.
     *
     * @param className the class name
     * @return single instance of BackdoorKafkaConsumer
     */
    private Object getInstance(String className) {
        Class<?> classObject = null;
        Object instance = null;
        try {
            classObject = getClass().getClassLoader().loadClass(className);
            instance = ctx.getBean(classObject);
        } catch (Exception ex) {
            try {
                if (classObject == null) {
                    throw new IllegalArgumentException("Could not load the class " + className);
                }
                instance = classObject.getDeclaredConstructor().newInstance();
            } catch (Exception exception) {
                String msg = String.format("Class %s could not be loaded. Not found on classpath.%n",
                        className);
                logger.error(msg + ExceptionUtils.getStackTrace(exception));
                throw new IllegalArgumentException(msg);
            }
        }
        return instance;
    }

    /**
     * Poll from the subscribed topic. And commits the offset values.
     * ConsumerRecords{@code <}byte[], byte[]{@code >} is transformed in to IgniteKey, IgniteEvent.
     */
    private void runConsumer() {
        startedConsumer.set(true);
        try {
            while (!closed.get()) {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(poll));
                healthy.set(true);
                if (!consumerRecords.isEmpty()) {
                    if (isOffsetsResetComplete()) {
                        resetKafkaConsumerOffset();
                    }

                    consumerRecords.forEach(this::processCallBack);
                    consumer.commitSync();
                }
            }

        } catch (WakeupException e) {
            healthy.set(false);
            logger.error("WakeupException in BackDoor Kafka Consumer", e);
            if (!closed.get()) {
                throw e;
            }
            consumer.close();
        } catch (Exception e) {
            healthy.set(false);
            logger.error("Unhandled BackDoor Kafka Consumer error !!! ", e);
            if (getStreamState() == State.RUNNING) {
                logger.info(
                        "Attempting to shutdown and restart Backdoor Kafka Consumer, "
                                + "as an exception occured and  Kafka Streams is in RUNNING state");
                restartKafkaBackDoorConsumer();
            } else {
                logger.error("Closing Backdoor Kafka Consumer, Unhandled exception "
                        + "occured and as Kafka Streams is not RUNNING");
                shutdownWithOutWakeup();
            }
        }
    }

    /**
     * Process call back.
     *
     * @param consumerRecord the consumer record
     */
    private void processCallBack(ConsumerRecord<byte[], byte[]> consumerRecord) {
        byte[] key = consumerRecord.key();
        byte[] value = consumerRecord.value();
        int partition = consumerRecord.partition();
        long offset = consumerRecord.offset();
        try {
            BackdoorKafkaConsumerCallback callBack = callBackMap.get(partition);
            if (callBack != null) {
                processCallBack(consumerRecord, key, value, partition, offset, callBack);
            } else {
                logger.trace("Partition {}, not part of current stream thread", partition);
            }

        } catch (Exception e) {
            logger.error("Error occured while invoking callback {}", e);
        }
    }

    /**
     * Process call back.
     *
     * @param consumerRecord the consumer record
     * @param key the key
     * @param value the value
     * @param partition the partition
     * @param offset the offset
     * @param callBack the call back
     */
    private void processCallBack(ConsumerRecord<byte[], byte[]> consumerRecord, byte[] key, byte[] value, 
            int partition, long offset, BackdoorKafkaConsumerCallback callBack) {
        IgniteKey<?> igniteKey = transformKey(key);
        IgniteEvent igniteEvent = transformValue(value);
        if (igniteKey == null || igniteEvent == null) {
            String msg = "Either Key or Value of connection status message could not be transformed. "
                    + "No further processing will be done for this message.";
            throw new InvalidKeyOrValueException(msg);
        }
        OffsetMetadata meta = new OffsetMetadata(new TopicPartition(consumerRecord.topic(), partition),
                consumerRecord.offset());
        String persistOffsetKey = getKey(kafkaConsumerTopic, partition);
        logger.debug("Forward to serviceCallBack - Key : {}, Value : {}, Offset : {}, Partition : {}", igniteKey,
                igniteEvent, consumerRecord.offset(), consumerRecord.partition());
        callBack.process(igniteKey, igniteEvent, meta);
        BackdoorKafkaTopicOffset backdoorKafkaTopicOffset = persistOffsetMap.get(persistOffsetKey);
        if (backdoorKafkaTopicOffset == null) {
            persistOffsetMap.put(persistOffsetKey,
                    new BackdoorKafkaTopicOffset(kafkaConsumerTopic, partition, offset));
        } else {
            backdoorKafkaTopicOffset.setOffset(offset);
        }
    }

    /**
     * Transform key.
     *
     * @param key the key
     * @return the ignite key
     */
    private IgniteKey<?> transformKey(byte[] key) {
        if (this.payloadKeyTransformer != null) {
            logger.debug("Transforming the key part of connection message using {}", this.connectionMsgKeyTransformer);
            return this.payloadKeyTransformer.fromBlob(key);
        } else {
            logger.debug("Transforming the key part of connection message using {}", this.igniteKeyTransformerImpl);
            return this.keyTransformer.fromBlob(key);
        }
    }

    /**
     * Transform value.
     *
     * @param value the value
     * @return the ignite event
     */
    private IgniteEvent transformValue(byte[] value) {
        if (this.payloadValueTransformer != null) {
            logger.debug("Transforming the value part of connection message using {}", connectionMsgValueTransformer);
            return this.payloadValueTransformer.fromBlob(value, Optional.empty());
        } else {
            logger.debug("Transforming the value part of connection "
                    + "message using default GenericIgniteEventTransformer");
            return this.valueTransformer.fromBlob(value, Optional.empty());
        }
    }

    /**
     * Gets the key.
     *
     * @param topic the topic
     * @param partition the partition
     * @return the key
     */
    protected String getKey(String topic, int partition) {
        return topic + ":" + partition;
    }

    /**
     * Reset kafka consumer offset.
     */
    protected void resetKafkaConsumerOffset() {
        Map<Integer, Long> topicOffsetMap = getTopicOffsetMap();

        logger.info("Attempting to reset offset for backdoor kafka consumer topic - {}", kafkaConsumerTopic);
        List<PartitionInfo> partitions = consumer.partitionsFor(kafkaConsumerTopic);
        List<TopicPartition> topicPartitions = partitions.stream()
                .map(p -> new TopicPartition(kafkaConsumerTopic, p.partition()))
                .toList();

        Map<TopicPartition, Long> endOffsetMap = consumer.endOffsets(topicPartitions);
        Map<TopicPartition, Long> beginningOffsetMap = consumer.beginningOffsets(topicPartitions);
        // Below set contains the partitions that are ASSIGNED to this consumer
        // for this kafkaConsumerTopic.
        Set<Integer> assignedPartitions = new HashSet<>();
        Set<TopicPartition> partitionSet = consumer.assignment();
        for (TopicPartition topicPartition : partitionSet) {
            assignedPartitions.add(topicPartition.partition());
        }

        for (TopicPartition topicPartition : topicPartitions) {
            // Skip offset reset for the partitions whose messages are not meant
            // to be processed by this consumer.
            if (!assignedPartitions.contains(topicPartition.partition())
                    && !this.callBackMap.containsKey(topicPartition.partition())) {
                logger.debug("Skipping offset reset for partition: {}",
                        topicPartition.partition());
                continue;
            }
            int partition = topicPartition.partition();
            long endOffset = endOffsetMap.get(topicPartition);
            long beginningOffset = beginningOffsetMap.get(topicPartition);

            Long offsetToSeek = topicOffsetMap.get(partition);

            if (offsetToSeek != null && offsetToSeek <= endOffset && offsetToSeek >= beginningOffset) {
                consumer.seek(topicPartition, offsetToSeek);
                logger.info(
                        "Reset offset to {} for topic {} and partition {} with beginningOffset {} and endOffset {}",
                        offsetToSeek, kafkaConsumerTopic, partition, beginningOffset, endOffset);
            } else if (offsetToSeek == null) {
                // offsetToSeek == null implies this instance of
                // stream processor is not responsible for this
                // partition. So its ok to seek to beginning.
                consumer.seekToEnd(Collections.singletonList(topicPartition));
                logger.info(
                        "Reset to offset to end as seek offset was {} for "
                                + "topic {} and partition {} with beginningOffset {} and endOffset {}",
                        offsetToSeek, kafkaConsumerTopic, partition, beginningOffset, endOffset);
            } else {
                // offsetToSeek > endOffset can only happen if
                // someone deletes the topic. Hence seek to
                // beginning.

                // offsetToSeek < beginningOffset is rare but it can
                // happen if the sp was not up for a very long
                // duration say 3 days and kafka retention was 2
                // days. Hence seek to beginning.
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
                logger.info("Reset to offset to beginning as seek offset was {} for " + "topic {} "
                        + "and partition {} with beginningOffset {} and endOffset {}",
                        offsetToSeek, kafkaConsumerTopic, partition, beginningOffset, endOffset);
            }
            setResetOffsets(false);
        }
    }

    /**
     * Gets the topic offset map.
     *
     * @return the topic offset map
     */
    protected Map<Integer, Long> getTopicOffsetMap() {
        Map<Integer, Long> map = new HashMap<>();
        List<BackdoorKafkaTopicOffset> topicOffsetList = topicOffsetDao.getTopicOffsetList(kafkaConsumerTopic);
        for (BackdoorKafkaTopicOffset topicOffset : topicOffsetList) {
            if (callBackMap.containsKey(topicOffset.getPartition())) {
                map.put(topicOffset.getPartition(), topicOffset.getOffset());
            }
        }
        return map;
    }

    /**
     * Restart kafka back door consumer.
     */
    // This method is invoked when BackDoor kafka consumer has an exception
    private void restartKafkaBackDoorConsumer() {
        shutdownWithOutWakeup();
        if ((restartAttempts.get() <= maxRestartAttempts)) {
            startBackDoorKafkaConsumer();
            restartAttempts.incrementAndGet();

            // reset number of attempts to zero if interval between two restarts
            // is
            // > restartAttemptResetIntervalMin
            long currentTime = System.currentTimeMillis();
            long diff = currentTime - previousRestart;
            diff = diff / Constants.THREAD_SLEEP_TIME_60000;
            if (diff > restartAttemptResetIntervalMin) {
                restartAttempts = new AtomicInteger(0);
            }
            previousRestart = currentTime;
        } else {
            logger.info("BackDoor restart attempts has exceeded {}. "
                    + "Shutting down Stream Processor", maxRestartAttempts);
            System.exit(1);
        }
    }

    /**
     * Shutdown with out wakeup.
     */
    // This method is invoked when BackDoor kafka consumer has an exception
    private void shutdownWithOutWakeup() {
        try {
            consumer.close();
        } catch (Exception e) {
            logger.error("Error while trying to close backdoor kafka consumer", e);
        } finally {
            closed.set(true);
            startedConsumer.set(false);
            ThreadUtils.shutdownExecutor(kafkaConsumerRunExecutor, Constants.THREAD_SLEEP_TIME_10000, false);
            ThreadUtils.shutdownExecutor(offsetsMgmtExecutor, Constants.THREAD_SLEEP_TIME_10000, false);
            logger.info("Closed Backdoor Kafka Consumer");
        }
    }

    /**
     * This method starts the kafka consumer by invoking runConsumer().
     */
    public void startBackDoorKafkaConsumer() {
        String kafkaConsumerGroupId;
        if (!startedConsumer.get()) {
            name = getName();
            if (this.callBackMap.isEmpty()) {
                logger.error("Callback map for service {} is found to be empty. "
                        + "Backdoor kafka consumer will not be started.", name);
                return;
            } 
            
            kafkaConsumerTopic = getKafkaConsumerTopic();
            if (convertTopicToLowerCase) {
                kafkaConsumerTopic = kafkaConsumerTopic.toLowerCase();
            }
            kafkaConsumerGroupId = getKafkaConsumerGroupId();
            poll = getPoll();

            closed.set(false);
            ObjectUtils.requireNonEmpty(name, "BackDoor Kafka Consumer name must be provided");
            ObjectUtils.requireNonEmpty(kafkaConsumerTopic, "Kafka Consumer topic must be provided");
            if (poll <= 0) {
                poll = DEFAULT_POLL_VALUE;
                logger.info("Poll value being changed to default value {} for {}", DEFAULT_POLL_VALUE, name);
            }
            ObjectUtils.requireNonEmpty(kafkaConsumerGroupId, "Group Id cannot be null for Kafka Consumer");
            kafkaConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupId);

            consumer = new KafkaConsumer<>(kafkaConsumerProps);
            logger.info("Backdoor Kafka Consumer initialized with properties : {}", kafkaConsumerProps);

            consumer.subscribe(Collections.singletonList(kafkaConsumerTopic));
            logger.info("Kafka consumer group {} subscribed to topic {}", kafkaConsumerGroupId, kafkaConsumerTopic);

            kafkaConsumerRunExecutor = Executors.newFixedThreadPool(1, getThreadFactory(name + "-kafkaConsumerDt"));

            kafkaConsumerRunExecutor.execute(() -> {
                try {
                    logger.info("Running backdoor kafka consumer ... :");
                    runConsumer();
                } catch (Exception e) {
                    throw new BackdoorKafkaConsumerException("Exception occurred in backdoor kafka consumer", e);
                }
            });

            offsetsMgmtExecutor = Executors.newSingleThreadScheduledExecutor(getThreadFactory(name + "-topicOffsetDt"));

            offsetsMgmtExecutor.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        persistOffset();
                    } catch (Exception e) {
                        logger.error("Error occured while persisting offset to database by backdoor consumer", e);
                    }
                }

                private void persistOffset() {
                    persistForEachTopicOffSet();
                }
            }, Constants.THIRTY_THOUSAND, offsetPersistenceDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("BackDoor Kafka Consumer already started for service " + name + ". Cannot Restart again !!!");
        }
    }

    /**
     * Persist for each topic off set.
     */
    private void persistForEachTopicOffSet() {
        for (BackdoorKafkaTopicOffset topicOffset : persistOffsetMap.values()) {
            try {
                topicOffsetDao.save(new BackdoorKafkaTopicOffset(topicOffset));
                logger.debug("Persisted kafka topic offset to database. {}", topicOffset.toString());
            } catch (Exception e) {
                logger.error("Error occured while persisting offset {} by backdoor consumer with exception:",
                        topicOffset.toString(), e);
            }
        }
    }

    /**
     * Gets the thread factory.
     *
     * @param threadName the thread name
     * @return the thread factory
     */
    private ThreadFactory getThreadFactory(String threadName) {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(threadName);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((thread1, t) -> 
                logger.error("Uncaught exception detected! . Exception is: {}", t));
            return thread;
        };
    }

    /**
     * Shut down kafka consumer. This method is invoked when Stream closes.
     */
    public void shutdown() {
        if (getStartedConsumer().get() && !getClosed().get()) {
            closed.set(true);
            startedConsumer.set(false);
            if (consumer != null) {
                consumer.wakeup();
            }

            // RTC-192213 - Added to clear the cache held by
            // IntegrationFeedCacheUpdateCallBack. More specifically added to
            // ensure that the third party kafka broker producers are flushed
            // before the application shuts down. This will ensure that the data
            // are flushed immediately in case of kafka batching.
            callBackMap.forEach((k, v) -> v.close());

            callBackMap.clear();
            logger.info("Cleared Callback map");

            ThreadUtils.shutdownExecutor(kafkaConsumerRunExecutor, Constants.THREAD_SLEEP_TIME_10000, false);
            ThreadUtils.shutdownExecutor(offsetsMgmtExecutor, Constants.THREAD_SLEEP_TIME_10000, false);
            removeConsumerGroup(getKafkaConsumerGroupId());
            logger.info("Closed Backdoor Kafka Consumer");

        }
    }

    /**
     * Removes the consumer group.
     *
     * @param consumerGroupId the consumer group id
     */
    protected void removeConsumerGroup(String consumerGroupId) {
        logger.info("Group ID received to be removed is: {}", consumerGroupId);
        if (!StringUtils.isEmpty(consumerGroupId)) {
            kafkaAdminClient.deleteConsumerGroups(Arrays.asList(consumerGroupId));
            logger.debug("Removed {} consumer group from cluster.", consumerGroupId);
        }
    }

    /**
     * Gets the kafka consumer run executor.
     *
     * @return the kafka consumer run executor
     */
    // Setters for unit test
    protected ExecutorService getKafkaConsumerRunExecutor() {
        return kafkaConsumerRunExecutor;
    }

    /**
     * Sets the kafka consumer run executor.
     *
     * @param kafkaConsumerRunExecutor the new kafka consumer run executor
     */
    protected void setKafkaConsumerRunExecutor(ExecutorService kafkaConsumerRunExecutor) {
        this.kafkaConsumerRunExecutor = kafkaConsumerRunExecutor;
    }

    /**
     * Gets the offsets mgmt executor.
     *
     * @return the offsets mgmt executor
     */
    protected ScheduledExecutorService getOffsetsMgmtExecutor() {
        return offsetsMgmtExecutor;
    }

    /**
     * Sets the offsets mgmt executor.
     *
     * @param offsetsMgmtExecutor the new offsets mgmt executor
     */
    protected void setOffsetsMgmtExecutor(ScheduledExecutorService offsetsMgmtExecutor) {
        this.offsetsMgmtExecutor = offsetsMgmtExecutor;
    }

    /**
     * Gets the closed.
     *
     * @return the closed
     */
    protected AtomicBoolean getClosed() {
        return closed;
    }

    /**
     * Gets the started consumer.
     *
     * @return the started consumer
     */
    protected AtomicBoolean getStartedConsumer() {
        return startedConsumer;
    }

    /**
     * Gets the kafka admin client.
     *
     * @return the kafka admin client
     */
    protected AdminClient getKafkaAdminClient() {
        return kafkaAdminClient;
    }

    /**
     * Sets the kafka admin client.
     *
     * @param kafkaAdminClient the new kafka admin client
     */
    protected void setKafkaAdminClient(AdminClient kafkaAdminClient) {
        this.kafkaAdminClient = kafkaAdminClient;
    }

    /**
     * addCallback().
     *
     * @param callBack callBack
     * @param partition partition
     */
    public void addCallback(BackdoorKafkaConsumerCallback callBack, int partition) {
        callBackMap.put(partition, callBack);
        logger.info("Adding Call back for service {}, for partition {} " + "and current size of map is {}", 
                getName(), partition, callBackMap.size());
    }

    /**
     * Sets the consumer.
     *
     * @param consumer the consumer
     */
    protected void setConsumer(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    /**
     * Checks if is healthy.
     *
     * @param forceHealthCheck the force health check
     * @return true, if is healthy
     */
    @Override
    public boolean isHealthy(boolean forceHealthCheck) {
        State currState = getStreamState();
        if (currState != null && currState != State.RUNNING) {
            return true;
        }
        return healthy.get();
    }

    /**
     * Sets the connection msg value transformer.
     *
     * @param connectionMsgValueTransformer the new connection msg value transformer
     */
    // The below setter and getter are for test cases
    public void setConnectionMsgValueTransformer(String connectionMsgValueTransformer) {
        this.connectionMsgValueTransformer = connectionMsgValueTransformer;
    }

    /**
     * Gets the payload value transformer.
     *
     * @return the payload value transformer
     */
    public Transformer getPayloadValueTransformer() {
        return payloadValueTransformer;
    }

}