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

package org.eclipse.ecsp.analytics.stream.base.kafka;

import de.flapdoodle.embed.process.runtime.Network;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.test.TestCondition;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.jdk.CollectionConverters;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class SingleNodeKafkaCluster extends ExternalResource {

    /** The Constant LOG. */
    private static final Logger LOG = LoggerFactory.getLogger(SingleNodeKafkaCluster.class);
    
    /** The Constant KAFKA_SCHEMAS_TOPIC. */
    private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
    
    /** The Constant KAFKASTORE_OPERATION_TIMEOUT_MS. */
    private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "60000";
    
    /** The Constant KAFKASTORE_DEBUG. */
    private static final String KAFKASTORE_DEBUG = "true";
    
    /** The Constant KAFKASTORE_INIT_TIMEOUT. */
    private static final String KAFKASTORE_INIT_TIMEOUT = "90000";
    
    /** The kafka broker port. */
    private static int kafkaBrokerPort = 1234; // pick a random port
    
    /** The broker config. */
    private final Properties brokerConfig;
    
    /** The zookeeper. */
    private EmbeddedZookeeper zookeeper;
    
    /** The broker. */
    private EmbeddedKafka broker;
    
    /** The running. */
    private boolean running;

    /**
     * Creates and starts the cluster.
     */
    public SingleNodeKafkaCluster() {
        this(new Properties());
    }

    /**
     * Creates and starts the cluster.
     *
     * @param brokerConfig
     *         Additional broker configuration settings.
     */
    public SingleNodeKafkaCluster(final Properties brokerConfig) {
        this.brokerConfig = new Properties();

        this.brokerConfig.putAll(brokerConfig);
    }

    /**
     * Zkconnectstring.
     *
     * @return the string
     */
    public String zkconnectstring() {
        return zookeeper.connectString();
    }

    /**
     * Creates and starts the cluster.
     *
     * @throws Exception the exception
     */
    public void start() throws Exception {
        LOG.debug("Initiating embedded Kafka cluster startup");
        LOG.debug("Starting a ZooKeeper instance...");
        zookeeper = new EmbeddedZookeeper();
        LOG.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

        final Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
        LOG.debug("Starting a Kafka instance on port {} ...",
                effectiveBrokerConfig.getProperty(KafkaConfig.ListenersProp()));
        broker = new EmbeddedKafka(effectiveBrokerConfig);
        LOG.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
                broker.brokerList(), broker.zookeeperConnect());
    }

    /**
     * Effective broker config from.
     *
     * @param brokerConfig the broker config
     * @param zookeeper the zookeeper
     * @return the properties
     */
    private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final EmbeddedZookeeper zookeeper) {
        final Properties effectiveConfig = new Properties();

        try {
            kafkaBrokerPort = Network.getFreeServerPort();
        } catch (IOException e) {
            LOG.error("Error fetching kafka port", e);
        }
        effectiveConfig.putAll(brokerConfig);
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(),
                TestConstants.INT_30 * TestConstants.THOUSAND);
        effectiveConfig.put(KafkaConfig.ListenersProp(), String.format("PLAINTEXT://127.0.0.1:%s", kafkaBrokerPort));
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(),
                TestConstants.INT_60 * TestConstants.THOUSAND);
        effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(),
                TestConstants.TWO * TestConstants.LONG_1024 * TestConstants.LONG_1024);
        effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
        effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        return effectiveConfig;
    }

    /**
     * Before.
     *
     * @throws Exception the exception
     */
    @Override
    protected void before() throws Exception {
        start();
    }

    /**
     * After.
     */
    @Override
    protected void after() {
        stop();
    }

    /**
     * Stops the cluster.
     */
    public void stop() {
        LOG.info("Stopping Confluent");
        try {
            if (broker != null) {
                broker.stop();
            }
            try {
                if (zookeeper != null) {
                    zookeeper.stop();
                }
            } catch (final IOException fatal) {
                throw new RuntimeException(fatal);
            }
        } finally {
            running = false;
        }
        LOG.info("Confluent Stopped");
    }

    /**
     * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
     * <p>
     * You can use this to tell Kafka Streams applications,
     * Kafka producers, and Kafka consumers (new consumer API) how to connect to this
     * cluster.
     * </p>
     *
     * @return the string
     */
    public String bootstrapServers() {
        return broker.brokerList();
    }

    /**
     * Delete topic.
     *
     * @param topic the topic
     */
    public void deleteTopic(final String topic) {
        broker.deleteTopic(topic);
    }

    /**
     * Creates a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic         The name of the topic.
     * @throws InterruptedException the interrupted exception
     */
    public void createTopic(final String topic) throws InterruptedException {
        createTopic(topic, 1, (short) 1, Collections.emptyMap());
    }

    /**
     * Creates a Kafka topic with the given parameters.
     *
     * @param topic         The name of the topic.
     * @param partitions         The number of partitions for this topic.
     * @param replication         The replication factor for (the partitions of) this topic.
     * @throws InterruptedException the interrupted exception
     */
    public void createTopic(final String topic, final int partitions,
            final short replication) throws InterruptedException {
        createTopic(topic, partitions, replication, Collections.emptyMap());
    }

    /**
     * Creates a Kafka topic with the given parameters.
     *
     * @param topic         The name of the topic.
     * @param partitions         The number of partitions for this topic.
     * @param replication         The replication factor for (partitions of) this topic.
     * @param topicConfig         Additional topic-level configuration settings.
     * @throws InterruptedException the interrupted exception
     */
    public void createTopic(final String topic,
            final int partitions,
            final short replication,
            final Map<String, String> topicConfig) throws InterruptedException {
        createTopic(TestConstants.THREAD_SLEEP_TIME_60000, topic, partitions, replication, topicConfig);
    }

    /**
     * Creates a Kafka topic with the given parameters and blocks until all topics got created.
     *
     * @param timeoutMs the timeout ms
     * @param topic         The name of the topic.
     * @param partitions         The number of partitions for this topic.
     * @param replication         The replication factor for (partitions of) this topic.
     * @param topicConfig         Additional topic-level configuration settings.
     * @throws InterruptedException the interrupted exception
     */
    public void createTopic(final long timeoutMs,
            final String topic,
            final int partitions,
            final short replication,
            final Map<String, String> topicConfig) throws InterruptedException {
        broker.createTopic(topic, partitions, replication, topicConfig);

    }

    /**
     * Deletes multiple topics and blocks until all topics got deleted.
     *
     * @param timeoutMs         the max time to wait for the topics to be deleted (does not block if {@code <= 0})
     * @param topics         the name of the topics
     * @throws InterruptedException the interrupted exception
     */
    public void deleteTopicsAndWait(final long timeoutMs, final String... topics) throws InterruptedException {
        for (final String topic : topics) {
            try {
                broker.deleteTopic(topic);
            } catch (final UnknownTopicOrPartitionException expected) {
                // indicates (idempotent) success
            }
        }

    }

    /**
     * Checks if is running.
     *
     * @return true, if is running
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * The Class TopicsDeletedCondition.
     */
    private final class TopicsDeletedCondition implements TestCondition {
        
        /** The deleted topics. */
        final Set<String> deletedTopics = new HashSet<>();

        /**
         * Instantiates a new topics deleted condition.
         *
         * @param topics the topics
         */
        private TopicsDeletedCondition(final String... topics) {
            Collections.addAll(deletedTopics, topics);
        }

        /**
         * Condition met.
         *
         * @return true, if successful
         */
        @Override
        public boolean conditionMet() {
            final Set<String> allTopicsFromZk = new HashSet<>(
                    CollectionConverters.SetHasAsJava(broker.kafkaServer()
                            .zkClient().getAllTopicsInCluster(false)).asJava());

            final Set<String> allTopicsFromBrokerCache = new HashSet<>(
                    CollectionConverters.SeqHasAsJava(broker.kafkaServer()
                            .metadataCache().getAllTopics().toSeq()).asJava());

            return !allTopicsFromZk.removeAll(deletedTopics) && !allTopicsFromBrokerCache.removeAll(deletedTopics);
        }
    }

    /**
     * The Class TopicCreatedCondition.
     */
    private final class TopicCreatedCondition implements TestCondition {
        
        /** The created topic. */
        final String createdTopic;

        /**
         * Instantiates a new topic created condition.
         *
         * @param topic the topic
         */
        private TopicCreatedCondition(final String topic) {
            createdTopic = topic;
        }

        /**
         * Condition met.
         *
         * @return true, if successful
         */
        @Override
        public boolean conditionMet() {
            return broker.kafkaServer().zkClient().getAllTopicsInCluster(false).contains(createdTopic)
                && broker.kafkaServer().metadataCache().contains(createdTopic);
        }
    }
}