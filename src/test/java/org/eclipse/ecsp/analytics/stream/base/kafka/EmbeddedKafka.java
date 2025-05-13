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

import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.ArraySeq;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;



/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by default.
 * Requires a running ZooKeeper instance to connect to.  By default,
 * it expects a ZooKeeper instance running at `127.0.0.1:2181`.  You can
 * specify a different ZooKeeper instance by setting the `zookeeper.connect` parameter in the broker's configuration.
 */
public class EmbeddedKafka {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKafka.class);

    /** The Constant DEFAULT_ZK_CONNECT. */
    private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";

    /** The effective config. */
    private final Properties effectiveConfig;
    
    /** The log dir. */
    private final File logDir;
    
    /** The tmp folder. */
    private final TemporaryFolder tmpFolder;
    
    /** The kafka. */
    private final KafkaServer kafka;

    /**
     * Creates and starts an embedded Kafka broker.
     *
     * @param config         Broker configuration settings.  Used to modify, for example,
     *         on which port the broker should listen to.  Note that you cannot
     *         change some settings such as `log.dirs`, `port`.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public EmbeddedKafka(final Properties config) throws IOException {
        tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        logDir = tmpFolder.newFolder();
        effectiveConfig = effectiveConfigFrom(config);
        final boolean loggingEnabled = true;

        final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
        LOGGER.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
                logDir, zookeeperConnect());
        kafka = TestUtils.createServer(kafkaConfig, Time.SYSTEM);
        LOGGER.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
                brokerList(), zookeeperConnect());
    }

    /**
     * Effective config from.
     *
     * @param initialConfig the initial config
     * @return the properties
     */
    private Properties effectiveConfigFrom(final Properties initialConfig) {
        final Properties effectiveConfigProps = new Properties();
        effectiveConfigProps.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
        effectiveConfigProps.put(KafkaConfig.ListenersProp(), "PLAINTEXT://127.0.0.1:9092");
        effectiveConfigProps.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
        effectiveConfigProps.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        effectiveConfigProps.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), Constants.INT_1000000);
        effectiveConfigProps.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);

        effectiveConfigProps.putAll(initialConfig);
        effectiveConfigProps.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());

        //effectiveConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, effectiveConfig)
        effectiveConfigProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        effectiveConfigProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        effectiveConfigProps.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        return effectiveConfigProps;
    }

    /**
     * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
     * You can use this to tell Kafka producers and consumers how to connect to this instance.
     *
     * @return the string
     */
    public String brokerList() {
        final EndPoint endPoint = ((ArraySeq<EndPoint>) kafka.advertisedListeners()).head();
        final String hostname = endPoint.host() == null ? "" : endPoint.host();

        return String.join(":", hostname, Integer.toString(
                kafka.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
        ));
    }

    /**
     * The ZooKeeper connection string aka `zookeeper.connect`.
     *
     * @return the string
     */
    public String zookeeperConnect() {
        return effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
    }

    /**
     * Stop the broker.
     */
    public void stop() {
        LOGGER.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
                brokerList(), zookeeperConnect());
        kafka.shutdown();
        kafka.awaitShutdown();
        LOGGER.debug("Removing temp folder {} with logs.dir at {} ...", tmpFolder, logDir);
        tmpFolder.delete();
        LOGGER.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
                brokerList(), zookeeperConnect());
    }

    /**
     * Create a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic
     *         The name of the topic.
     */
    public void createTopic(final String topic) {
        createTopic(topic, 1, (short) 1, Collections.emptyMap());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic
     *         The name of the topic.
     * @param partitions
     *         The number of partitions for this topic.
     * @param replication
     *         The replication factor for (the partitions of) this topic.
     */
    public void createTopic(final String topic, final int partitions, final short replication) {
        createTopic(topic, partitions, replication, Collections.emptyMap());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic
     *         The name of the topic.
     * @param partitions
     *         The number of partitions for this topic.
     * @param replication
     *         The replication factor for (partitions of) this topic.
     * @param topicConfig
     *         Additional topic-level configuration settings.
     */
    public void createTopic(final String topic,
            final int partitions,
            final short replication,
            final Map<String, String> topicConfig) {
        LOGGER.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
                topic, partitions, replication, topicConfig);

        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

        try (final AdminClient adminClient = AdminClient.create(properties)) {
            final NewTopic newTopic = new NewTopic(topic, partitions, replication);
            newTopic.configs(topicConfig);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException fatal) {
            throw new RuntimeException(fatal);
        }

    }

    /**
     * Delete a Kafka topic.
     *
     * @param topic
     *         The name of the topic.
     */
    public void deleteTopic(final String topic) {
        LOGGER.debug("Deleting topic {}", topic);
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

        try (final AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (final InterruptedException e) {
            LOGGER.error("InterruptedException occurred when attempting to delete topics", e);
            throw new RuntimeException(e);
        } catch (final ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                LOGGER.error("ExecutionException occurred when attempting to delete topics", e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Kafka server.
     *
     * @return the kafka server
     */
    KafkaServer kafkaServer() {
        return kafka;
    }
}