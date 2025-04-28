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

package org.eclipse.ecsp.analytics.stream.base.healthcheck;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.eclipse.ecsp.analytics.stream.base.KafkaSslConfig;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.exception.UnableToReadFileException;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.healthcheck.HealthMonitor;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.eclipse.ecsp.analytics.stream.base.utils.Constants.FOR_PARTITION;

/**
 * KafkaTopicsHealthMonitor is responsible for the checking the health of kafka topics.
 * If the topics' configuration is not as expected, then health status will be returned as "unhealthy".
 * Check {@link #isHealthy(boolean)} for the basis of health check.
 */
@Service
public class KafkaTopicsHealthMonitor implements HealthMonitor {

    /** The Constant PROPS. */
    private static final Properties PROPS = new Properties();
    
    /** The bootstrap server URLs to connect to Kafka broker. */
    @Value("${" + PropertyNames.BOOTSTRAP_SERVERS + ":}")
    private String bootstrapServer;

    /** Whether to restart on getting unhealthy status by this health monitor. */
    @Value("${" + PropertyNames.KAFKA_TOPICS_NEEDS_RESTART_ON_FAILURE + ":false}")
    private boolean needsRestartOnFailure;
    
    /** IF this health monitor is enabled or not. */
    @Value("${" + PropertyNames.ENABLE_HEALTHCHECK + ":false}")
    private boolean enabled;
    
    /** The kafka topic file which contains topics configuration like number of partitions, replication factor etc. */
    @Value("${" + PropertyNames.KAFKA_TOPICS_FILE_PATH + ":/data/topics.txt}")
    private String kafkaTopicFile;
    
    /** The connections max idle ms. */
    @Value("${" + PropertyNames.CONNECTIONS_MAX_IDLE_MS + ":-1}")
    private String connectionsMaxIdleMs;
    
    /** The kafka ssl config. */
    @Autowired
    private KafkaSslConfig kafkaSslConfig;

    /** The props. */
    private Properties props = new Properties();

    /** The topic config. */
    private Map<String, String[]> topicConfig;

    /** The topics. */
    private Set<String> topics;

    /** The {@link AdminClient} instance. */
    private AdminClient admin;

    /** The logger. */
    private IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaTopicsHealthMonitor.class);
    
    /**
     * Initializes properties for this Health monitor class.
     */
    @PostConstruct
    public void init() {
        PROPS.put(PropertyNames.BOOTSTRAP_SERVERS, bootstrapServer);
        PROPS.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
        kafkaSslConfig.setSslPropsIfEnabled(props);
        logger.debug("Admin client config for topics health monitor {}", PROPS);
        topicConfig = getTopicsConfig();
        topics = topicConfig.keySet();
        logger.info("Creating admin client with properties : {}", props);
        admin = AdminClient.create(PROPS);
        logger.info("admin client created - {}", admin.getClass());
        logger.info("Get topic descriptions for topics {}", topics);
    }

    /**
     * The logic for kafka topics healthcheck is based on whether the topics that have been created have 
     * the expected configuration or not.
     *
     * @param forceHealthCheck if to perform force health check
     * @return true, if it is healthy
     */
    @Override
    public boolean isHealthy(boolean forceHealthCheck) {
        boolean flag = true;
        String[] topicConfigured = null;
        Map<String, KafkaFuture<TopicDescription>> topicPartitionMap = null;
        try {
            DescribeTopicsResult topicResult = admin.describeTopics(topics);
            topicPartitionMap = topicResult.topicNameValues();
        } catch (Exception e) {
            logger.error("Kafka topics monitor unabe to describe topics", e);
        }
        KafkaFuture<TopicDescription> topicDescription = null;
        List<TopicPartitionInfo> topicPartitionInfoList = null;
        StringBuilder errBuilder = null;
        StringBuilder warnBuilder = null;
        int expectedPartitionSize = 0;
        int configuredReplicationFactor = 0;
        int expectedMinIsr = 0;
        for (String topic : topics) {
            errBuilder = new StringBuilder();
            warnBuilder = new StringBuilder();
            topicDescription = topicPartitionMap.get(topic);
            topicConfigured = topicConfig.get(topic);

            try {
                topicPartitionInfoList = topicDescription.get().partitions();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Kafka topics monitor  error : Unable to fetch partitions {}", e);
                return false;
            }
            expectedPartitionSize = Integer.parseInt(topicConfigured[1]);
            configuredReplicationFactor = Integer.parseInt(topicConfigured[Constants.TWO]);
            expectedMinIsr = configuredReplicationFactor - 1;
            // Expected ISR should be atleast 1.
            expectedMinIsr = (expectedMinIsr >= 1) ? expectedMinIsr : 1;
            int partitionSizeFromKafka = topicPartitionInfoList.size();
            if (expectedPartitionSize != partitionSizeFromKafka) {
                flag = false;
                errBuilder.append("Partiton mismatch :: expectedPartitionSize=").append(expectedPartitionSize)
                        .append(", partitionSizeFromKafka=").append(partitionSizeFromKafka).append("\n");
            }
            for (TopicPartitionInfo partition : topicPartitionInfoList) {
                flag = checkFlagForPartions(flag, errBuilder, warnBuilder,
                        configuredReplicationFactor, expectedMinIsr, partition);
            }
            String error = errBuilder.toString();
            String warn = warnBuilder.toString();
            if (error.length() > 0) {
                logger.error("For topic : " + topic + ", following needs to be fixed \n" + error);
            }
            if (warn.length() > 0) {
                logger.warn("For topic : " + topic + ", following may be a potential issue \n" + warn);
            }
        }

        return flag;
    }

    /**
     * Check flag for partions.
     *
     * @param flag the flag
     * @param errBuilder the err builder
     * @param warnBuilder the warn builder
     * @param configuredReplicationFactor the configured replication factor
     * @param expectedMinIsr the expected min isr
     * @param partition the partition
     * @return true, if successful
     */
    private static boolean checkFlagForPartions(boolean flag, StringBuilder errBuilder, 
            StringBuilder warnBuilder, int configuredReplicationFactor,
            int expectedMinIsr, TopicPartitionInfo partition) {
        List<Node> isrList;
        isrList = partition.isr();
        int actualReplicationFactor = partition.replicas().size();
        Node partitionLeader = partition.leader();
        int partitionLeaderId = partitionLeader == null ? Constants.NEGATIVE_ONE : partition.leader().id();
        int actualIsr = isrList.size();
        int partitionId = partition.partition();
        if (actualReplicationFactor < configuredReplicationFactor) {
            warnBuilder.append(FOR_PARTITION).append(partitionId)
                    .append(":: Actual number of replicas ")
                    .append(actualReplicationFactor).append(" is less than configured replication factor ")
                    .append(configuredReplicationFactor).append("\n");
        }
        if (partitionLeaderId < 0 || actualIsr < expectedMinIsr
                || actualReplicationFactor < expectedMinIsr) {
            flag = false;
            errBuilder.append(FOR_PARTITION).append(partitionId)
                    .append(" :: Leader not assigned (or) expected Replication factor is not available "
                            + "(or) actual isr less than expected isr . Partition leader=")
                    .append(partitionLeaderId)
                    .append(", actualReplicationFactor=").append(actualReplicationFactor)
                    .append(", replication=").append(actualReplicationFactor)
                    .append(", actualISR=").append(actualIsr)
                    .append(", expectedMinIsr=").append(expectedMinIsr).append("\n");

        }
        boolean isrLeaderFlag = false;
        for (Node node : isrList) {
            if (partitionLeaderId == node.id()) {
                isrLeaderFlag = true;
                break;
            }
        }
        if (!isrLeaderFlag) {
            flag = false;
            errBuilder.append(FOR_PARTITION).append(partitionId).append(" :: leader ")
                    .append(partitionId).append(" is not available in isr list");
        }
        return flag;
    }

    /**
     * Name of the health monitor.
     *
     * @return the string
     */
    @Override
    public String monitorName() {
        return Constants.KAFKA_TOPICS_HEALTH_MONITOR;
    }

    /**
     * Needs restart on failure.
     *
     * @return true, if successful
     */
    @Override
    public boolean needsRestartOnFailure() {
        return needsRestartOnFailure;
    }

    /**
     * Metric name under which health metrics will be published.
     *
     * @return the string
     */
    @Override
    public String metricName() {
        return Constants.KAFKA_TOPICS_METRIC_NAME;
    }

    /**
     * Checks if is enabled.
     *
     * @return true, if is enabled
     */
    @Override
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Gets the topics config.
     *
     * @return the topics config
     */
    private Map<String, String[]> getTopicsConfig() {
        Map<String, String[]> topicInfoConfigMap = new HashMap<>();
        if (isEnabled()) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(kafkaTopicFile));
                String line = null;
                String[] params = null;
                while ((line = br.readLine()) != null) {
                    params = line.split(Constants.DELIMITER);
                    topicInfoConfigMap.put(params[0], params);
                }
                br.close();
                if (topicInfoConfigMap.size() == 0) {
                    logger.error("No topics configured in the mounted path {}", kafkaTopicFile);
                }
            } catch (Exception e) {
                logger.error(
                        "Kafka topics monitor : Error while reading topics.txt. "
                                + "Please check if /data has been mounted in the container",
                        e);
                throw new UnableToReadFileException(
                        "Error while reading topics.txt. Please check if "
                                + kafkaTopicFile + " has been mounted in the container", e);
            }
        } else {
            logger.info("Kafka topics monitor is disabled. Topics config will not be fetched from data volume");
        }
        return topicInfoConfigMap;
    }

}
