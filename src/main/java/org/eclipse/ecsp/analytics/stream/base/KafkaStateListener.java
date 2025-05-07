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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaConsumer;
import org.eclipse.ecsp.analytics.stream.base.offset.OffsetManager;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.healthcheck.HealthMonitor;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link KafkaStreams.StateListener} and {@link HealthMonitor} that monitors
 * the state of {@link KafkaStreams} and takes appropriate actions based on state changes.
 *
 * <p>Key functionalities include:
 * <ul>
 *   <li>Restarting {@link BackdoorKafkaConsumer} instances when the state changes to RUNNING.</li>
 *   <li>Monitoring the REBALANCING state and restarting the application if it persists for too long.</li>
 *   <li>Notifying {@link OffsetManager} to repopulate offsets from MongoDB.</li>
 * </ul>
 */
@Component
public class KafkaStateListener implements KafkaStreams.StateListener, HealthMonitor {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaStateListener.class);

    /** How long to monitor the KafkaStreams state. */
    @Value("${" + PropertyNames.KAFKA_REBALANCE_TIME_MINS + ":10}")
    private int timeToRebalance;
    
    /** The timeout before closing the KafkaStreams. */
    @Value("${" + PropertyNames.KAFKA_CLOSE_TIMEOUT_SECS + ":30}")
    private int closeTimeout;
    
    /** Indicates whether the KafkaStreams is in rebalancing state or not. */
    private volatile boolean balancing;
    
    /** The {@link KafkaStreams} instance. */
    private KafkaStreams streams;
    
    /** The list of {@link BackdoorKafkaConsumer} to be restarted if KafkaStreams state changes 
     * from anything to RUNNING. */
    @Autowired
    private List<BackdoorKafkaConsumer> backdoorConsumers;
    
    /** The {@link OffsetManager} instance. */
    @Autowired
    private OffsetManager offsetManager;
    
    /** The Spring's ApplicationContext. */
    @Autowired
    private ApplicationContext applicationContext;

    /** Whether DMA is enabled or not. */
    @Value("${" + PropertyNames.DMA_ENABLED + ":true}")
    private boolean isDmaEnabled;

    /** Whether KafkaConsumer group health monitor is enabled or not. */
    @Value("${health.kafka.consumer.group.monitor.enabled:true}")
    protected boolean healthMonitorEnabled;

    /** Whether to restart the KafkaStreams if KafkaConsumer group health monitor reports UNHEALTHY. */
    @Value("${health.kafka.consumer.group.needs.restart.on.failure:false}")
    protected boolean needsRestartOnFailure;

    /** The Constant GROUP_HEALTH_MONITOR. */
    protected static final String GROUP_HEALTH_MONITOR = "KAFKA_CONSUMER_GROUP_HEALTH_MONITOR";
    
    /** The Constant GROUP_HEALTH_GUAGE. */
    protected static final String GROUP_HEALTH_GUAGE = "KAFKA_CONSUMER_GROUP_HEALTH_GUAGE";
    
    /** Indicates the health status reported by this health monitor. */
    private volatile boolean healthy;

    /**
     * Instantiates a new kafka state listener.
     */
    public KafkaStateListener() {
        //default constructor
    }

    /**
     * Sets the streams.
     *
     * @param streams the new streams
     */
    public void setStreams(KafkaStreams streams) {
        this.streams = streams;
    }

    /**
     * Should be used only in testcases.
     *
     * @param backdoorConsumers backdoorConsumers
     */
    void setBackdoorConsumers(List<BackdoorKafkaConsumer> backdoorConsumers) {
        this.backdoorConsumers = backdoorConsumers;
    }

    /**
     * Handles state changes in the {@link KafkaStreams} instance.
     *
     * <p>This method performs the following actions:
     * <ul>
     *   <li>Notifies all {@link BackdoorKafkaConsumer} instances about the state change.</li>
     *   <li>Updates the health status based on the new state.</li>
     *   <li>Monitors the REBALANCING state and starts a thread to validate if it persists too long.</li>
     *   <li>Notifies {@link OffsetManager} to repopulate offsets when transitioning from REBALANCING to RUNNING.</li>
     *   <li>Invokes any registered {@link KafkaStateAgentListener} instances when
     *   transitioning from REBALANCING to RUNNING.</li>
     * </ul>
     *
     * @param newState the new state of the {@link KafkaStreams}.
     * @param oldState the previous state of the {@link KafkaStreams}.
     */
    @Override
    public void onChange(State newState, State oldState) {

        backdoorConsumers.forEach(backdoorConsumer ->
            backdoorConsumer.setStreamState(newState)
        );
        if (State.RUNNING == newState) {
            healthy = true;
        } else {
            healthy = false;
        }
        logger.info("Stream state changed from {} to {}", oldState, newState);
        if (State.REBALANCING == newState || State.ERROR == newState || State.NOT_RUNNING == newState) {
            if (!balancing) {
                balancing = true;
                Thread rebalanceMonitor = new Thread(() -> {
                    try {
                        Thread.sleep(timeToRebalance * Constants.LONG_60000);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted exception occurred when waiting for REBALANCING to complete. "
                                + "Error - {}", e);
                        Thread.currentThread().interrupt();
                    }
                    validateStateBalancing();
                });
                rebalanceMonitor.setName("IgniteKafkaStateListener:" + Thread.currentThread().getName());
                rebalanceMonitor.setDaemon(true);
                rebalanceMonitor.start();
            }
        } else {
            balancing = false;
        }
        if (State.REBALANCING == oldState && State.RUNNING == newState) {
            Map<String, KafkaStateAgentListener> kafkaAgentListeners =
                    applicationContext.getBeansOfType(KafkaStateAgentListener.class);

            kafkaAgentListeners.values().forEach(listner ->
                listner.onChange(newState, oldState)
            );
            offsetManager.setUp();
        }
    }

    /**
     * Checks if the KafkaStreams has been rebalancing for more than 10 mins. If yes, then close the 
     * current streams instance and restart the application.
     */
    private void validateStateBalancing() {
        if (balancing) {
            logger.error("I have been rebalancing or in error state for last {} minutes. Exiting the JVM to restart.",
                    timeToRebalance);
            if (streams.close(Duration.ofSeconds(closeTimeout))) {
                logger.error("All threads were successfully stopped, Streams closed");
            } else {
                logger.error("Streams closed after time out of {} seconds.", closeTimeout);
            }
            System.exit(1);
        } else {
            logger.info("Stream back to normal");
        }
    }

    /**
     * Returns true if the health monitor for KafkaConsumer group is enabled.
     *
     * @return true, if is enabled
     */
    @Override
    public boolean isEnabled() {
        return healthMonitorEnabled;
    }

    /**
     * Returns true if the health monitor for KafkaConsumer group is HEALTHY.
     *
     * @param arg0 the arg 0
     * @return true, if is healthy
     */
    @Override
    public boolean isHealthy(boolean arg0) {
        return healthy;
    }

    /**
     * Metric name.
     *
     * @return the string
     */
    @Override
    public String metricName() {
        return GROUP_HEALTH_GUAGE;
    }

    /**
     * Name of the Prometheus Guage under which these health metrics will be captured.
     *
     * @return the name of the Guage.
     */
    @Override
    public String monitorName() {
        return GROUP_HEALTH_MONITOR;
    }

    /**
     * Returns true if the application Needs to be restarted on UNHEALTHY health status reported by 
     * this health monitor.
     *
     * @return true, if to be restarted.
     */
    @Override
    public boolean needsRestartOnFailure() {
        return needsRestartOnFailure;
    }
}
