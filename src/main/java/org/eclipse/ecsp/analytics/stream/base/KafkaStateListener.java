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


import io.prometheus.client.Counter;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaConsumer;
import org.eclipse.ecsp.analytics.stream.base.offset.OffsetManager;
import org.eclipse.ecsp.healthcheck.HealthMonitor;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * An implementation of {@link KafkaStreams.StateListener} and
 * {@link HealthMonitor} that monitors the state of {@link KafkaStreams} and
 * takes appropriate actions based on state changes.
 *
 * <p>Key functionalities include:
 * <ul>
 * <li>Restarting {@link BackdoorKafkaConsumer} instances when the state changes
 * to RUNNING.</li>
 * <li>Monitoring the REBALANCING state and restarting the application if it
 * persists for too long.</li>
 * <li>Notifying {@link OffsetManager} to repopulate offsets from MongoDB.</li>
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

    /** The {@link OffsetManager} instance. */
    @Autowired
    private OffsetManager offsetManager;

    /** The Spring's ApplicationContext. */
    @Autowired
    private ApplicationContext applicationContext;

    /** Whether DMA is enabled or not. */
    @Value("${" + PropertyNames.DMA_ENABLED + ":true}")
    private boolean isDmaEnabled;
    
    /** The name of the service. */
    @Value("${" + PropertyNames.SERVICE_NAME + "}")
    private String serviceName;
    
    /**
     * Flag to enable prometheus metrics.
     */
    @Value("${" + PropertyNames.ENABLE_PROMETHEUS + ":true}")
    private boolean prometheusEnabled;

    /** Whether KafkaConsumer group health monitor is enabled or not. */
    @Value("${health.kafka.consumer.group.monitor.enabled:true}")
    protected boolean healthMonitorEnabled;

    /** The Constant GROUP_HEALTH_MONITOR. */
    protected static final String GROUP_HEALTH_MONITOR = "KAFKA_CONSUMER_GROUP_HEALTH_MONITOR";

    /** The Constant GROUP_HEALTH_GUAGE. */
    protected static final String GROUP_HEALTH_GUAGE = "KAFKA_CONSUMER_GROUP_HEALTH_GUAGE";
    
    /** The Constant KAFKA_STREAMS_THREAD_STATE_GAUGE. */
    protected static final String KAFKA_STREAMS_THREAD_STATE_COUNTER = "KAFKA_STREAMS_THREAD_STATE_COUNTER";

    /** Indicates the health status reported by this health monitor. */
    private volatile boolean healthy;
    
    /**
     * Prometheus counter to keep count of the state of Kafka Streams threads.
     * This counter will be used to report and count the state of each Kafka Streams thread
     * with labels as service name, thread name, state and last updated timestamp.
     */
    private Counter streamsThreadStateCounter;
    
    /**
     * Initializes the Prometheus counter for Kafka Streams thread state.
     */
    @PostConstruct
    public void init() {
        streamsThreadStateCounter = Counter.build()
                .name(KAFKA_STREAMS_THREAD_STATE_COUNTER)
                .help("Kafka Streams Thread State")
                .labelNames("service_name", "thread_name", "state", "last_updated_timestamp")
                .register();
        logger.info("Initialized Prometheus gauge for Kafka Streams thread state.");
    }

    /**
     * Instantiates a new kafka state listener.
     */
    public KafkaStateListener() {
        // default constructor
    }

    /**
     * Handles state changes in the {@link KafkaStreams} instance.
     *
     * <p>This method performs the following actions:
     * <ul>
     * <li>Updates the health status based on the new state.</li>
     * <li>Monitors the NON RUNNING states and publishes respective state metrics to Prometheus.</li>
     * <li>Notifies {@link OffsetManager} to repopulate offsets when
     * transitioning from REBALANCING to RUNNING.</li>
     * <li>Invokes any registered {@link KafkaStateAgentListener} instances when
     * transitioning from REBALANCING to RUNNING.</li>
     * </ul>
     *
     * @param newState
     *            the new state of the {@link KafkaStreams}.
     * @param oldState
     *            the previous state of the {@link KafkaStreams}.
     */
    @Override
    public void onChange(State newState, State oldState) {
        if (State.RUNNING == newState) {
            healthy = true;
        } else {
            healthy = false;
        }
        logger.info("Stream state changed from {} to {}", oldState, newState);
        if (State.REBALANCING == newState || State.ERROR == newState || State.NOT_RUNNING == newState) {
            String streamsThreadName = Thread.currentThread().getName();
            logger.error("Streams thread: {} is in {} state!", streamsThreadName, newState.toString());
            
            if (prometheusEnabled) {
                switch (newState) {
                    case REBALANCING:
                        streamsThreadStateCounter.labels(serviceName, streamsThreadName, 
                                State.REBALANCING.toString(), System.currentTimeMillis() + "")
                                .inc();
                        break;
                    case ERROR:
                        streamsThreadStateCounter.labels(serviceName, streamsThreadName, 
                                State.ERROR.toString(), System.currentTimeMillis() + "")
                                .inc();
                        break;
                    case NOT_RUNNING:
                        streamsThreadStateCounter.labels(serviceName, streamsThreadName, 
                                State.NOT_RUNNING.toString(), System.currentTimeMillis() + "")
                                .inc();
                        break;
                    default:
                        logger.error("Unknown Kafka Streams state!");
                }        
            }
        }
        if (State.REBALANCING == oldState && State.RUNNING == newState) {
            Map<String, KafkaStateAgentListener> kafkaAgentListeners = applicationContext
                    .getBeansOfType(KafkaStateAgentListener.class);

            kafkaAgentListeners.values().forEach(listner -> listner.onChange(newState, oldState));
            offsetManager.setUp();
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
     * @param arg0
     *            the arg 0
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
     * Name of the Prometheus Guage under which these health metrics will be
     * captured.
     *
     * @return the name of the Guage.
     */
    @Override
    public String monitorName() {
        return GROUP_HEALTH_MONITOR;
    }

    /**
     * Always return false because streambase library should not take this important decision of restarting 
     * the whole service and introduce downtime, just because stream thread(s) went into NON_RUNNING state.
     * This decision should be taken by the service owner / ops itself based on its requirements.
     *
     * @return false.
     */
    @Override
    public boolean needsRestartOnFailure() {
        return false;
    }
}
