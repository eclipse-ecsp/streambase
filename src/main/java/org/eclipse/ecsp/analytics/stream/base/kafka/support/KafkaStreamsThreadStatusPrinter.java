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

package org.eclipse.ecsp.analytics.stream.base.kafka.support;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.ThreadMetadata;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.ThreadUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Prints status of stream threads and their allocated tasks.
 *
 * @author ssasidharan
 */
@Component
public class KafkaStreamsThreadStatusPrinter {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaStreamsThreadStatusPrinter.class);

    /** The thread metadata logger enabled. */
    @Value("${print.threads.metadata.enabled:true}")
    private boolean threadMetadataLoggerEnabled = true;
    
    /** The interval. */
    @Value("${print.threads.metadata.interval.ms:30000}")
    private long interval = 100;
    
    /** The service name. */
    @Value("${service.name:}")
    private String serviceName = "stream-base";
    
    /** The node name. */
    @Value("${" + PropertyNames.NODE_NAME + ":}")
    private String nodeName = "localhost";
    
    /** The stream thread alive states. */
    @Value("#{'${stream.threads.active.states}'.split(',')}")
    private List<String> streamThreadAliveStates = new ArrayList<>(
            Arrays.asList("CREATED", "STARTING", "PARTITIONS_REVOKED",
                    "PARTITIONS_ASSIGNED", "RUNNING", "PENDING_SHUTDOWN"));
    
    /** The stream thread dead states. */
    @Value("#{'${stream.threads.dead.states}'.split(',')}")
    private List<String> streamThreadDeadStates = new ArrayList<>(
            Arrays.asList("DEAD"));
    
    /** The enable prometheus. */
    @Value("${" + PropertyNames.ENABLE_PROMETHEUS + "}")
    private boolean enablePrometheus = true;
    
    /** The total stream threads. */
    @Value("${" + PropertyNames.NUM_STREAM_THREADS + ":1}")
    private Integer totalStreamThreads = 1;
    
    /** The exec. */
    private ScheduledExecutorService exec;
    
    /** The kafka streams. */
    private KafkaStreams kafkaStreams;
    
    /** The thread liveness metrics. */
    private volatile Gauge threadLivenessMetrics;
    
    /** The total thread metrics. */
    private volatile Gauge totalThreadMetrics;

    /**
     * init(): to initialize the properties.
     *
     * @param ks ks
     */

    public void init(KafkaStreams ks) {
        if (enablePrometheus) {

            threadLivenessMetrics = Gauge
                    .build("thread_liveness", "Track stream-processor alive stream thread count")
                    .labelNames("service", "node").register(CollectorRegistry.defaultRegistry);

            totalThreadMetrics = Gauge.build("total_thread", "Track total thread stream-processor started")
                    .labelNames("service", "node").register(CollectorRegistry.defaultRegistry);

        }

        if (threadMetadataLoggerEnabled || enablePrometheus) {
            kafkaStreams = ks;
            exec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setName("thread-status-printer");
                return t;
            });
            exec.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    logAndUpdateMetrics();
                }

                private void logAndUpdateMetrics() {
                    try {
                        logThreadMetadata(kafkaStreams.metadataForLocalThreads());
                        updateStreamThreadMetrics(kafkaStreams.metadataForLocalThreads());
                    } catch (Exception e) {
                        logger.error("Exception in printing threads metadata", e);
                        if (kafkaStreams.state() == KafkaStreams.State.ERROR) {
                            logger.info("KafkaStream in ERROR state. Terminating thread-status-printer");
                            close();
                        }
                    }
                }

                private void updateStreamThreadMetrics(Set<ThreadMetadata> tmSet) {
                    if (!enablePrometheus) {
                        return;
                    }
                    long aliveThreads = tmSet.stream().filter(
                                    tm -> streamThreadAliveStates.contains(tm.threadState())
                                            && !streamThreadDeadStates.contains(tm.threadState()))
                            .count();
                    threadLivenessMetrics.labels(serviceName, nodeName).set(aliveThreads);
                    totalThreadMetrics.labels(serviceName, nodeName).set(totalStreamThreads);

                }

            }, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Log thread metadata.
     *
     * @param tmSet the tm set
     */
    private void logThreadMetadata(Set<ThreadMetadata> tmSet) {
        if (!threadMetadataLoggerEnabled) {
            return;
        }
        for (ThreadMetadata tm : tmSet) {
            logger.info("Thread {} is in state {} with activeTasks: [{}] and standbyTasks [{}]",
                    tm.threadName(), tm.threadState(), tm.activeTasks(), tm.standbyTasks());
        }

    }

    /**
     * close(): close the opened resources.
     */
    public void close() {
        if (threadMetadataLoggerEnabled || enablePrometheus) {
            ThreadUtils.shutdownExecutor(exec,
                    Constants.THREAD_SLEEP_TIME_10000, false);
        }
    }
}
