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

package org.eclipse.ecsp.analytics.stream.base.metrics.reporter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.eclipse.ecsp.analytics.stream.base.context.StreamBaseSpringContext;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class that will write the metrics on to the console.
 * Which will be added to KafkaStream reporter You need to add the property
 * metrics.reporter=org.eclipse.ecsp.analytics.stream.base.metrics.reporter.
 * ConsoleMetricReporter in your application properties file
 */
@Component
public class ConsoleMetricReporter implements MetricsReporter {
    
    /** The Constant LOCK. */
    private static final Object LOCK = new Object();
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(ConsoleMetricReporter.class);
    
    /** The metric list. */
    List<KafkaMetric> metricList = new ArrayList<>();
    
    /** The Constant JSON_MAPPER. */
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    static {
        JSON_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Configure.
     *
     * @param configs the configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        //overridden method
    }

    /**
     * Inits the.
     *
     * @param metrics the metrics
     */
    @Override
    public void init(List<KafkaMetric> metrics) {

        synchronized (LOCK) {
            metricList = metrics;
            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                return t;
            });
            exec.scheduleWithFixedDelay(() -> {
                try {
                    printMetrics();
                } catch (Exception e) {
                    logger.error("Exception while printing the metrics", e);
                }
            }, Constants.FOUR, Constants.SIXTY, TimeUnit.MINUTES);
        }

    }

    /**
     * Prints the metrics.
     */
    private void printMetrics() {
        HarmanRocksDBMetricsExporter exporter;
        synchronized (LOCK) {
            logger.debug("Printing Metrics:");
            for (KafkaMetric metric : metricList) {
                try {

                    Map<String, Object> kv = new HashMap<>();
                    kv.put("metricName", metric.metricName().name());
                    kv.put("groupName", metric.metricName().group());
                    kv.put("metricValue", metric.metricValue());
                    kv.put("description", metric.metricName().description());
                    kv.putAll(metric.config().tags());
                    logger.info("Printing metrics : {}", JSON_MAPPER.writeValueAsString(kv));
                } catch (JsonProcessingException e) {
                    logger.error("Unable to print the metrics for {} ", metric.metricName().name());
                }
            }
            if (!metricList.isEmpty() && metricList.stream()
                    .anyMatch(e -> e.metricName().name().equalsIgnoreCase("bytes-written-rate"))) {
                exporter = StreamBaseSpringContext.getBean(HarmanRocksDBMetricsExporter.class);
                logger.info("Fetched bean: {} from spring context", exporter.getClass().getName());
                exporter.publishMetrics(metricList);
            }
        }
    }

    /**
     * Metric change.
     *
     * @param metric the metric
     */
    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (LOCK) {
            logger.debug("Registering metric ! Name:{}, group:{},client-id:{},value:{}", 
                    metric.metricName().name(), metric.metricName().group(), 
                    metric.config().tags().get("client-id"), metric.metricValue().toString());
            if (metricList.contains(metric)) {
                metricList.remove(metric);
            }
            metricList.add(metric);
        }
    }

    /**
     * Metric removal.
     *
     * @param metric the metric
     */
    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (LOCK) {
            logger.debug("Removing metric:{}", metric.metricName().name());
            metricList.remove(metric);
        }

    }

    /**
     * Close.
     */
    @Override
    public void close() {
        //overridden method
    }

}
