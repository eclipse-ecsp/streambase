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

import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.ecsp.utils.metrics.IgniteRocksDBGuage;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.util.List;

/**
 * Fetches RocksDB's property based metrics from the RocksDB library
 * through a Scheduled thread executor at configured time interval
 * and sets the value of each metric to its corresponding Prometheus guage.
 *
 * @author hbadshah
 */
@Component
@ConditionalOnProperty(name = PropertyNames.ROCKSDB_METRICS_ENABLED, havingValue = "true")
@EnableScheduling
public class HarmanRocksDBMetricsExporter {
    
    /** The Constant LOGGER. */
    private static final IgniteLogger LOGGER = IgniteLoggerFactory.getLogger(HarmanRocksDBMetricsExporter.class);
    
    /** The rocks db metrics enabled. */
    @Value("${" + PropertyNames.ROCKSDB_METRICS_ENABLED + ":false}")
    private boolean rocksDbMetricsEnabled;

    /** The rocks db metrics list. */
    @Value("#{'${" + PropertyNames.ROCKSDB_METRICS_LIST + "}'.split(',')}")
    private List<String> rocksDbMetricsList;

    /** The prometheus enabled. */
    @Value("${" + PropertyNames.ENABLE_PROMETHEUS + "}")
    private boolean prometheusEnabled;

    /** The svc. */
    @Value("${" + PropertyNames.SERVICE_NAME + "}")
    private String svc;

    /** The node name. */
    @Value("${NODE_NAME:localhost}")
    private String nodeName;

    /** The rocksdb guage. */
    @Autowired
    private IgniteRocksDBGuage rocksdbGuage;

    /** The db. */
    private RocksDB db;

    /** The is valid list. */
    private boolean isValidList = false;

    /**
     * init().
     */
    @PostConstruct
    public void init() {
        if (prometheusEnabled && rocksDbMetricsEnabled) {
            LOGGER.info("RocksDB metrics is enabled");
            if (rocksDbMetricsList == null || rocksDbMetricsList.isEmpty()) {
                LOGGER.info("RocksDB metrics is enabled but rocksdb.metrics.list is empty."
                        + " None of the RocksDB metrics will be exported to Prometheus.");
                return;
            }
            rocksdbGuage.setup();
            prefix();
            isValidList = true;
        }
    }

    /**
     * RocksDB accepts the name of the metric in the format: "rocksdb.{@code <}metric_name{@code >}
     * For example: if metricName = compaction-pending, then to get this
     * metric's value from RocksDB, we must convert it into the format =
     * rocksdb.compaction-pending.
     * Therefore this method prefixes each metric name with "rocksdb."
     * in the input {@link HarmanRocksDBMetricsExporter#rocksDBMetricsList}
     *
     */
    private void prefix() {
        rocksDbMetricsList = rocksDbMetricsList.stream().map(property -> Constants.ROCKSDB_PREFIX + property)
                .toList();
        LOGGER.info("Modified rocksdb.metrics.list is: {}", rocksDbMetricsList);
    }

    /**
     * This method fetches each metric name one by one and gets its value from
     * the RocksDB's instance that's been initialized and opened in {@code HarmanRocksDBStore}
     * The value fetched above is then set to the corresponding Prometheus guage by the same name.
     */
    @Scheduled(fixedDelayString = "${" + PropertyNames.ROCKSDB_METRICS_THREAD_FREQUENCY_MS + "}",
            initialDelayString = "${" + PropertyNames.ROCKSDB_METRICS_THREAD_INITIAL_DELAY_MS + "}")
    public void fetchMetrics() {
        if (db != null && isValidList) {
            int i = 0;
            while (true) {
                if (i == rocksDbMetricsList.size()) {
                    break;
                }
                try {
                    String metricName = rocksDbMetricsList.get(i);
                    i++;
                    long val = db.getLongProperty(metricName);
                    LOGGER.info("Got metrics: {} value: {} from RocksDB", metricName, val);
                    rocksdbGuage.set(val, metricName, svc, nodeName);
                } catch (RocksDBException exception) {
                    LOGGER.error("Unable to fetch metrics for property: {}, exception is {}. Exception status is {}");
                }
            }
        } else {
            LOGGER.debug("Either RocksDB instance is null or RocksDB metrics "
                    + "list not populated correctly. Unable to publish RocksDB metrics");
        }
    }

    /**
     * Sets the rocks db.
     *
     * @param db the new rocks db
     */
    public void setRocksDb(RocksDB db) {
        this.db = db;
    }

    /**
     *  Sets the value of each metric in the metricList which will subsequently publish the metrics' 
     *  value to prometheus.
     *
     * @param metricList List of metrics to publish.
     */
    public void publishMetrics(List<KafkaMetric> metricList) {
        if (prometheusEnabled) {
            LOGGER.debug("Publishing Rocks db metrics to prometheus");
            metricList.stream().filter(e -> e.metricName().group().equalsIgnoreCase("stream-state-metrics"))
                    .forEach(e -> rocksdbGuage.set(e.metricValue() instanceof Double metricValue ? metricValue
                            : ((BigInteger) e.metricValue()).doubleValue(), e.metricName().name(), svc, nodeName));
        }
    }
}
