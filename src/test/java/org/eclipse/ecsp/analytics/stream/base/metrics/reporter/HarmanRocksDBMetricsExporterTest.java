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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.ThreadUtils;
import org.eclipse.ecsp.utils.metrics.IgniteRocksDBGuage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * Test class for {@link HarmanRocksDBMetricsExporter}.
 */
public class HarmanRocksDBMetricsExporterTest {
    
    /** The exporter. */
    @InjectMocks
    private HarmanRocksDBMetricsExporter exporter = new HarmanRocksDBMetricsExporter();

    /** The rocksdb guage. */
    private IgniteRocksDBGuage rocksdbGuage = mock(IgniteRocksDBGuage.class);

    /** The db. */
    @Mock
    private RocksDB db;

    /** The exception. */
    @Mock
    private RocksDBException exception;

    /** The status. */
    @Mock
    private Status status;

    /** The utils. */
    @Mock
    private ThreadUtils utils;

    /**
     * Test init.
     */
    @Test
    public void testInit() {
        ReflectionTestUtils.setField(exporter, "prometheusEnabled", true);
        ReflectionTestUtils.setField(exporter, "rocksDBMetricsEnabled", true);
        List<String> metricsList = new ArrayList<>();
        metricsList.add("compaction-pending");
        metricsList.add("background-errors");
        metricsList.add("size-all-mem-tables");
        ReflectionTestUtils.setField(exporter, "rocksDBMetricsList", metricsList);
        ReflectionTestUtils.setField(exporter, "rocksdbGuage", rocksdbGuage);
        ReflectionTestUtils.invokeMethod(exporter, "init");

        Mockito.verify(rocksdbGuage, Mockito.times(1)).setup();
        metricsList = (List<String>) ReflectionTestUtils.getField(exporter, "rocksDBMetricsList");
        metricsList.forEach(metricName -> Assert.assertTrue(metricName.startsWith("rocksdb.")));
    }

    /**
     * Test fetch metrics.
     *
     * @throws RocksDBException the rocks DB exception
     */
    @Test
    public void testFetchMetrics() throws RocksDBException {
        List<String> metricsList = new ArrayList<>();
        metricsList.add("compaction-pending");
        metricsList.add("background-errors");
        metricsList.add("size-all-mem-tables");
        ReflectionTestUtils.setField(exporter, "rocksDBMetricsList", metricsList);
        ReflectionTestUtils.invokeMethod(exporter, "prefix");

        db = Mockito.mock(RocksDB.class);
        ReflectionTestUtils.setField(exporter, "rocksdbGuage", rocksdbGuage);
        ReflectionTestUtils.invokeMethod(exporter, "setRocksDB", db);
        ReflectionTestUtils.setField(exporter, "isValidList", true);
        exporter.fetchMetrics();

        Mockito.verify(db, Mockito.times(Constants.THREE)).getLongProperty(Mockito.anyString());
        Mockito.verify(rocksdbGuage, Mockito.times(Constants.THREE)).set(Mockito.anyDouble(), Mockito.any());
    }

    /**
     * Test publish metrics.
     */
    @Test
    public void testPublishMetrics() {
        ReflectionTestUtils.setField(exporter, "prometheusEnabled", true);
        ReflectionTestUtils.setField(exporter, "rocksdbGuage", rocksdbGuage);

        Measurable measurable = new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return 0;
            }
        };
        KafkaMetric metric1 = new KafkaMetric(new Object(),
                new MetricName("test", "stream-state-metrics", "test", new HashMap<>()),
                measurable, new MetricConfig(), Time.SYSTEM);
        List<KafkaMetric> kafkaMetrics = Arrays.asList(metric1);
        exporter.publishMetrics(kafkaMetrics);
        verify(rocksdbGuage).set(anyDouble(), anyString(), any(), any());
    }
}
