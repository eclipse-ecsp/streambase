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

package org.eclipse.ecsp.analytics.stream.base.stores;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.context.StreamBaseSpringContext;
import org.eclipse.ecsp.analytics.stream.base.metrics.reporter.HarmanRocksDBMetricsExporter;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanRocksDBStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.rocksdb.RocksDB;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * {@link HarmanRocksDBStoreTest} UT class.
 */
public class HarmanRocksDBStoreTest {

    /** The metrics recorder. */
    RocksDBMetricsRecorder metricsRecorder = mock(RocksDBMetricsRecorder.class);
    
    /** The processor context. */
    @Mock
    ProcessorContext processorContext;
    
    /** The state store context. */
    @Mock
    StateStoreContext stateStoreContext;
    
    /** The state store. */
    @Mock
    StateStore stateStore;
    
    /** The state store config. */
    @Mock
    Properties stateStoreConfig;
    
    /** The context. */
    @Mock
    ApplicationContext context;
    
    /** The harman rocks DB metrics exporter. */
    @Mock
    HarmanRocksDBMetricsExporter harmanRocksDBMetricsExporter;
    
    /** The harman rocks DB store. */
    @InjectMocks
    private HarmanRocksDBStore harmanRocksDBStore = new HarmanRocksDBStore<Bytes, byte[]>("rocksdb-test", "rocksdb",
            Serdes.Bytes(),
            Serdes.ByteArray(), new Properties(), metricsRecorder);

    /**
     * Setup.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        RocksDB.loadLibrary();
    }

    /**
     * Test rocks DB with metrics disabled.
     */
    @Test
    public void testRocksDBWithMetricsDisabled() {
        Properties properties = new Properties();
        properties.setProperty(PropertyNames.ROCKSDB_METRICS_ENABLED, "false");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        harmanRocksDBStore.setProperties(properties);
        when(stateStoreConfig.isEmpty()).thenReturn(false);
        when(stateStoreContext.stateDir()).thenReturn(new File("/tmp/kafka-streams"));
        harmanRocksDBStore.init(stateStoreContext, stateStore);
        harmanRocksDBStore.close();
        verify(metricsRecorder, Mockito.times(0)).addValueProviders(any(), any(), any(), any());
        verify(metricsRecorder, Mockito.times(0)).removeValueProviders(any());
        verify(metricsRecorder, Mockito.times(0)).init(any(), any());
    }

    /**
     * Test rocks DB with unsupported exception.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testRocksDBWithUnsupportedException() {
        Properties properties = new Properties();
        properties.setProperty(PropertyNames.ROCKSDB_METRICS_ENABLED, "false");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        harmanRocksDBStore.setProperties(properties);
        when(stateStoreConfig.isEmpty()).thenReturn(false);
        when(stateStoreContext.stateDir()).thenReturn(new File("/tmp/kafka-streams"));
        harmanRocksDBStore.init(processorContext, stateStore);
        harmanRocksDBStore.close();
        verify(metricsRecorder, Mockito.times(0)).addValueProviders(any(), any(), any(), any());
        verify(metricsRecorder, Mockito.times(0)).removeValueProviders(any());
        verify(metricsRecorder, Mockito.times(0)).init(any(), any());
    }

    /**
     * Test rocks DB with metrics enabled.
     */
    @Test
    public void testRocksDBWithMetricsEnabled() {

        Properties properties = new Properties();
        properties.setProperty(PropertyNames.ROCKSDB_METRICS_ENABLED, "true");
        harmanRocksDBStore.setProperties(properties);
        Mockito.when(stateStoreConfig.isEmpty()).thenReturn(false);
        doNothing().when(metricsRecorder).init(any(), any());
        Mockito.when(stateStoreContext.taskId())
                .thenReturn(new TaskId(Constants.TWO, 1));
        Map<String, Object> config = new HashMap<>();
        config.put(METRICS_RECORDING_LEVEL_CONFIG, RecordingLevel.DEBUG.toString());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        Mockito.when(stateStoreContext.appConfigs()).thenReturn(config);
        StreamBaseSpringContext streamBaseSpringContext = new StreamBaseSpringContext();
        streamBaseSpringContext.setApplicationContext(context);
        when(context.getBean(HarmanRocksDBMetricsExporter.class)).thenReturn(harmanRocksDBMetricsExporter);
        when(stateStoreContext.stateDir()).thenReturn(new File("/tmp/kafka-streams"));
        harmanRocksDBStore.init(stateStoreContext, stateStore);
        harmanRocksDBStore.close();
        verify(metricsRecorder, Mockito.times(1)).init(any(), any());
        verify(metricsRecorder, Mockito.times(1)).addValueProviders(any(), any(), any(), any());
        verify(metricsRecorder, Mockito.times(1)).removeValueProviders(any());
    }

    /**
     * Test rocks DB restore batch.
     */
    @Test
    public void testRocksDBRestoreBatch() {

        String key = "key1";
        byte[] keyArr = key.getBytes(StandardCharsets.UTF_8);
        String value = "value1";
        byte[] valueArr = key.getBytes(StandardCharsets.UTF_8);
        Headers header = new RecordHeaders();

        ConsumerRecord<byte[], byte[]> record1 = new
                ConsumerRecord<byte[], byte[]>("testTopic", 0, 0, keyArr, valueArr);
        List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
        consumerRecords.add(record1);

        Properties properties = new Properties();
        properties.setProperty(PropertyNames.ROCKSDB_METRICS_ENABLED, "true");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        harmanRocksDBStore.setProperties(properties);
        Mockito.when(stateStoreConfig.isEmpty()).thenReturn(false);
        doNothing().when(metricsRecorder).init(any(), any());
        Mockito.when(stateStoreContext.taskId()).thenReturn(new TaskId(Constants.TWO, 1));
        Map<String, Object> config = new HashMap<>();
        config.put(METRICS_RECORDING_LEVEL_CONFIG, RecordingLevel.DEBUG.toString());
        Mockito.when(stateStoreContext.appConfigs()).thenReturn(config);
        when(stateStoreContext.stateDir()).thenReturn(new File("/tmp/kafka-streams"));
        StreamBaseSpringContext streamBaseSpringContext = new StreamBaseSpringContext();
        streamBaseSpringContext.setApplicationContext(context);
        when(context.getBean(HarmanRocksDBMetricsExporter.class)).thenReturn(harmanRocksDBMetricsExporter);

        harmanRocksDBStore.init(stateStoreContext, stateStore);
        harmanRocksDBStore.restoreBatch(consumerRecords);

        assertNotNull("Position is null", harmanRocksDBStore.getPosition());

        harmanRocksDBStore.close();
    }

    /**
     * Test rocks DB with metrics enabled with path traversal attack attempt.
     */
    @Test
    public void testRocksDBWithMetricsEnabledWithPathTraversalAttackAttempt() {

        harmanRocksDBStore = new HarmanRocksDBStore<Bytes, byte[]>("..\\..\\rocksdb-test", "rocksdb",
                Serdes.Bytes(),
                Serdes.ByteArray(), new Properties(), metricsRecorder);

        Properties properties = new Properties();
        properties.setProperty(PropertyNames.ROCKSDB_METRICS_ENABLED, "true");
        harmanRocksDBStore.setProperties(properties);
        Mockito.when(stateStoreConfig.isEmpty())
                .thenReturn(false);
        doNothing().when(metricsRecorder).init(any(), any());
        Mockito.when(stateStoreContext.taskId())
                .thenReturn(new TaskId(Constants.TWO, 1));
        Map<String, Object> config = new HashMap<>();
        config.put(METRICS_RECORDING_LEVEL_CONFIG, RecordingLevel.DEBUG.toString());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        Mockito.when(stateStoreContext.appConfigs()).thenReturn(config);
        StreamBaseSpringContext streamBaseSpringContext = new StreamBaseSpringContext();
        streamBaseSpringContext.setApplicationContext(context);
        when(context.getBean(HarmanRocksDBMetricsExporter.class)).thenReturn(harmanRocksDBMetricsExporter);
        when(stateStoreContext.stateDir()).thenReturn(new File("/tmp/kafka-streams"));
        harmanRocksDBStore.init(stateStoreContext, stateStore);
        harmanRocksDBStore.close();
        verify(metricsRecorder, Mockito.times(1)).init(any(), any());
        verify(metricsRecorder, Mockito.times(1)).addValueProviders(any(), any(), any(), any());
        verify(metricsRecorder, Mockito.times(1)).removeValueProviders(any());
    }
}
