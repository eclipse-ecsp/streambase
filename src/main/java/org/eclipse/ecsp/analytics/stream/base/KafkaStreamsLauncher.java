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

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.eclipse.ecsp.analytics.stream.base.KafkaStreamsProcessorContext.StoreType;
import org.eclipse.ecsp.analytics.stream.base.discovery.StreamProcessorDiscoveryService;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidStoreException;
import org.eclipse.ecsp.analytics.stream.base.kafka.support.KafkaStreamsThreadStatusPrinter;
import org.eclipse.ecsp.analytics.stream.base.kafka.support.LoggingStateRestoreListener;
import org.eclipse.ecsp.analytics.stream.base.processors.ProtocolTranslatorPostProcessor;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanRocksDBStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaSslUtils;
import org.eclipse.ecsp.diagnostic.DiagnosticService;
import org.eclipse.ecsp.healthcheck.HealthMonitor;
import org.eclipse.ecsp.healthcheck.HealthService;
import org.eclipse.ecsp.healthcheck.HealthServiceCallBack;
import org.eclipse.ecsp.stream.dma.handler.MaxFailuresUncaughtExceptionHandler;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.rocksdb.RocksDB;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.KAFKA_STREAMS_MAX_FAILURES;
import static org.eclipse.ecsp.analytics.stream.base.PropertyNames.KAFKA_STREAMS_MAX_TIME_INTERVAL;

/**
 * Launches {@link KafkaStreams} application processing logic packaged appropriately for Kafka Streams.
 * This launcher provides with the topology building and the state-store configuration to
 * the streaming application.
 *
 * @author ssasidharan
 *
 * @param <KIn> the type parameter for incoming key.
 * @param <VIn> the type parameter for incoming value.
 * @param <KOut> the type parameter for outgoing key.
 * @param <VOut> the type parameter for outgoing value.
 * 
 */
@Component
public class KafkaStreamsLauncher<KIn, VIn, KOut, VOut> extends AbstractLauncher<KIn, VIn, KOut, VOut> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaStreamsLauncher.class);
    
    /** The group id suffix. */
    private int groupIdSuffix = 0;
    
    /** The {@link KafkaStreams} instance. */
    private KafkaStreams streams = null;

    /** Whether to clean up state store or not. */
    @Value("${perform.state.store.cleanup:false}")
    private boolean cleanupStateStore = false;

    /** The list of health monitor whose health status can be ignored at the bootstrap 
     * of the application. */
    @Value("#{'${ignore.bootstrap.failure.monitors}'.split(',')}")
    private List<String> bootstrapIgnoredMonitors;

    /** Whether to restart the streams application or not. */
    @Value("${sp.restart.on.failure:false}")
    private boolean restartOnFailure;

    /** The wait time in mills. */
    @Value("${sp.restart.wait.time.in.millis:10000}")
    private int waitTimeInMills;

    /** The maximum failures. */
    @Value("${" + KAFKA_STREAMS_MAX_FAILURES + "}")
    private int maximumFailures;

    /** The max time interval. */
    @Value("${" + KAFKA_STREAMS_MAX_TIME_INTERVAL + "}")
    private long maxTimeInterval;

    /** The Spring ApplicationContext. */
    @Autowired
    private ApplicationContext ctx;

    /** The {@link KafkaStateListener} implementation. */
    @Autowired
    private KafkaStateListener kafkaStateListener;

    /** The {@link HealthService} instance. */
    @Autowired
    private HealthService healthService;

    /** The {@link KafkaStreamsThreadStatusPrinter} instance. */
    @Autowired
    private KafkaStreamsThreadStatusPrinter threadStatusPrinter;

    /** The diagnostic service. */
    @Autowired
    private DiagnosticService diagnosticService;

    /**
     * Closes the {@link KafkaStreams} instance and terminates the application.
     */
    @Override
    public void terminate() {
        if (streams != null) {
            logger.info("Closing streams application");
            threadStatusPrinter.close();
            streams.close();
            super.terminate();
        }
    }

    /**
     * Terminate streams with timeout. To be used only for unit test cases.
     */
    @Override
    public void terminateStreamsWithTimeout() {
        if (streams != null) {
            logger.info("Closing streams application with timeout of 30 seconds");
            threadStatusPrinter.close();
            streams.close(Duration.ofSeconds(Constants.INT_30));
            super.terminate();
        }
    }

    /**
     * Builds the topology for the streaming application. The topology is built like a linked list, where
     * multiple stream processor classes are linked together like a chain. A Kafka record is read from the 
     * stream and is passed through all the processors linked together in chain one by one. Each processor 
     * may or may not process the record (depending upon the use-case of the processor).
     *
     * <p>
     * Stream-base library exposes multiple stream processor classes called pre-processor and 
     * post-processors, where pre-processor classes execute certain logic on the record like transforming the
     * byte[] array data into desired format etc. and post-processor classes execute the logic on the record
     * once it's been processed completely by the service's stream-processor and again given back to stream-base
     * library, for eg. Putting the record into some sink Kafka topic.
     * </p>
     *
     * @param processors The complete list of all the processor classes that need to be chained.
     * @param props the Properties instance containing all the configs for the application.
     * @return the {@link Topology} instance.
     */
    private Topology buildKafkaStreamsTopology(List<StreamProcessor<?, ?, ?, ?>> processors, Properties props) {
        // Pre-loading seems to avoid the .so corruption issue
        RocksDB.loadLibrary();
        Map<String, List<String>> outputSinks = new HashMap<>();
        Map<StreamProcessor<?, ?, ?, ?>, Class<? extends StreamProcessor>> processorMap = new HashMap<>();
        List<StreamProcessor<?, ?, ?, ?>> processorClones = new ArrayList<>();
        String prev = "source";
        String qualifier = getStreamQualifier(props);
        Set<String> inputSources = validateInputSources(processors, props, outputSinks, processorMap, processorClones);
        Topology topology = new Topology();
        topology.addSource(prev, inputSources.stream().map(t -> t + qualifier).toList().toArray(new String[0]));
        inputSources.stream().forEach(s -> logger.info("Subscribing processors to source topic {}", s));
        /*
         * Register processor in order to get notification whenever incremental
         * master/settings data arrived in shared data topics.
         */
        groupIdSuffix++;
        int numberOfProcessors = processorClones.size();
        int counter = 1;
        boolean lastProcessor = false;
        boolean legacyTopologySinkBuilder = true;

        for (StreamProcessor<?, ?, ?, ?> processorTemplate : processorClones) {
            lastProcessor = isLastProcessor(numberOfProcessors, counter);
            if (lastProcessor) {
                logger.info("Chained Processor Class {} and ProtocolTranslatorPostProcessor Class name {}",
                        processorTemplate.getClass().getName(), ProtocolTranslatorPostProcessor.class.getName());
                legacyTopologySinkBuilder = !(processorTemplate instanceof ProtocolTranslatorPostProcessor);
            }
            addProcessorToTopology(props, processorMap, prev, topology, lastProcessor, processorTemplate);

            // Get the value of state.store.type property
            HarmanPersistentKVStore<?, ?> storeTemplate = processorTemplate.createStateStore();

            if (storeTemplate != null) {

                /*
                 * StreamProcessor must not provide the implementation of
                 * createStateStore method, if they want HashMap as state store
                 * instead of RocksDB store.
                 *
                 * Throw exception if StreamProcessor has specified the store
                 * type(state.store.type) as map and provided the implementation
                 * of createStateStore method
                 */
                checkStoreType(props);
                addStoreToTopology(topology, processorTemplate, storeTemplate);
            }
            prev = processorTemplate.name();
            // increment the stream processor
            counter++;
        }
        // We should support configurable value types for the output topics
        getProcessor(outputSinks, processorClones, qualifier, topology, legacyTopologySinkBuilder);
        return topology;
    }

    /**
     * Adds the state-store to KafkaStreams topology.
     *
     * @param topology the {@link Topology} instance
     * @param processorTemplate the {@link StreamProcessor} instance.
     * @param storeTemplate the {@link HarmanPersistentKVStore} instance.
     */
    private static void addStoreToTopology(Topology topology, StreamProcessor<?, ?, ?, ?> processorTemplate,
            HarmanPersistentKVStore<?, ?> storeTemplate) {
        topology.addStateStore(new StoreBuilder() {

            @Override
            public StoreBuilder withCachingEnabled() {
                return this;
            }

            @Override
            public StoreBuilder withLoggingEnabled(Map config) {
                return this;
            }

            @Override
            public StoreBuilder withLoggingDisabled() {
                return this;
            }

            @Override
            public StateStore build() {
                return processorTemplate.createStateStore();
            }

            @Override
            public Map logConfig() {
                return new HashMap<String, String>();
            }

            @Override
            public boolean loggingEnabled() {
                return true;
            }

            @Override
            public String name() {
                return storeTemplate.name();
            }

            // RTC-141484 - Kafka version upgrade from 1.0.0. to 2.2.0
            // changes.
            @Override
            public StoreBuilder withCachingDisabled() {
                return this;
            }

        }, processorTemplate.name());
    }

    /**
     * Checks store type.
     *
     * @param props the props
     */
    private static void checkStoreType(Properties props) {
        String storeType = props.getProperty(PropertyNames.STATE_STORE_TYPE, StoreType.ROCKSDB.getStoreType());
        if (storeType.equals(StoreType.MAP.getStoreType())) {
            throw new InvalidStoreException("Store Implementation found for hash map state store.");
        }
    }

    /**
     * Adds a processor to the topology.
     *
     * @param props the Properties instance
     * @param processorMap {@link StreamProcessor} Class' instance.
     * @param prev the previous processor chained to this processor.
     * @param topology the {@link Topology} instance.
     * @param lastProcessor If this is the last processor to be chained.
     * @param processorTemplate the {@link StreamProcessor} implementation instance.
     */
    private void addProcessorToTopology(Properties props, Map<StreamProcessor<?, ?, ?, ?>, 
            Class<? extends StreamProcessor>> processorMap, String prev, Topology topology, 
               boolean lastProcessor, StreamProcessor<?, ?, ?, ?> processorTemplate) {
        topology.addProcessor(processorTemplate.name(), getProcessorSupplier(processorMap.get(processorTemplate), 
                 props, this.metricRegistry, lastProcessor), prev);
        logger.info("Chained {} -> {}", prev, processorTemplate.name());
    }

    /**
     * Checks if it is last processor.
     *
     * @param numberOfProcessors the number of processors
     * @param counter the counter
     * @return true, if is last processor
     */
    private boolean isLastProcessor(int numberOfProcessors, int counter) {
        return counter == numberOfProcessors;
    }

    /**
     * Validate input sources.
     *
     * @param processors the processors
     * @param props the props
     * @param outputSinks the output sinks
     * @param processorMap the processor map
     * @param processorClones the processor clones
     * @return the Set view of input sources.
     */
    private Set<String> validateInputSources(List<StreamProcessor<?, ?, ?, ?>> processors,
            Properties props, Map<String, List<String>> outputSinks, Map<StreamProcessor<?, ?, ?, ?>, Class<?
            extends StreamProcessor>> processorMap,
            List<StreamProcessor<?, ?, ?, ?>> processorClones) {
        Set<String> inputSources = new HashSet<>();
        for (StreamProcessor<?, ?, ?, ?> sp : processors) {
            addInputSourcesSinkers(props, inputSources, outputSinks, processorMap, processorClones, sp);
        }
        // give preference to stream processor input topics
        if (inputSources.isEmpty()) {
            inputSources.add(props.getProperty(PropertyNames.SOURCE_TOPIC_NAME));
        }
        if (inputSources.isEmpty()) {
            throw new IllegalArgumentException("No input topics configured!!!");
        }
        return inputSources;
    }

    /**
     * Gets the processor.
     *
     * @param outputSinks the output sinks
     * @param processorClones the processor clones
     * @param qualifier the qualifier
     * @param topology the topology
     * @param legacyTopologySinkBuilder the legacy topology sink builder
     * @return the processor
     */
    private void getProcessor(Map<String, List<String>> outputSinks,
            List<StreamProcessor<?, ?, ?, ?>> processorClones,
            String qualifier, Topology topology, boolean legacyTopologySinkBuilder) {
        logger.info("Legacy topology sink builder is set to:{}", legacyTopologySinkBuilder);
        if (legacyTopologySinkBuilder) {
            logger.info("Building the topology sink builder in legacy way.");
            outputSinks.forEach((sink, sinkers) -> topology.addSink(sink,
                    sink + qualifier, new StringSerializer(), new StringSerializer(),
                    sinkers.toArray(new String[0])));
        } else {
            logger.info("Last processor in the chain will be the sink node for all the sink topics.");
            // get the last processor from the processor list
            StreamProcessor<?, ?, ?, ?> lastProcessorInChain = processorClones.get(processorClones.size() - 1);
            logger.info("Last processor in the chain:{}", lastProcessorInChain.name());
            outputSinks.forEach(
                    (sink, sinkers) -> topology.addSink(sink, sink + qualifier,
                            new ByteArraySerializer(), new ByteArraySerializer(),
                            lastProcessorInChain.name()));
        }
    }

    /**
     * Adds the input sources sinkers.
     *
     * @param props the Properties instance.
     * @param inputSources the input sources
     * @param outputSinks the output sinks
     * @param processorMap the processor map
     * @param processorClones the processor clones
     * @param sp the sp
     */
    private void addInputSourcesSinkers(Properties props, Set<String> inputSources,
            Map<String, List<String>> outputSinks,
            Map<StreamProcessor<?, ?, ?, ?>, Class<? extends StreamProcessor>> processorMap,
            List<StreamProcessor<?, ?, ?, ?>> processorClones, StreamProcessor<?, ?, ?, ?> sp) {
        StreamProcessor<?, ?, ?, ?> clone = null;
        try {
            clone = (StreamProcessor<?, ?, ?, ?>) ctx.getAutowireCapableBeanFactory().createBean(sp.getClass());
            clone.initConfig(props);
            processorClones.add(clone);
            processorMap.put(clone, sp.getClass());
        } catch (BeansException be) {
            throw new IllegalArgumentException("Unable to instantiate processor", be);
        }
        inputSources.addAll(Arrays.asList(clone.sources()));
        for (String sink : clone.sinks()) {
            outputSinks.computeIfAbsent(sink, k -> new ArrayList<>());
            List<String> sinkers = outputSinks.get(sink);
            sinkers.add(clone.name());
        }
    }

    /**
     * Validates KafkaStreams specific configurations before launching streams.
     *
     * @param props the {@link Properties} instance.
     */
    private void checkMergeDefaults(Properties props) {
        validateProps(props);
        String replFactor = null;
        if ((replFactor = props.getProperty(PropertyNames.REPLICATION_FACTOR)) == null) {
            props.put(PropertyNames.REPLICATION_FACTOR, Constants.TWO);
        } else {
            props.put(PropertyNames.REPLICATION_FACTOR, Integer.parseInt(replFactor));
        }
        // RTC-141484 - Kafka version upgrade from 1.0.0. to 2.2.0 changes.
        String keySerde = props.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG);
        if (keySerde == null) {
            logger.info("default.key.serde not found in properties. Using ByteArray as default.");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        }
        String valueSerde = props.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG);
        if (valueSerde == null) {
            logger.info("default.value.serde not found in properties. Using ByteArray as default.");
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        }
        String keyDeSerde = props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        if (keyDeSerde == null) {
            logger.info("Consume key deserializer not found in properties. Using ByteArray as default.");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    Serdes.ByteArray().deserializer().getClass().getName());
        }
        String valueDeSerde = props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        if (valueDeSerde == null) {
            logger.info("Consumer value deserializer not found in properties. Using ByteArray as default.");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    Serdes.ByteArray().deserializer().getClass().getName());
        }
        String kafkaRebalanceTime = props.getProperty(PropertyNames.KAFKA_REBALANCE_TIME_MINS);
        if (StringUtils.isEmpty(kafkaRebalanceTime)) {
            props.put(PropertyNames.KAFKA_REBALANCE_TIME_MINS, "10");
        }
        String kafkaCloseTimeout = props.getProperty(PropertyNames.KAFKA_CLOSE_TIMEOUT_SECS);
        if (StringUtils.isEmpty(kafkaCloseTimeout)) {
            props.put(PropertyNames.KAFKA_CLOSE_TIMEOUT_SECS, "30");
        }
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, RecordingLevel.DEBUG.toString());
        KafkaSslUtils.checkAndApplySslProperties(props);
        logger.info("Initializing with properties:");
        props.forEach((k, v) -> logger.info("\t" + k + "=" + v));
    }

    /**
     * Validate props.
     *
     * @param props the props
     */
    private void validateProps(Properties props) {
        if (props.getProperty(PropertyNames.APPLICATION_ID) == null) {
            throw new IllegalArgumentException(PropertyNames.APPLICATION_ID + " is mandatory");
        }
        // if (props.getProperty(PropertyNames.AUTO_OFFSET_RESET_CONFIG) ==
        // null) {
        // throw new
        // IllegalArgumentException(PropertyNames.AUTO_OFFSET_RESET_CONFIG + "
        // is mandatory");
        // }
        if (props.getProperty(PropertyNames.BOOTSTRAP_SERVERS) == null) {
            props.put(PropertyNames.BOOTSTRAP_SERVERS, "localhost:9092");
        }
        if (props.get(PropertyNames.ZOOKEEPER_CONNECT) == null) {
            props.put(PropertyNames.ZOOKEEPER_CONNECT, "localhost:2181/haa");
        }
        String numThreads = null;
        if ((numThreads = props.getProperty(PropertyNames.NUM_STREAM_THREADS)) == null) {
            props.put(PropertyNames.NUM_STREAM_THREADS, Constants.FOUR);
        } else {
            props.put(PropertyNames.NUM_STREAM_THREADS, Integer.parseInt(numThreads));
        }
    }

    /**
     * Gets {@link ProcessorSupplier} instance. 
     *
     * @param processor the processor
     * @param props the props
     * @param metricRegistry {@link MetricRegistry}
     * @param isLastProcessor if it is last processor
     * @return the processor supplier instance.
     * 
     * @see ProcessorSupplier
     *
     */
    @SuppressWarnings("unchecked")
    private ProcessorSupplier<KIn, VIn, KOut, VOut> getProcessorSupplier(
            Class<?> processor, Properties props,
            MetricRegistry metricRegistry, boolean isLastProcessor) {
        return () -> {
            try {
                StreamProcessor<KIn, VIn, KOut, VOut> clone = (StreamProcessor<KIn, VIn, KOut, VOut>) 
                    ctx.getAutowireCapableBeanFactory().createBean(processor);
                clone.initConfig(props);
                KafkaStreamsProcessor<KIn, VIn, KOut, VOut> streamProcessor = 
                        (KafkaStreamsProcessor<KIn, VIn, KOut, VOut>) ctx.getAutowireCapableBeanFactory()
                        .createBean(KafkaStreamsProcessor.class);
                streamProcessor.initProcessor(clone, props, metricRegistry, isLastProcessor);
                return streamProcessor;
            } catch (BeansException be) {
                throw new IllegalArgumentException("Unable to instantiate processor", be);
            }
        };
    }

    /**
     * Gets the stream qualifier.
     *
     * @param config the Properties instance.
     * @return the stream qualifier for this environment.
     */
    private String getStreamQualifier(Properties config) {
        String topicQualifier = "";
        String env = config.getProperty(PropertyNames.ENV);
        if ((env != null) && (env.length() > 0)) {
            String tenant = config.getProperty(PropertyNames.TENANT);
            topicQualifier = "-" + env + "-" + tenant;
        }
        return topicQualifier;
    }

    /**
     * Provision to check the health status of certain components, if enabled. For eg. Redis, MongoDB, 
     * Kafka Consumer group, MQTT Server etc. If the health monitor is part of the "ignore at bootstrap" list,
     * then health status reported by those won't matter and application will proceed to start regardless.
     * If initial health check fails then log the error and exit the application.
     *
     * @return true, if application needs to be terminated.
     */
    protected boolean bootstrapHealthCheck() {
        logger.info("List of health monitors that can be ignored if initial"
                + " health check fails are : {}", bootstrapIgnoredMonitors);
        List<HealthMonitor> failedHealthMonitors = healthService.triggerInitialCheck();
        boolean exit = false;
        for (HealthMonitor monitor : failedHealthMonitors) {
            String name = monitor.monitorName();
            if (!bootstrapIgnoredMonitors.contains(monitor.monitorName())) {
                exit = true;
                logger.error("Initial health check failed for health monitor : {}. Cannot be ignored", name);
            } else {
                logger.info("Ignoring initial health check fail for health monitor : {},"
                        + " as it is part of the ignored list", name);
            }
        }
        return exit;
    }

    /**
     * Gets the streams instance.
     *
     * @return the streams
     */
    // used for unit testing
    public KafkaStreams getStreams() {
        return streams;
    }

    /**
     * Launches the {@link KafkaStreams} after executing some preliminary steps and checks.
     * This method is the place where the callback for HealthService is registered. This callback defines 
     * the action to be taken when a health monitor is continuously reporting unhealthy.
     *
     * @param props the Properties instance containing all the configuration supplied.
     */
    @Override
    protected void doLaunch(Properties props) {
        boolean exit = false;
        try {
            exit = bootstrapHealthCheck();
        } catch (Exception e) {
            logger.error("Error while bootstrapping Health Check {}", e);
            exit = true;
        }
        if (exit) {
            logger.info("Waiting on prometheus to collect metrics for {} milliseconds before exiting stream processor.",
                    waitTimeInMills);
            try {
                Thread.sleep(waitTimeInMills);
            } catch (InterruptedException e) {
                logger.error("Interrupted exception has occured in kafka streams launcher post bootstrapHealthCheck");
                Thread.currentThread().interrupt();
            }
            logger.error("Exiting stream processor as initial health check has failed");
            System.exit(1);
        } else {
            logger.info("Initial health check of stream processor has passed. Continuing with streams creation");
        }
        checkMergeDefaults(props);
        initializeMetricReporter(props);
        HarmanRocksDBStore.setProperties(props);
        StreamProcessorDiscoveryService<?, ?, ?, ?> discoverySvc = loadDiscoveryService(props);
        discoverySvc.setProperties(props);
        List<? extends StreamProcessor<?, ?, ?, ?>> processors = discoverySvc.discoverProcessors();
        logger.info("Number of stream processors: {}", processors.size());
        Topology topology = buildKafkaStreamsTopology((List<StreamProcessor<?, ?, ?, ?>>) processors, props);
        streams = new KafkaStreams(topology, props);
        kafkaStateListener.setStreams(streams);
        streams.setStateListener(kafkaStateListener);
        streams.setGlobalStateRestoreListener(new LoggingStateRestoreListener());
        threadStatusPrinter.init(streams);
        final MaxFailuresUncaughtExceptionHandler exceptionHandler =
                new MaxFailuresUncaughtExceptionHandler(maximumFailures,
                        maxTimeInterval);
        streams.setUncaughtExceptionHandler(exceptionHandler);
        if (cleanupStateStore) {
            streams.cleanUp();
        }
        healthService.registerCallBack(new KslHealthServiceCallBack());
        healthService.startHealthServiceExecutor();
        diagnosticService.triggerDiagnosis();
        streams.start();
    }

    /**
     * Gets the bootstrap ignored monitors.
     *
     * @return the bootstrap ignored monitors
     */
    List<String> getBootstrapIgnoredMonitors() {
        return bootstrapIgnoredMonitors;
    }

    /**
     * Sets the bootstrap ignored monitors.
     *
     * @param monitorNames the new bootstrap ignored monitors
     */
    void setBootstrapIgnoredMonitors(List<String> monitorNames) {
        this.bootstrapIgnoredMonitors = monitorNames;
    }

    /**
     * Checks if is restart on failure.
     *
     * @return true, if is restart on failure
     */
    boolean isRestartOnFailure() {
        return restartOnFailure;
    }

    /**
     * Sets the restart on failure.
     *
     * @param restartOnFailure the new restart on failure
     */
    void setRestartOnFailure(boolean restartOnFailure) {
        this.restartOnFailure = restartOnFailure;
    }

    /**
     * Sets the health service.
     *
     * @param healthService the new health service
     */
    void setHealthService(HealthService healthService) {
        this.healthService = healthService;
    }

    /**
     * The callback Class for {@link HealthService} framework.
     */
    class KslHealthServiceCallBack implements HealthServiceCallBack {

        /**
         * Performs restart if the application is configured to be restarted if health monitor(s) 
         * report unhealthy.
         *
         * @return true, if is to be restarted.
         */
        @Override
        public boolean performRestart() {
            if (restartOnFailure) {
                terminate();
            }
            return restartOnFailure;
        }

    }

}
