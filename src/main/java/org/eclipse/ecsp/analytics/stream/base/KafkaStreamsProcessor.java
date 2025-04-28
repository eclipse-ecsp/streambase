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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.utils.DLQHandler;
import org.eclipse.ecsp.domain.IgniteExceptionDataV1_1;
import org.eclipse.ecsp.entities.EventData;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.eclipse.ecsp.utils.metrics.IgniteErrorCounter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;

/**
 * A thin layer on top of our processors to support.
 * <ul>
 * <li>portability between similar systems in the future</li>
 * <li>an AOP like layer that will allow interjection of common processing
 * across all processors.</li>
 * </ul>
 *
 * @author ssasidharan
 * @param <KIn> the generic type
 * @param <VIn> the generic type
 * @param <KOut> the generic type
 * @param <VOut> the generic type
 */
public class KafkaStreamsProcessor<KIn, VIn, KOut, VOut> implements
        Processor<KIn, VIn, KOut, VOut>, ConfigChangeListener, TickListener {

    /** The Constant DELIM. */
    private static final String DELIM = "/";
    
    /** The Constant PROCESS_RATE. */
    private static final String PROCESS_RATE = "ProcessingRatePerSecond";
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaStreamsProcessor.class);
    
    /** The worker. This holds the instance of a {@link StreamProcessor}*/
    private StreamProcessor<KIn, VIn, KOut, VOut> worker;
    
    /** The {@link KafkaStreamsProcessorContext} .*/
    private KafkaStreamsProcessorContext<KOut, VOut> context;
    
    /** The config. */
    private Properties config;
    
    /** The {@link MetricRegistry}. */
    private MetricRegistry metricRegistry;
    
    /** The processed event rate meter. */
    private Meter processedEventRateMeter = null;
    
    /** The last processor in chain. */
    private boolean lastProcessorInChain = false;
    
    /** The {@link DLQHandler}. */
    @Autowired
    private DLQHandler dLQHandler;

    /** The {@link IgniteErrorCounter}. */
    @Autowired
    private IgniteErrorCounter errorCounter;

    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /**
     * Instantiates a new kafka streams processor.
     */
    public KafkaStreamsProcessor() {
        // default constructor
    }

    /**
     * Initializes the stream processor class.
     *
     * @param worker {@link StreamProcessor}
     * @param config The configuration supplied.
     * @param metricRegistry {@link MetricRegistry}
     * @param lastProcessorInChain lastProcessorInChain
     */
    public void initProcessor(StreamProcessor<KIn, VIn, KOut, VOut> worker, Properties config, 
                MetricRegistry metricRegistry, boolean lastProcessorInChain) {
        logger.debug("Initializing KafkaStreamsProcessor for worker {} ", worker.name());
        this.worker = worker;
        this.config = config;
        this.metricRegistry = metricRegistry;
        this.lastProcessorInChain = lastProcessorInChain;
        logger.info("Creating KafkaStreamsProcessor wrapper {} for worker {} named {}", this, worker, worker.name());

    }

    /**
     * Initiliazes the init() on the stream-processor worker and registers the metrics for it.
     *
     * @param c the c
     */
    @Override
    public void init(ProcessorContext<KOut, VOut> c) {
        logger.debug("Inside init method of KafkaStreamsProcessor for worker {}",
                worker.name());
        this.context = new KafkaStreamsProcessorContext<>(c, config, this.metricRegistry, lastProcessorInChain, ctx);
        logger.info("Initializing stream processor {}", worker.name());
        worker.init(this.context);
        //  this here, so that it calls only after the Processor is initialized
        WallClock.getInstance().subscribe(this);
        registerMetrics();
        logger.info("Initialization complete for stream processor {}", worker.name());
    }

    /**
     * Process the Kafka record as soon as it comes in the topic. This method also handles the 
     * exception occurred in the processing of the record by a stream processor down the line.
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<KIn, VIn> kafkaRecord) {

        KIn key = kafkaRecord.key();
        VIn value = kafkaRecord.value();
        try {
            mark(processedEventRateMeter);
            if (key instanceof byte[] keyBytes) {
                byte[] valueBytes = (byte[]) value;
                logger.debug("Processing byte array Key:{},byte array value:{} by worker:{}",
                        new String(keyBytes, StandardCharsets.UTF_8),
                        new String(valueBytes, StandardCharsets.UTF_8), worker.name());
            }
            if (key instanceof IgniteKey) {
                logger.debug("Processing ignite Key:{}, ignite value:{} by worker:{}", key, value, worker.name());
                if (value instanceof IgniteEvent igniteEvent) {
                    EventData eventData = igniteEvent.getEventData();
                    if (dLQHandler.isReprocessingEnabled()
                            && eventData instanceof IgniteExceptionDataV1_1 igniteExceptionData) {
                        String processorName = igniteExceptionData.getProcessorName();
                        if (worker.name().equals(processorName)) {
                            worker.process(kafkaRecord);
                        } else {
                            @SuppressWarnings("unchecked")
                            KOut igniteKey = (KOut) key;

                            @SuppressWarnings("unchecked")
                            VOut igniteValue = (VOut) value;

                            Record<KOut, VOut> igniteRecord =
                                    new Record<>(igniteKey, igniteValue, kafkaRecord.timestamp());
                            this.context.forward(igniteRecord);
                        }
                        return;
                    }
                }
            }
            worker.process(kafkaRecord);
        } catch (Exception ex) {
            errorCounter.incErrorCounter(Optional.ofNullable(context.getTaskID()), ex.getClass());
            dLQHandler.forwardToDlq(context, key, value, ex, worker.name());
        }
    }

    /**
     * Punctuate.
     *
     * @param timestamp the timestamp
     */
    // RTC-141484 - Kafka version upgrade from 1.0.0. to 2.2.0 changes.
    public void punctuate(long timestamp) {
        worker.punctuate(timestamp);
    }

    /**
     * Invokes close() on this worker.
     */
    @Override
    public void close() {
        worker.close();
        WallClock.getInstance().unsubscribe(this);
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        worker.configChanged(props);
    }

    /**
     * Tick.
     *
     * @param seconds the seconds
     */
    @Override
    public void tick(long seconds) {
        worker.punctuateWc(seconds);
    }

    /**
     * Metric to report rate at which events are getting processed,
     * or in other words rate at which the process method is getting called.
     */
    private void registerEventRateMetric() {
        if (Boolean.parseBoolean(this.config.getProperty("metrics.event.rate.enable", "true"))) {
            logger.info("Registering metrics to report the event rate per second");
            String metricName = this.worker.name() + DELIM + PROCESS_RATE;
            // Every thread should share the same metric
            if (null != metricRegistry) {
                try {
                    processedEventRateMeter = this.metricRegistry.register(metricName, new Meter());
                } catch (Exception e) {
                    logger.info("Metric {} already registered.", metricName);
                    processedEventRateMeter = this.metricRegistry.getMeters().get(metricName);
                }
            } else {
                logger.error("Metric registry is not initialized.");
            }

        }
    }

    /**
     * Register metrics.
     */
    private void registerMetrics() {
        registerEventRateMetric();
    }

    /**
     * Marking method for metered event metric.
     *
     * @param meter meter
     */
    private void mark(Meter meter) {
        if (null != meter) {
            meter.mark();
        }
    }

    /**
     * Gets the error count.
     *
     * @param exceptionClassName the exception class name
     * @return the error count
     */
    public double getErrorCount(Class<?> exceptionClassName) {
        return errorCounter.getErrorCounterValue(Optional.ofNullable(context.getTaskID()), exceptionClassName);
    }
}
