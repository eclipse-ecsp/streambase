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

package org.eclipse.ecsp.analytics.stream.base.processors;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidKeyOrValueException;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidServiceNameException;
import org.eclipse.ecsp.analytics.stream.base.platform.IgnitePlatform;
import org.eclipse.ecsp.analytics.stream.base.platform.utils.PlatformUtils;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.vehicleprofile.utils.VehicleProfileClientApiUtil;
import org.eclipse.ecsp.domain.AbstractBlobEventData;
import org.eclipse.ecsp.domain.DataUsageEventDataV1_0;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.IgniteEventSource;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.EventData;
import org.eclipse.ecsp.entities.IgniteBlobEvent;
import org.eclipse.ecsp.entities.IgniteDeviceAwareBlobEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.serializer.IngestionSerializer;
import org.eclipse.ecsp.serializer.IngestionSerializerFactory;
import org.eclipse.ecsp.transform.IgniteKeyTransformer;
import org.eclipse.ecsp.transform.Transformer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;  
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * ProtocolTranslatorPreProcessor is one of the pre processors who intercepts
 * the message and key in byte array format and converts it to an IgniteEvent
 * and IgniteKey which is then forwarded to the processors ahead of it.
 *
 * @author avadakkootko
 *
 */

public class ProtocolTranslatorPreProcessor implements StreamProcessor<byte[], byte[], IgniteKey<?>, IgniteEvent> {

    /** The spc. */
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /** The serializer. */
    private IngestionSerializer serializer;
    
    /** The transformer map. */
    private Map<String, Transformer> transformerMap = new HashMap<>();
    
    /** The ignite key transformer. */
    @SuppressWarnings("rawtypes")
    private Optional<IgniteKeyTransformer> igniteKeyTransformer;

    /** The service name. */
    @Value("${service.name:}")
    private String serviceName;
    
    /** The node name. */
    @Value("${" + PropertyNames.NODE_NAME + ":}")
    private String nodeName;
    
    /** The enable prometheus. */
    @Value("${" + PropertyNames.ENABLE_PROMETHEUS + "}")
    private boolean enablePrometheus;

    /** The enable data consumption metric. */
    @Value("${prometheus.data.consumption.metric.enabled:false}")
    private boolean enableDataConsumptionMetric;

    /** The histogram buckets. */
    @Value("#{'${prometheus.histogram.buckets}'.split(',')}")
    private double[] histogramBuckets;

    /** The data consumption histogram buckets. */
    @Value("#{'${prometheus.data.consumption.metric.buckets}'.split(',')}")
    private double[] dataConsumptionHistogramBuckets;

    /** The device aware enable. */
    @Value("${device.aware.enabled:false}")
    private boolean deviceAwareEnable;

    /** The histogram. */
    private static volatile Histogram histogram;
    
    /** The histogram inbound latency. */
    private static volatile Histogram histogramInboundLatency;

    /** The service data consumtion metrics. */
    private static volatile Histogram serviceDataConsumtionMetrics;

    /** The uptime. */
    private static volatile Gauge uptime;

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(ProtocolTranslatorPreProcessor.class);
    
    /** The Constant LOCK. */
    private static final Object LOCK = new Object();
    
    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /** The vehicle profile client api util. */
    @Autowired
    private VehicleProfileClientApiUtil vehicleProfileClientApiUtil;

    /** The platform utils. */
    @Autowired
    private PlatformUtils platformUtils;

    /** The platform id service impl. */
    @Value("${" + PropertyNames.IGNITE_PLATFORM_SERVICE_IMPL_CLASS_NAME + ":}")
    private String platformIdServiceImpl;

    /** The duplicate messge filtering agent. */
    private MessgeFilterAgent duplicateMessgeFilteringAgent;

    /** The is enable duplicate message check. */
    @Value("${" + PropertyNames.MSG_FILTER_ENABLED + ":false}")
    private boolean isEnableDuplicateMessageCheck;

    /** The is kafka data consumption metrics enabled. */
    //enabling streaming of event size to kafka for analytics dashboard RTC-301848
    @Value("${" + PropertyNames.KAFKA_DATA_CONSUMPTION_METRICS + ":false}")
    private boolean isKafkaDataConsumptionMetricsEnabled;

    /** The data usage topic. */
    @Value("${" + PropertyNames.KAFKA_DATA_CONSUMPTION_METRICS_KAFKA_TOPIC + ":}")
    private String dataUsageTopic;

    /** The kafka headers enabled. */
    @Value("${" + PropertyNames.KAFKA_HEADERS_ENABLED + ":false}")
    private boolean kafkaHeadersEnabled;
    
    /** The kafka topic name platform prefixes. */
    @Value("#{'${" + PropertyNames.KAFKA_TOPIC_NAME_PLATFORM_PREFIXES + ":}'.split(',')}")
    protected List<String> kafkaTopicNamePlatformPrefixes;
    
    /** The vehicle profile platform ids. */
    @Value("#{'${" + PropertyNames.VEHICLE_PROFILE_PLATFORM_IDS + ":}'.split(',')}")
    private List<String> vehicleProfilePlatformIds;

    /** The ignite event. */
    private IgniteEvent igniteEvent = null;
    
    /** The transformer source. */
    private String transformerSource = null;

    /**
     * Inits the.
     *
     * @param spc the spc
     */
    @Override
    public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
        this.spc = spc;
        logger.debug("enablePrometheus {}", enablePrometheus);
        logger.info("{} is configured as {}", PropertyNames.KAFKA_TOPIC_NAME_PLATFORM_PREFIXES, 
                kafkaTopicNamePlatformPrefixes);
        logger.info("{} is configured as {}", PropertyNames.VEHICLE_PROFILE_PLATFORM_IDS, vehicleProfilePlatformIds);
        if ((null == histogram || null == uptime) && enablePrometheus) {
            synchronized (LOCK) {
                if (null == histogram) {
                    histogram = Histogram
                            .build("event_processing_duration_ms", "event_processing_duration_ms")
                            .labelNames("svc", "tid", "node")
                            .buckets(histogramBuckets)
                            .register(CollectorRegistry.defaultRegistry);
                }
                if (null == histogramInboundLatency) {
                    histogramInboundLatency = Histogram
                            .build("inbound_latency", "inbound_latency")
                            .labelNames("svc", "tid", "node")
                            .buckets(histogramBuckets)
                            .register(CollectorRegistry.defaultRegistry);
                }
                if (null == uptime) {
                    uptime = Gauge.build("uptime", "uptime")
                            .labelNames("svc", "node")
                            .register(CollectorRegistry.defaultRegistry);
                }
                if (enableDataConsumptionMetric && null == serviceDataConsumtionMetrics) {
                    serviceDataConsumtionMetrics = Histogram
                            .build("service_data_consumption", "service_data_consumption")
                            .labelNames("svc", "event", "node")
                            .buckets(dataConsumptionHistogramBuckets)
                            .register(CollectorRegistry.defaultRegistry);
                }
            }
        }
        if (isEnableDuplicateMessageCheck) {
            duplicateMessgeFilteringAgent = ctx.getBean(MessgeFilterAgent.class);
        }

    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return "protocol-translator-pre-processor";
    }

    /**
     * In process method first it identifies using DataSniffer class if it is
     * json serialized or java serialized.
     * If json serialized then IgniteEventTransformer Implementation is invoked
     * to convert byte[] to IgniteEvent
     * If java serialized then OemProtocolTransformer is invoked to convert
     * byte[] to IgniteEvent.
     * NOTE : IgniteKeyTransformer is mandatory unlike the above two
     * transformers. This is because usage of other two transformers is known
     * only at runtime unline IgniteKeyTransformer.
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<byte[], byte[]> kafkaRecord) {

        if (null == kafkaRecord) {
            logger.error("Input record to ProtocolTranslatorPreProcessor cannot be null");
            throw new IllegalArgumentException("Input record to ProtocolTranslatorPreProcessor cannot be null");
        }

        byte[] key = kafkaRecord.key();
        byte[] value = kafkaRecord.value();

        Histogram.Timer htimer = getHistogramTimer();

        if (key != null && value != null) {
            validateAndForwardEvent(kafkaRecord, key, value);
            if (null != htimer && enablePrometheus) {
                double time = htimer.observeDuration();
                logger.debug("Time taken to complete event : {}", time);
            }
        } else {
            logger.error("key or value to ProtocolTranslatorPreProcessor cannot be null");
            if (null != htimer && enablePrometheus) {
                double time = htimer.observeDuration();
                logger.debug("Time taken to complete event : {}", time);
            }
            throw new IllegalArgumentException("key or value to ProtocolTranslatorPreProcessor cannot be null");
        }
    }

    /**
     * Validate and forward event.
     *
     * @param kafkaRecord the kafka record
     * @param key the key
     * @param value the value
     */
    private void validateAndForwardEvent(Record<byte[], byte[]> kafkaRecord, byte[] key, byte[] value) {
        Map<String, String> kafkaHeaders;
        IgniteKey<?> igniteKey = null;
        // If igniteKeyTransformer if present
        if (igniteKeyTransformer.isPresent()) {
            igniteKey = igniteKeyTransformer.get().fromBlob(key);
        }
        verifyIgniteKey(key, igniteKey);
        kafkaHeaders = getKafkaHeadersIfEnabled(Arrays.toString(kafkaRecord.key()), kafkaRecord);

        // Use serializer to check if byte[] is json serialized or java
        // serialized.
        if (serializer.isSerialized(value)) {
            if (!createIgniteEvent(value, igniteKey)) {
                return;
            }
        } else if (!createIgniteEventForPlatformIfPresent(kafkaHeaders, value, igniteKey)) {
            return;
        }
        verifyAndCollectDataConsumptionMetric(igniteEvent, igniteKey);
        setKafkaHeadersIfEnabled(kafkaHeaders, (AbstractIgniteEvent) igniteEvent);
        populatePlatformIdFromServiceImpl(kafkaRecord, igniteKey);
        populateVehicleIdFromVehicleProfileApi();
        logger.info("Forwarding key {}, value {} from ProtocolTranslatorPreProcessor", igniteKey, igniteEvent);
        spc.forward(new Record<>(igniteKey, igniteEvent, kafkaRecord.timestamp()));
    }

    /**
     * Verify ignite key.
     *
     * @param key the key
     * @param igniteKey the ignite key
     */
    private static void verifyIgniteKey(byte[] key, IgniteKey<?> igniteKey) {
        if (igniteKey == null) {
            logger.error(
                    "Transformed IgniteKey in ProtocolTranslatorPreProcessor cannot be null for key byte array {}",
                    Arrays.toString(key));
            throw new InvalidKeyOrValueException("Transformed IgniteKey in ProtocolTranslatorPreProcessor "
                    + "cannot be null for key byte array {}" + Arrays.toString(key));
        }
    }

    /**
     * Gets the histogram timer.
     *
     * @return the histogram timer
     */
    private Histogram.Timer getHistogramTimer() {
        Histogram.Timer htimer = null;
        if (enablePrometheus) {
            htimer = histogram.labels(serviceName, spc.getTaskID(), nodeName).startTimer();
            uptime.labels(serviceName, nodeName).set(1);
        }
        return htimer;
    }

    /**
     * Creates the ignite event.
     *
     * @param value the value
     * @param igniteKey the ignite key
     * @return true, if successful
     */
    private boolean createIgniteEvent(byte[] value, IgniteKey<?> igniteKey) {
        IgniteDeviceAwareBlobEvent igniteDeviceAwareBlobEvent = null;
        IgniteBlobEvent blobEvent = null;
        EventData eventData = null;
        if (deviceAwareEnable) {
            igniteDeviceAwareBlobEvent = getIgniteDeviceAwareBlobEvent(value, igniteKey, igniteDeviceAwareBlobEvent);
            eventData = igniteDeviceAwareBlobEvent.getEventData();
        } else {
            blobEvent = getIgniteBlobEvent(value, igniteKey, blobEvent);
            eventData = blobEvent.getEventData();
        }
        
        if (eventData == null) {
            logger.error("Event Data is not present in IgniteBlobEvent {} with key {}", blobEvent, igniteKey);
            return false;
        }
        AbstractBlobEventData blobEventData = (AbstractBlobEventData) eventData;
        transformerSource = blobEventData.getEventSource();
        if (StringUtils.isEmpty(transformerSource)) {
            logger.error("Transformer Source not set for key {}", igniteKey);
            return false;
        }
        Transformer trans = transformerMap.get(transformerSource);
        if (!isTransformerPresent(trans, transformerSource, igniteKey)) {
            return false;
        }
        if (deviceAwareEnable) {
            igniteEvent = trans.fromBlob(blobEventData.getPayload(), Optional.ofNullable(igniteDeviceAwareBlobEvent));
        } else {
            igniteEvent = trans.fromBlob(blobEventData.getPayload(), Optional.ofNullable(blobEvent));
        }
        logger.debug("IgniteEvent after transformation {} for key {}", igniteEvent, igniteKey);
        return isIgniteEventNotNull(igniteEvent, igniteKey);
    }
    
    /**
     * Checks if is transformer present.
     *
     * @param trans the trans
     * @param source the source
     * @param igniteKey the ignite key
     * @return true, if is transformer present
     */
    private boolean isTransformerPresent(Transformer trans, String source, IgniteKey<?> igniteKey) {
        if (trans == null) {
            logger.error("Ignite Transformer Implementation is missing for source or platform {} for key {}", 
                source, igniteKey);
            return false;
        }
        logger.info("Transforming kafka record with key {} to IgniteEvent for source or platform {} using {}", 
            igniteKey, source, trans.getClass().getName());
        return true;
    }
    
    /**
     * Checks if is ignite event not null.
     *
     * @param igniteEvent the ignite event
     * @param igniteKey the ignite key
     * @return true, if is ignite event not null
     */
    private boolean isIgniteEventNotNull(IgniteEvent igniteEvent, IgniteKey<?> igniteKey) {
        if (igniteEvent == null) {
            logger.error(
                    "Transformed IgniteEvent in ProtocolTranslatorPreProcessor cannot be null for key {}",
                    igniteKey);
            return false;
        }
        return true;
    }

    /**
     * Gets the ignite blob event.
     *
     * @param value the value
     * @param igniteKey the ignite key
     * @param blobEvent the blob event
     * @return the ignite blob event
     */
    private IgniteBlobEvent getIgniteBlobEvent(byte[] value, IgniteKey<?> igniteKey, IgniteBlobEvent blobEvent) {
        try {
            blobEvent = serializer.deserialize(value);
        } catch (Exception e) {
            logger.error("Unable to deserialize data into IgniteBlobEvent! Error is: {}", e);
        }
        if (blobEvent == null) {
            logger.error("Deserialized IgniteBlobEvent for key {} cannot be null", igniteKey);
            throw new IllegalArgumentException("Deserialized IgniteBlobEvent cannot be null for key " + igniteKey);
        }
        logger.debug("IgniteBlobEvent blobEvent {} after sniffing for key {}", blobEvent, igniteKey);
        return blobEvent;
    }

    /**
     * Gets the ignite device aware blob event.
     *
     * @param value the value
     * @param igniteKey the ignite key
     * @param igniteDeviceAwareBlobEvent the ignite device aware blob event
     * @return the ignite device aware blob event
     */
    private IgniteDeviceAwareBlobEvent getIgniteDeviceAwareBlobEvent(byte[] value, IgniteKey<?> igniteKey,
            IgniteDeviceAwareBlobEvent igniteDeviceAwareBlobEvent) {
        try {
            igniteDeviceAwareBlobEvent = (IgniteDeviceAwareBlobEvent) serializer.deserialize(value);
        } catch (Exception e) {
            logger.error("Blob event class type mismatch. Expected IgniteDeviceAwareBlobEvent!");
        }
        if (igniteDeviceAwareBlobEvent == null) {
            logger.error("Deserialized IgniteDeviceAwareBlobEvent for key {} cannot be null", igniteKey);
            throw new IllegalArgumentException("Deserialized IgniteDeviceAwareBlobEvent cannot be "
                    + "null for key " + igniteKey);
        }
        logger.debug("IgniteDeviceAwareBlobEvent igniteDeviceAwareBlobEvent {} after sniffing for key {}",
                igniteDeviceAwareBlobEvent, igniteKey);
        return igniteDeviceAwareBlobEvent;
    }
    
    /**
     * Creates the ignite event for platform if present.
     *
     * @param kafkaHeaders the kafka headers
     * @param value the value
     * @param igniteKey the ignite key
     * @return true, if successful
     */
    private boolean createIgniteEventForPlatformIfPresent(Map<String, String> kafkaHeaders, byte[] value, 
            IgniteKey<?> igniteKey) {
        Transformer trans = null;
        String platformId = null;
        if (null != kafkaHeaders && !StringUtils.isEmpty(kafkaHeaders.get(Constants.PLATFORM_ID))) {
            platformId = kafkaHeaders.get(Constants.PLATFORM_ID);
            logger.info("Retrieved platformId from kafka headers as {} for key {}", platformId, igniteKey);
        } else {
            String sourceTopicName = this.spc.streamName();
            logger.debug("Attempting to fetch platformID from prefix of source topic name : {}", sourceTopicName);
            if (StringUtils.isNotEmpty(sourceTopicName)) {
                platformId = getPlatformIdFromTopicPrefix(sourceTopicName);
            }
        }
        if (StringUtils.isNotBlank(platformId)) {
            trans = transformerMap.get(platformId);
            transformerSource = platformId;
            if (!isTransformerPresent(trans, platformId, igniteKey)) {
                return false;
            }
            igniteEvent = trans.fromBlob(value, igniteKey);
        } else {
            trans = transformerMap.get(IgniteEventSource.IGNITE);
            transformerSource = IgniteEventSource.IGNITE;
            if (!isTransformerPresent(trans, IgniteEventSource.IGNITE, igniteKey)) {
                return false;
            }
            igniteEvent = trans.fromBlob(value, Optional.empty());
        }
        logger.debug("IgniteEvent after transformation {} for key {}", igniteEvent, igniteKey);
        
        if (!isIgniteEventNotNull(igniteEvent, igniteKey)) {
            return false;
        }
        if (StringUtils.isNotBlank(igniteEvent.getPlatformId())) {
            return true;
        }
        if (StringUtils.isNotBlank(platformId)) {
            ((AbstractIgniteEvent) igniteEvent).setPlatformId(platformId);
        }
        return true;
    }
    
    /**
     * Gets the platform id from topic prefix.
     *
     * @param sourceTopicName the source topic name
     * @return the platform id from topic prefix
     */
    private String getPlatformIdFromTopicPrefix(String sourceTopicName) {
        
        String platformId = null;
        if (null == kafkaTopicNamePlatformPrefixes || kafkaTopicNamePlatformPrefixes.isEmpty()) {
            logger.debug("kafkaTopicNamePlatformPrefixes list is either null, or empty. "
                    + "No platformId found from Kafka topic name");
            return platformId;
        }
        for (String platformPrefix : kafkaTopicNamePlatformPrefixes) {
            if (!StringUtils.isEmpty(platformPrefix)) {
                logger.debug("Checking platformId in source topic name against prefix : {}", platformPrefix);
                if (sourceTopicName.toLowerCase().startsWith(platformPrefix.toLowerCase())) {
                    platformId = platformPrefix;
                    logger.info("Retrieved platformId from kafka topic name prefix as {}", platformId);
                    break;
                }
            }
        }
        return platformId;
    }

    /**
     * Verify and collect data consumption metric.
     *
     * @param igniteEvent the ignite event
     * @param igniteKey the ignite key
     */
    private void verifyAndCollectDataConsumptionMetric(IgniteEvent igniteEvent, IgniteKey<?> igniteKey) {
        //This is for capturing metrics only in Prometheus.
        if (enablePrometheus) {
            long diff = igniteEvent.getTimestamp() - System.currentTimeMillis();
            histogramInboundLatency.labels(serviceName, spc.getTaskID(), nodeName).observe(diff);
        }

        if (isEnableDuplicateMessageCheck) {
            boolean isDuplicate = duplicateMessgeFilteringAgent.isDuplicate(igniteKey, igniteEvent);
            ((AbstractIgniteEvent) igniteEvent).setDuplicateMessage(isDuplicate);
        }
        //This is for capturing the size metric in Kafka or Prometheus.
        if (isKafkaDataConsumptionMetricsEnabled || enablePrometheus) {
            collectDataConsumptionMetric(igniteKey, igniteEvent);
        }
    }

    /**
     * Sets the kafka headers if enabled.
     *
     * @param kafkaHeaders the kafka headers
     * @param igniteEvent the ignite event
     */
    private void setKafkaHeadersIfEnabled(Map<String, String> kafkaHeaders, AbstractIgniteEvent igniteEvent) {
        if (kafkaHeadersEnabled) {
            igniteEvent.setKafkaHeaders(kafkaHeaders);
        }
    }
    
    /**
     * Populate platform id from service impl.
     *
     * @param kafkaRecord the kafka record
     * @param igniteKey the ignite key
     */
    private void populatePlatformIdFromServiceImpl(Record<byte[], byte[]> kafkaRecord, IgniteKey<?> igniteKey) {
        if (StringUtils.isEmpty(platformIdServiceImpl)) {
            return;
        }
        IgnitePlatform ignitePlatform = null;
        try {
            ignitePlatform = (IgnitePlatform) platformUtils.getInstanceByClassName(platformIdServiceImpl);
        } catch (IllegalArgumentException e) {
            logger.error(PropertyNames.IGNITE_PLATFORM_SERVICE_IMPL_CLASS_NAME + " refers to a class that "
                    + "is not available on the classpath", e);
        }
        if (null == ignitePlatform) {
            return;
        }
        String platformId = ignitePlatform.getPlatformId(spc, new Record<>(igniteKey, igniteEvent, 
                kafkaRecord.timestamp()));
        logger.info("Platform ID fetched from class : {}  is : {}", platformIdServiceImpl, platformId);
        ((AbstractIgniteEvent) igniteEvent).setPlatformId(platformId);
    }
    
    /**
     * Populate vehicle id from vehicle profile api.
     */
    private void populateVehicleIdFromVehicleProfileApi() {

        String sourceDeviceId = igniteEvent.getSourceDeviceId();
        String platformId = igniteEvent.getPlatformId();
        if (vehicleProfilePlatformIds.contains(platformId) 
                && StringUtils.isNotEmpty(sourceDeviceId)
                && StringUtils.isEmpty(igniteEvent.getVehicleId())) {
            logger.debug("Fetching VehicleId from vehicle profile for sourceDeviceId {}, and platformID {} ", 
                    sourceDeviceId, platformId);
            String vehicleId = vehicleProfileClientApiUtil.callVehicleProfile(sourceDeviceId);
            logger.info("VehicleId {} is fetched from vehicle profile for deviceId {}, and platformID {}", 
                    vehicleId, sourceDeviceId, platformId);
            ((AbstractIgniteEvent) igniteEvent).setVehicleId(vehicleId);
        }
    }

    /**
     * Gets the kafka headers if enabled.
     *
     * @param requestId the request id
     * @param inputRecord the input record
     * @return the kafka headers if enabled
     */
    private Map<String, String> getKafkaHeadersIfEnabled(String requestId, Record<byte[], byte[]> inputRecord) {
        Map<String, String> kafkaHeaders = null;
        if (kafkaHeadersEnabled) {
            Headers headers = inputRecord.headers();
            logger.debug("Retreived headers for current kafka input record with requestID or igniteKey : {}, "
                    + "headers : {}", requestId, headers);
            if (headers != null) {
                kafkaHeaders = new HashMap<>();
                Iterator<Header> iterator = headers.iterator();
                while (iterator.hasNext()) {
                    Header header = iterator.next();
                    kafkaHeaders.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                }
            }
        }
        return kafkaHeaders;
    }
    
    /**
     * Collect data consumption metric.
     *
     * @param igniteKey the ignite key
     * @param igniteEvent the ignite event
     */
    private void collectDataConsumptionMetric(IgniteKey<?> igniteKey, IgniteEvent igniteEvent) {
        Transformer trans = transformerMap.get(transformerSource);
        // handle composite-event
        if (igniteEvent.getNestedEvents() != null && !igniteEvent.getNestedEvents().isEmpty()) {
            for (IgniteEvent event : igniteEvent.getNestedEvents()) {
                sampleEventSize(igniteKey, event, trans);
            }
        } else { 
            // handle standard-ignite-event
            sampleEventSize(igniteKey, igniteEvent, trans);
        }
    }

    /**
     * Sample event size.
     *
     * @param igniteKey the ignite key
     * @param igniteEvent the ignite event
     * @param trans the trans
     */
    private void sampleEventSize(IgniteKey<?> igniteKey, IgniteEvent igniteEvent, Transformer trans) {
        byte[] byteArray = null;
        if (enableDataConsumptionMetric || isKafkaDataConsumptionMetricsEnabled) {
            byteArray = trans.toBlob(igniteEvent);
        }
        //Capture only in Prometheus
        if (enableDataConsumptionMetric) {
            // size in KBs
            serviceDataConsumtionMetrics.labels(serviceName, igniteEvent.getEventId(), nodeName)
                .observe((double) byteArray.length / Constants.BYTE_1024);
        }
        //Capture only in Kafka.
        if (isKafkaDataConsumptionMetricsEnabled 
               && Stream.of(EventID.MSG_SEQ_BUF_FLUSH_EVENT, EventID.DFF_FEEDBACK_EVENT, 
                    EventID.DEVICEMESSAGEFAILURE, EventID.IGNITE_EXCEPTION_EVENT)
                        .noneMatch(igniteEvent.getEventId()::equalsIgnoreCase)) {
            //stream event size with eventID and vehicleID
            IgniteEvent dataUsageEvent = createDataUsageEvent(igniteEvent, 
                    (double) byteArray.length / Constants.BYTE_1024);
            spc.forwardDirectly(igniteKey, dataUsageEvent, dataUsageTopic);
        }
    }

    /**
     * Creates the data usage event.
     *
     * @param igniteEvent the ignite event
     * @param eventSize the event size
     * @return the ignite event
     */
    private IgniteEvent createDataUsageEvent(IgniteEvent igniteEvent, double eventSize) {
        IgniteEventImpl analyticsEvent = new IgniteEventImpl();
        analyticsEvent.setEventId(EventID.DATA_USAGE_EVENT);
        analyticsEvent.setTimestamp(System.currentTimeMillis());
        analyticsEvent.setVersion(Version.V1_0);
        DataUsageEventDataV1_0 analyticsEventData = new DataUsageEventDataV1_0();
        analyticsEventData.setEventName(igniteEvent.getEventId());
        analyticsEventData.setPayLoadSize(eventSize);
        analyticsEventData.setEventTimestamp(igniteEvent.getTimestamp());
        if (!StringUtils.isEmpty(igniteEvent.getVehicleId())) {
            analyticsEventData.setVehicleId(igniteEvent.getVehicleId());
        } else {
            analyticsEventData.setDeviceId(igniteEvent.getSourceDeviceId());
        }
        analyticsEvent.setEventData(analyticsEventData);
        return analyticsEvent;
    }


    /**
     * initConfig is used to initialize the properties and the transformers
     * Note IgniteKeyTransformer is mandatory.
     * Unlike IgniteEventTransformer and OemProtocolTransformer whose usage is
     * determined at runtime, IgniteKeyTransformer isn't hence if not
     * initialized error can be thrown and can exit before initializing.
     *
     * @param props the props
     */
    @Override
    public void initConfig(Properties props) {
        String transformerList = props.getProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES);
        String igniteKeyTransformerImpl = props.getProperty(PropertyNames.IGNITE_KEY_TRANSFORMER);
        String igniteSerializationImpl = props.getProperty(PropertyNames.INGESTION_SERIALIZER_CLASS);
        
        IgniteKeyTransformer<?> kt = null;

        if (StringUtils.isBlank(transformerList)) {
            logger.error("Event transformer list cannot be blank");
            throw new IllegalArgumentException("Event transformer list cannot be blank");
        }

        if (StringUtils.isBlank(igniteSerializationImpl)) {
            logger.error("Ignite event Serializer cannot be blank");
            throw new IllegalArgumentException("Ignite event Serializer cannot be blank");
        }

        if (StringUtils.isBlank(igniteKeyTransformerImpl)) {
            logger.error("Ignite key transformer cannot be blank");
            throw new IllegalArgumentException("Ignite key transformer cannot be blank");
        }

        logger.info("Event transformer List from property file : {}", transformerList);
        logger.info("Ignite key transformer from property file : {}", igniteKeyTransformerImpl);
        logger.info("Ignite event serializer from property file : {}", igniteSerializationImpl);

        serializer = IngestionSerializerFactory.getInstance(igniteSerializationImpl);
        // Initialize transformers from List
        String[] transformerArr = transformerList.split(",");
        boolean transformerInjectPropertyFlg = 
                Boolean.parseBoolean(props.getProperty(PropertyNames.TRANSFORMER_INJECT_PROPERTY_ENABLE));
        for (String transformer : transformerArr) {
            Transformer t = null;
            // Flag which will ensure that the instances of transformers
            // created via runtime class loader( Non Spring Based Bean
            // creation) will have the properties available to it via the
            // parameterized constructor.
            if (transformerInjectPropertyFlg) {
                try {
                    logger.info("Loading parameterized constructor for transformer");
                    t = (Transformer) ctx.getAutowireCapableBeanFactory().getBean(transformer, props);
                } catch (Exception ex) {
                    throw new IllegalArgumentException(
                            transformer + " parameterized constructor is not available to accept Properties.");
                }
            } else {
                t = (Transformer) ctx.getAutowireCapableBeanFactory().getBean(transformer);
            }
            logger.info("Adding entry in transformerMap for source : {}, and transformer : {}", 
                    t.getSource(), t.getClass().getName());
            transformerMap.put(t.getSource(), t);
        }

        // Initialize IgniteKeyTransformer is present
        // If an IgniteKeyTransformer is not present throw an exception and
        // exit.
        try {
            kt = (IgniteKeyTransformer) getClass().getClassLoader().loadClass(igniteKeyTransformerImpl)
                    .getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    PropertyNames.IGNITE_KEY_TRANSFORMER + " refers to a class that is not available on the classpath");
        } catch (InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException("Failed to invoke class " + PropertyNames.IGNITE_KEY_TRANSFORMER, e);
        }

        igniteKeyTransformer = Optional.ofNullable(kt);

    }

    /**
     * Punctuate.
     *
     * @param timestamp the timestamp
     */
    @Override
    public void punctuate(long timestamp) {
    //method
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        //method
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        // no additional configuration to be applied
    }

    /**
     * Creates the state store.
     *
     * @return the harman persistent KV store
     */
    @Override
    public HarmanPersistentKVStore createStateStore() {
        return null;
    }
    
    /**
     * Post initialization of this bean.
     */
    @PostConstruct
    public void initialize() {
        if (StringUtils.isEmpty(serviceName)) {
            throw new InvalidServiceNameException("Service Name is unavailable");
        }
    }
}