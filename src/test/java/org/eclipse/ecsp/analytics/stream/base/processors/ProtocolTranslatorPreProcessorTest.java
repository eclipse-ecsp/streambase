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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.platform.IgnitePlatform;
import org.eclipse.ecsp.analytics.stream.base.platform.utils.PlatformUtils;
import org.eclipse.ecsp.analytics.stream.vehicleprofile.utils.VehicleProfileClientApiUtil;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.IgniteEventSource;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.transform.Transformer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;



/**
 * {@link ProtocolTranslatorPreProcessorTest}.
 */
public class ProtocolTranslatorPreProcessorTest {

    /** The props. */
    Properties props;
    
    /** The spc. */
    @Mock
    StreamProcessingContext spc;
    
    /** The transformer. */
    @Mock
    Transformer transformer;
    
    /** The autowire capable bean factory. */
    @Mock
    AutowireCapableBeanFactory autowireCapableBeanFactory;
    
    /** The protocol translator pre processor. */
    @InjectMocks
    private ProtocolTranslatorPreProcessor protocolTranslatorPreProcessor;
    
    /** The ctx. */
    @Mock
    private ApplicationContext ctx;
    
    /** The messge filter agent. */
    @Mock
    private MessgeFilterAgent messgeFilterAgent;

    /** The platform utils. */
    @Mock
    private PlatformUtils platformUtils;

    /** The vehicle profile client api util. */
    @Mock
    private VehicleProfileClientApiUtil vehicleProfileClientApiUtil;

    /**
     * setup().
     *
     * @throws Exception Exception
     */

    @Before
    public void setUp() throws Exception {
        props = new Properties();
        MockitoAnnotations.openMocks(this);
    }

    /**
     * Test init config exception 1.
     */
    //Event transformer list cannot be
    @Test(expected = IllegalArgumentException.class)
    public void testInitConfigException1() {
        ProtocolTranslatorPreProcessorTest.TestEvent event = new ProtocolTranslatorPreProcessorTest.TestEvent();
        event.setEventId(EventID.DELETE_SCHEDULE_EVENT);
        protocolTranslatorPreProcessor.initConfig(props);
    }

    /**
     * Test init config exception 2.
     */
    //Ignite event Serializer cannot be blank
    @Test(expected = IllegalArgumentException.class)
    public void testInitConfigException2() {
        ProtocolTranslatorPreProcessorTest.TestEvent event = new ProtocolTranslatorPreProcessorTest.TestEvent();
        event.setEventId(EventID.DELETE_SCHEDULE_EVENT);
        props.setProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES, "genericIgniteEventTransformer");
        protocolTranslatorPreProcessor.initConfig(props);
    }

    /**
     * Test init config exception 3.
     */
    //Ignite key transformer cannot be blank
    @Test(expected = IllegalArgumentException.class)
    public void testInitConfigException3() {
        ProtocolTranslatorPreProcessorTest.TestEvent event = new ProtocolTranslatorPreProcessorTest.TestEvent();
        event.setEventId(EventID.DELETE_SCHEDULE_EVENT);
        props.setProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES,
                "genericIgniteEventTransformer");
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS,
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        protocolTranslatorPreProcessor.initConfig(props);
    }

    /**
     * Test init config exception 4.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInitConfigException4() {
        ProtocolTranslatorPreProcessorTest.TestEvent event =
                new ProtocolTranslatorPreProcessorTest.TestEvent();
        event.setEventId(EventID.DELETE_SCHEDULE_EVENT);
        props.setProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES,
                "genericIgniteEventTransformer");
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS,
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        props.setProperty(PropertyNames.IGNITE_KEY_TRANSFORMER,
                "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        props.setProperty(PropertyNames.TRANSFORMER_INJECT_PROPERTY_ENABLE, "true");
        protocolTranslatorPreProcessor.initConfig(props);
    }

    /**
     * Test init method.
     */
    @Test()
    public void testInitMethod() {
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "isEnableDuplicateMessageCheck", true);
        when(ctx.getBean(MessgeFilterAgent.class)).thenReturn(messgeFilterAgent);
        protocolTranslatorPreProcessor.init(spc);
        verify(ctx).getBean(MessgeFilterAgent.class);
        Assert.notNull(ctx.getBean(MessgeFilterAgent.class), "MessgeFilterAgent must not be null");
    }

    /**
     * Test process method with null key.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testProcessMethodWithNullKey() {
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "isKafkaDataConsumptionMetricsEnabled", true);
        protocolTranslatorPreProcessor.process(null);
    }

    /**
     * Test process method with null ignite key.
     */
    @Test(expected = RuntimeException.class)
    public void testProcessMethodWithNullIgniteKey() {
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "isKafkaDataConsumptionMetricsEnabled", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "igniteKeyTransformer", Optional.ofNullable(null));
        IgniteKey igniteKey = new TestKey();
        IgniteEvent igniteEvent = new IgniteEventImpl();
        Record<byte[], byte[]> kafkaRecord =
                new Record<>(igniteKey.toString().getBytes(), igniteEvent.toString().getBytes(),
                System.currentTimeMillis());
        protocolTranslatorPreProcessor.process(kafkaRecord);
    }

    /**
     * Test kafka header method.
     */
    @Test
    public void testKafkaHeaderMethod() {
        Map map = new HashMap<>();
        map.put(IgniteEventSource.IGNITE, transformer);
        IgniteEventImpl igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");
        List<String> vpPlatformIds = new ArrayList<>();
        vpPlatformIds.add("platform1");
        vpPlatformIds.add("platform2");
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, ""
                + "vehicleProfilePlatformIds", vpPlatformIds);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "isKafkaDataConsumptionMetricsEnabled", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "kafkaHeadersEnabled", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "ctx", ctx);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "deviceAwareEnable", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "transformerMap", map);
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS, 
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        props.setProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES, "genericIgniteEventTransformer");
        props.setProperty(PropertyNames.IGNITE_KEY_TRANSFORMER, 
                "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        props.setProperty(PropertyNames.TRANSFORMER_INJECT_PROPERTY_ENABLE, "true");
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS,
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        when(ctx.getAutowireCapableBeanFactory()).thenReturn(autowireCapableBeanFactory);
        when(transformer.fromBlob(any(), any(Optional.class))).thenReturn(igniteEvent);
        when(transformer.toBlob(any())).thenReturn(igniteEvent.toString().getBytes());
        when(autowireCapableBeanFactory.getBean(anyString(), any(Properties.class))).thenReturn(transformer);
        protocolTranslatorPreProcessor.initConfig(props);
        IgniteKey igniteKey = new TestKey();
        Record<byte[], byte[]> kafkaRecord =
                new Record<>(igniteKey.toString().getBytes(), igniteEvent.toString().getBytes(),
                System.currentTimeMillis());
        protocolTranslatorPreProcessor.process(kafkaRecord);
        verify(spc, Mockito.times(1)).forward(any());
    }

    /**
     * Test platform kafka header method.
     */
    @Test
    public void testPlatformKafkaHeaderMethod() {
        String kafkaHeaderKey = "platformId";
        String kafkaHeaderValue = "testPlatform";
        List<Header> kafkaHeaders = new ArrayList<>();
        kafkaHeaders.add(new RecordHeader(kafkaHeaderKey, kafkaHeaderValue.getBytes(StandardCharsets.UTF_8)));
        
        Map<String, Transformer> map = new HashMap<>();
        map.put(IgniteEventSource.IGNITE, transformer);
        map.put("testPlatform", transformer);
        IgniteEventImpl igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");
        List<String> kafkaTopicNamePlatformPrefixes = new ArrayList<>();
        kafkaTopicNamePlatformPrefixes.add("testPlatform");
        List<String> vpPlatformIds = new ArrayList<>();
        vpPlatformIds.add("platform1");
        vpPlatformIds.add("platform2");
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "vehicleProfilePlatformIds", vpPlatformIds);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "isKafkaDataConsumptionMetricsEnabled", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "kafkaHeadersEnabled", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "ctx", ctx);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "deviceAwareEnable", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "transformerMap", map);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, 
                "kafkaTopicNamePlatformPrefixes", kafkaTopicNamePlatformPrefixes);
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS, 
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        props.setProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES, 
                "genericEventTransformer,somePlatformEventTransformer");
        props.setProperty(PropertyNames.IGNITE_KEY_TRANSFORMER, 
                "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        props.setProperty(PropertyNames.TRANSFORMER_INJECT_PROPERTY_ENABLE, "true");
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS,
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        when(ctx.getAutowireCapableBeanFactory()).thenReturn(autowireCapableBeanFactory);
        when(spc.streamName()).thenReturn(null);
        when(transformer.fromBlob(any(), any(IgniteKey.class))).thenReturn(igniteEvent);
        when(transformer.toBlob(any())).thenReturn(igniteEvent.toString().getBytes());
        when(autowireCapableBeanFactory.getBean(anyString(), any(Properties.class))).thenReturn(transformer);
        protocolTranslatorPreProcessor.initConfig(props);
        IgniteKey igniteKey = new TestKey();
        Headers header = new RecordHeaders(kafkaHeaders);
        Record<byte[], byte[]> kafkaRecord =
                new Record<>(igniteKey.toString().getBytes(), igniteEvent.toString().getBytes(),
                System.currentTimeMillis(), header);
        protocolTranslatorPreProcessor.process(kafkaRecord);
        verify(spc, Mockito.times(1)).forward(any());
    }

    /**
     * Test platform kafka topic.
     */
    @Test
    public void testPlatformKafkaTopic() {
        Map<String, Transformer> map = new HashMap<>();
        map.put(IgniteEventSource.IGNITE, transformer);
        map.put("platform1", transformer);
        IgniteEventImpl igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");
        List<String> kafkaTopicNamePlatformPrefixes = new ArrayList<>();
        kafkaTopicNamePlatformPrefixes.add("platform1");
        List<String> vpPlatformIds = new ArrayList<>();
        vpPlatformIds.add("platform1");
        vpPlatformIds.add("platform2");
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "vehicleProfilePlatformIds", vpPlatformIds);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "isKafkaDataConsumptionMetricsEnabled", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "kafkaHeadersEnabled", false);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "ctx", ctx);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "deviceAwareEnable", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "transformerMap", map);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "kafkaTopicNamePlatformPrefixes", kafkaTopicNamePlatformPrefixes);
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS, 
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        props.setProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES, 
                "genericEventTransformer,somePlaformEventTransformer");
        props.setProperty(PropertyNames.IGNITE_KEY_TRANSFORMER, 
                "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        props.setProperty(PropertyNames.TRANSFORMER_INJECT_PROPERTY_ENABLE, "true");
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS,
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        when(ctx.getAutowireCapableBeanFactory()).thenReturn(autowireCapableBeanFactory);
        when(spc.streamName()).thenReturn("platform1-test-topic");
        when(transformer.fromBlob(any(), any(IgniteKey.class))).thenReturn(igniteEvent);
        when(transformer.toBlob(any())).thenReturn(igniteEvent.toString().getBytes());
        when(autowireCapableBeanFactory.getBean(anyString(), any(Properties.class))).thenReturn(transformer);
        protocolTranslatorPreProcessor.initConfig(props);
        IgniteKey igniteKey = new TestKey();
        Record<byte[], byte[]> kafkaRecord =
                new Record<>(igniteKey.toString().getBytes(), igniteEvent.toString().getBytes(),
                System.currentTimeMillis());
        protocolTranslatorPreProcessor.process(kafkaRecord);
        verify(spc, Mockito.times(1)).forward(any());
    }
    
    /**
     * Test platform service implementation.
     */
    public void testPlatformServiceImpl() {
        Map<String, Transformer> map = new HashMap<>();
        map.put(IgniteEventSource.IGNITE, transformer);
        map.put("platform1", transformer);
        map.put("platform2", transformer);
        IgniteEventImpl igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");
        String sourceDeviceId = "test123";
        igniteEvent.setSourceDeviceId(sourceDeviceId);
        List<String> kafkaTopicNamePlatformPrefixes = new ArrayList<>();
        kafkaTopicNamePlatformPrefixes.add("platform2");
        List<String> vpPlatformIds = new ArrayList<>();
        vpPlatformIds.add("platform1");
        vpPlatformIds.add("platform2");
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "vehicleProfilePlatformIds", vpPlatformIds);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "isKafkaDataConsumptionMetricsEnabled", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "kafkaHeadersEnabled", false);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "ctx", ctx);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "deviceAwareEnable", true);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "transformerMap", map);
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor,
                "kafkaTopicNamePlatformPrefixes", kafkaTopicNamePlatformPrefixes);
        IgnitePlatformImpl platformImpl = new IgnitePlatformImpl();
        ReflectionTestUtils.setField(protocolTranslatorPreProcessor, "platformIdServiceImpl",
                platformImpl.getClass().getCanonicalName());
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS, 
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        props.setProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES, 
                "genericEventTransformer,somePlatformEventTransformer");
        props.setProperty(PropertyNames.IGNITE_KEY_TRANSFORMER, 
                "org.eclipse.ecsp.transform.IgniteKeyTransformerStringImpl");
        props.setProperty(PropertyNames.TRANSFORMER_INJECT_PROPERTY_ENABLE, "true");
        props.setProperty(PropertyNames.INGESTION_SERIALIZER_CLASS,
                "org.eclipse.ecsp.serializer.IngestionSerializerFstImpl");
        when(ctx.getAutowireCapableBeanFactory()).thenReturn(autowireCapableBeanFactory);
        when(spc.streamName()).thenReturn("platform-test-topic");
        when(transformer.fromBlob(any(), any(IgniteKey.class))).thenReturn(igniteEvent);
        when(transformer.toBlob(any())).thenReturn(igniteEvent.toString().getBytes());
        when(autowireCapableBeanFactory.getBean(anyString(), any(Properties.class))).thenReturn(transformer);
        when(platformUtils.getInstanceByClassName(platformImpl.getClass().getCanonicalName())).thenReturn(platformImpl);
        when(vehicleProfileClientApiUtil.callVehicleProfile(sourceDeviceId)).thenReturn("VIN123");
        protocolTranslatorPreProcessor.initConfig(props);
        IgniteKey igniteKey = new TestKey();
        Record<byte[], byte[]> kafkaRecord =
                new Record<>(igniteKey.toString().getBytes(), igniteEvent.toString().getBytes(),
                System.currentTimeMillis());
        protocolTranslatorPreProcessor.process(kafkaRecord);
        verify(spc, Mockito.times(1)).forward(any());
    }

    /**
     * The Class TestKey.
     */
    private class TestKey implements IgniteKey<String> {
        
        /**
         * Gets the key.
         *
         * @return the key
         */
        @Override
        public String getKey() {

            return "Vehicle12345";
        }
    }

    /**
     * inner class TestEvent extends IgniteEventImpl.
     */
    public class TestEvent extends IgniteEventImpl {
        
        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /**
         * Instantiates a new test event.
         */
        public TestEvent() {
        }

        /**
         * Gets the target device id.
         *
         * @return the target device id
         */
        @Override
        public Optional<String> getTargetDeviceId() {
            return Optional.of("test");
        }
    }

    /**
     * The Class IgnitePlatformImpl.
     */
    private class IgnitePlatformImpl implements IgnitePlatform {

        /**
         * Gets the platform id.
         *
         * @param cxt the cxt
         * @param arg0 the arg 0
         * @return the platform id
         */
        @Override
        public String getPlatformId(StreamProcessingContext<IgniteKey<?>, IgniteEvent> cxt, 
                Record<IgniteKey<?>, IgniteEvent> arg0) {
            return "testPlatform";
        }
    }
}

