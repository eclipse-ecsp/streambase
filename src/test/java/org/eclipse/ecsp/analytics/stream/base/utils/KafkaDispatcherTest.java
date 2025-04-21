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

package org.eclipse.ecsp.analytics.stream.base.utils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.ecsp.analytics.stream.base.KafkaProducerInstance;
import org.eclipse.ecsp.analytics.stream.base.KafkaSslConfig;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageDispatchers;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.handler.DefaultPostDispatchHandler;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageHandler;
import org.eclipse.ecsp.transform.IgniteKeyTransformer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;


/**
 * UT class {@link KafkaDispatcherTest}.
 */
public class KafkaDispatcherTest {
    
    /** The dispatcher. */
    @InjectMocks
    KafkaDispatcher dispatcher = new KafkaDispatcher();

    /** The key transformer. */
    @Mock
    IgniteKeyTransformer<String> keyTransformer;

    /** The kafka producer. */
    @Mock
    KafkaProducer<byte[], byte[]> kafkaProducer;

    /** The post dispatch handler. */
    @Mock
    DeviceMessageHandler postDispatchHandler;

    /** The kafka producer instance. */
    @Mock
    private KafkaProducerInstance kafkaProducerInstance;

    /** The kafka headers. */
    private Map<String, String> kafkaHeaders;

    /** The spc. */
    @Mock
    private StreamProcessingContext spc;
    
    /** The kafka ssl config. */
    @Mock
    private KafkaSslConfig kafkaSslConfig;

    /**
     * setup().
     */
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        Properties kafkaConfig = new Properties();
        String kafkaBootstrapServers = "localhost:9092";
        String kafkaSslEnable = "true";
        String keystore = "src/test/resources/kafka.client.keystore.jks";
        String keystorePwd = "password";
        String keyPwd = "password";
        String truststore = "src/test/resources/kafka.client.truststore.jks";
        String truststorePwd = "password";
        String sslClientAuth = "required";
        String maxRequestSize = "1000012";
        String acksConfig = "1";
        String retriesConfig = "2147483647";
        String batchSizeConfig = "16384";
        String lingerMsConfig = "0";
        String bufferMemoryConfig = "33554432";
        String requestTimeoutMsConfig = "30000";
        String deliveryTimeoutMsConfig = "120000";
        String compressionTypeConfig = "none";

        kafkaConfig.put(PropertyNames.BOOTSTRAP_SERVERS, kafkaBootstrapServers);
        kafkaConfig.put(PropertyNames.KAFKA_SSL_ENABLE, kafkaSslEnable);
        kafkaConfig.put(PropertyNames.KAFKA_CLIENT_KEYSTORE, keystore);
        kafkaConfig.put(PropertyNames.KAFKA_CLIENT_KEYSTORE_PASSWORD, keystorePwd);
        kafkaConfig.put(PropertyNames.KAFKA_CLIENT_KEY_PASSWORD, keyPwd);
        kafkaConfig.put(PropertyNames.KAFKA_CLIENT_TRUSTSTORE, truststore);
        kafkaConfig.put(PropertyNames.KAFKA_CLIENT_TRUSTSTORE_PASSWORD, truststorePwd);
        kafkaConfig.put(PropertyNames.KAFKA_SSL_CLIENT_AUTH, sslClientAuth);
        kafkaConfig.put(PropertyNames.KAFKA_MAX_REQUEST_SIZE, maxRequestSize);
        kafkaConfig.put(PropertyNames.KAFKA_ACKS_CONFIG, acksConfig);
        kafkaConfig.put(PropertyNames.KAFKA_RETRIES_CONFIG, retriesConfig);
        kafkaConfig.put(PropertyNames.KAFKA_BATCH_SIZE_CONFIG, batchSizeConfig);
        kafkaConfig.put(PropertyNames.KAFKA_LINGER_MS_CONFIG, lingerMsConfig);
        kafkaConfig.put(PropertyNames.KAFKA_BUFFER_MEMORY_CONFIG, bufferMemoryConfig);
        kafkaConfig.put(PropertyNames.KAFKA_REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMsConfig);
        kafkaConfig.put(PropertyNames.KAFKA_DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMsConfig);
        kafkaConfig.put(PropertyNames.KAFKA_COMPRESSION_TYPE_CONFIG, compressionTypeConfig);
        
        KafkaSslUtils.checkAndApplySslProperties(kafkaConfig);

        ReflectionTestUtils.setField(dispatcher, "kafkaBootstrapServers", kafkaBootstrapServers);
        ReflectionTestUtils.setField(dispatcher, "maxRequestSize", maxRequestSize);
        ReflectionTestUtils.setField(dispatcher, "acksConfig", acksConfig);
        ReflectionTestUtils.setField(dispatcher, "retriesConfig", retriesConfig);
        ReflectionTestUtils.setField(dispatcher, "batchSizeConfig", batchSizeConfig);
        ReflectionTestUtils.setField(dispatcher, "lingerMsConfig", lingerMsConfig);
        ReflectionTestUtils.setField(dispatcher, "bufferMemoryConfig", bufferMemoryConfig);
        ReflectionTestUtils.setField(dispatcher, "requestTimeoutMsConfig", requestTimeoutMsConfig);
        ReflectionTestUtils.setField(dispatcher, "deliveryTimeoutMsConfig", deliveryTimeoutMsConfig);
        ReflectionTestUtils.setField(dispatcher, "compressionTypeConfig", compressionTypeConfig);
        ReflectionTestUtils.setField(dispatcher, "kafkaSslConfig", kafkaSslConfig);

        // Mockito can't mock static methods, so initialising explicitly
        kafkaProducerInstance.getProducerInstance(kafkaConfig);

        kafkaHeaders = new HashMap<>();
        kafkaHeaders.put("header_key_1", "header_value_1");
        kafkaHeaders.put("header_key_2", "header_value_2");
        kafkaHeaders.put("header_key_3", "header_value_3");
    }

    /**
     * Test dispatch.
     */
    /*
     * Happy flow. When IgniteKey and DeviceMessage are being passed to KafkaDispatcher,
     * then the DeviceMessage is getting dispatched to the configured kafka topic.
     */
    @Test
    public void testDispatch() {
        String msg = "message";
        TestEvent igniteEvent = new TestEvent();
        DeviceMessage message = new DeviceMessage(msg.getBytes(), Version.V1_0,
                igniteEvent, "topic", Constants.THREAD_SLEEP_TIME_60000);
        message.setEvent(igniteEvent);

        Map<String, Map<String, String>> brokerToEcuTypesMapping = new HashMap<>();
        Map<String, String> ecuTypesMap = new HashMap<>();
        ecuTypesMap.put("testecu", "testTopic");
        brokerToEcuTypesMapping.put("kafka", ecuTypesMap);

        ReflectionTestUtils.setField(dispatcher, "brokerToEcuTypesMapping", brokerToEcuTypesMapping);
        ReflectionTestUtils.setField(dispatcher, "keyTransformer", keyTransformer);
        ReflectionTestUtils.setField(dispatcher, "kafkaProducer", kafkaProducer);
        ReflectionTestUtils.setField(dispatcher, "dmaPostDispatchHandler", postDispatchHandler);

        Mockito.when(keyTransformer.toBlob(any(IgniteKey.class)))
                .thenReturn(new byte[Constants.BYTE_1024]);

        TestKey igniteKey = new TestKey();
        dispatcher.dispatch(igniteKey, message);

        verify(kafkaProducer, Mockito.times(1)).send(any(ProducerRecord.class), any(Callback.class));
        verify(postDispatchHandler, Mockito.times(1)).handle(igniteKey, message);
    }

    /**
     * Test init.
     */
    @Test
    public void testInit() {
        ReflectionTestUtils.invokeMethod(dispatcher, "init", new Object[0]);
        assertNotNull(ReflectionTestUtils.getField(dispatcher, "kafkaProducer"));
    }

    /**
     * Test setup.
     */
    @Test
    public void testSetup() {
        ReflectionTestUtils.setField(dispatcher, "brokerToEcuTypesMapping", new HashMap<>());
        assertNotNull(ReflectionTestUtils.getField(dispatcher, "brokerToEcuTypesMapping"));
    }

    /**
     * Test set next handler.
     */
    @Test
    public void testSetNextHandler() {
        DeviceMessageHandler handler = new DefaultPostDispatchHandler();
        ReflectionTestUtils.setField(dispatcher, "dmaPostDispatchHandler", handler);
        DeviceMessageHandler postDispatchHandler = (DeviceMessageHandler) ReflectionTestUtils.getField(dispatcher,
                "dmaPostDispatchHandler");
        assertNotNull(postDispatchHandler);
    }

    /**
     * Test dispatch with null key.
     */
    @Test(expected = java.lang.AssertionError.class)
    public void testDispatchWithNullKey() {
        String msg = "message";
        TestEvent igniteEvent = new TestEvent();
        DeviceMessage message = new DeviceMessage(msg.getBytes(), Version.V1_0,
                igniteEvent, "topic", Constants.THREAD_SLEEP_TIME_60000);
        message.setEvent(igniteEvent);

        dispatcher.dispatch(null, message);
        Assert.fail("Key is NULL. Not dispatching the data to Kafka");
    }

    /**
     * Test dispatch with null value.
     */
    @Test(expected = java.lang.AssertionError.class)
    public void testDispatchWithNullValue() {
        IgniteKey key = new TestKey();
        dispatcher.dispatch(key, null);
        Assert.fail("Value is NULL. Not dispatching the data to Kafka");
    }

    /**
     * test for setup().
     */
    @Test
    public void testSetupMethod() {
        Map brokerToEcuTypesMapping = new HashMap<String, Map<String, String>>();
        brokerToEcuTypesMapping.put(DeviceMessageDispatchers.KAFKA, new HashMap<>());
        dispatcher.setup(brokerToEcuTypesMapping, spc);
        verify(kafkaProducerInstance, Mockito.times(1)).getProducerInstance(any());
    }

    /**
     * inner class TestKey implements IgniteKey.
     */
    public class TestKey implements IgniteKey<String> {

        /**
         * getKey().
         *
         * @return String
         */
        @Override
        public String getKey() {
            return "vin123";
        }

        /**
         * To string.
         *
         * @return the string
         */
        @Override
        public String toString() {
            return "vin123";
        }
    }

    /**
     * class TestEvent extends IgniteEventImpl.
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
         * Gets the event id.
         *
         * @return the event id
         */
        @Override
        public String getEventId() {
            return "Sample";
        }

        /**
         * Gets the ecu type.
         *
         * @return the ecu type
         */
        @Override
        public String getEcuType() {
            return "testecu";
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

        /**
         * Gets the kafka headers.
         *
         * @return the kafka headers
         */
        @Override
        public Map<String, String> getKafkaHeaders() {
            return kafkaHeaders;
        }
    }
}
