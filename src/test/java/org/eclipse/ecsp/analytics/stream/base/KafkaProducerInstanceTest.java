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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaSslUtils;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;


/**
 * UT class {@link KafkaProducerInstanceTest}.
 */
public class KafkaProducerInstanceTest {

    /**
     * Test producer instance with ssl enabled.
     */
    @Test
    public void testProducerInstanceWithSslEnabled() {

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
        KafkaProducer<byte[], byte[]> kafkaProducer = KafkaProducerInstance.getProducerInstance(kafkaConfig);
        
        Assertions.assertNotNull(kafkaProducer);
    }
    
    /**
     * Test producer instance with one way tls enabled.
     */
    @Test
    public void testProducerInstanceWithOneWayTlsEnabled() {

        Properties kafkaConfig = new Properties();
        String kafkaBootstrapServers = "localhost:9092";
        String kafkaSslEnable = "false";
        String kafkaOneWayTlsEnable = "true";
        String truststore = "src/test/resources/kafka.client.truststore.jks";
        String truststorePwd = "password";
        String sslClientAuth = "none";
        String maxRequestSize = "1000012";
        String acksConfig = "1";
        String retriesConfig = "2147483647";
        String batchSizeConfig = "16384";
        String lingerMsConfig = "0";
        String bufferMemoryConfig = "33554432";
        String requestTimeoutMsConfig = "30000";
        String deliveryTimeoutMsConfig = "120000";
        String compressionTypeConfig = "none";
        String kafkaSaslMechanism = "PLAIN";
        String kafkaSaslJaasConfig = 
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" "
            + "password=\"password\";";
        kafkaConfig.put(PropertyNames.BOOTSTRAP_SERVERS, kafkaBootstrapServers);
        kafkaConfig.put(PropertyNames.KAFKA_SSL_ENABLE, kafkaSslEnable);
        kafkaConfig.put(PropertyNames.KAFKA_ONE_WAY_TLS_ENABLE, kafkaOneWayTlsEnable);
        kafkaConfig.put(PropertyNames.KAFKA_SASL_MECHANISM, kafkaSaslMechanism);
        kafkaConfig.put(PropertyNames.KAFKA_SASL_JAAS_CONFIG, kafkaSaslJaasConfig);
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
        KafkaProducer<byte[], byte[]> kafkaProducer = KafkaProducerInstance.getProducerInstance(kafkaConfig);
        
        Assertions.assertNotNull(kafkaProducer);
    }

}
