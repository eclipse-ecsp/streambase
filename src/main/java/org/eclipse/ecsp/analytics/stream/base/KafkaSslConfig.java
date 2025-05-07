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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaSslUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Spring bean to read and set properties for enabling SSL or SASL_SSL protocol for Kafka client.
 *
 * @author karora
 *
 */
@Component
public class KafkaSslConfig {

    /** The Constant LOGGER. */
    private static final IgniteLogger LOGGER = IgniteLoggerFactory.getLogger(KafkaSslConfig.class);

    /** Specifies whether SSL is enabled for Kafka. */
    @Value("${" + PropertyNames.KAFKA_SSL_ENABLE + ":false}")
    private boolean kafkaSslEnable;
    
    /** Specifies whether SASL is enabled for Kafka. */
    @Value("${" + PropertyNames.KAFKA_ONE_WAY_TLS_ENABLE + ":false}")
    private boolean kafkaOneWayTlsEnable;

    /** The path to Kafka client keystore. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_KEYSTORE + ":}")
    private String keystore;

    /** The Kafka client keystore password. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_KEYSTORE_PASSWORD + ":}")
    private String keystorePwd;

    /** The Kafka client key password. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_KEY_PASSWORD + ":}")
    private String keyPwd;

    /** The path to Kafka client trustore where server's public key details are located. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_TRUSTSTORE + ":}")
    private String truststore;

    /** The Kafka client truststore password. */
    @Value("${" + PropertyNames.KAFKA_CLIENT_TRUSTSTORE_PASSWORD + ":}")
    private String truststorePwd;

    /** The ssl client auth. */
    @Value("${" + PropertyNames.KAFKA_SSL_CLIENT_AUTH + ":}")
    private String sslClientAuth;
    
    /** The sasl mechanism. */
    @Value("${" + PropertyNames.KAFKA_SASL_MECHANISM + ":}")
    private String saslMechanism;
    
    /** The sasl jaas config. */
    @Value("${" + PropertyNames.KAFKA_SASL_JAAS_CONFIG + ":}")
    private String saslJaasConfig;
    
    /** The ssl endpoint algo. */
    @Value("${" + PropertyNames.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM + ":}")
    private String sslEndpointAlgo;
    
    /**
     * Utility method to set the required properties for enabling SASL_SSL or SSL, if enabled.
     * To enable SSL, set kafka.ssl.enable=true
     * To enable one way TLS with SASL, set kafka.one.way.tls.enable=true
     *
     * @param props The properties instance.
     */
    public void setSslPropsIfEnabled(Properties props) {
        if (kafkaSslEnable || kafkaOneWayTlsEnable) {
            LOGGER.info("SSL/TLS is enabled. Setting corresponding properties.");
            setSslProps(props);
        }
    }
    
    /**
     * Sets the SSL properties in the properties instance.
     *
     * @param targetProps The properties instance.
     */
    private void setSslProps(Properties targetProps) {
        Properties sourceProps = new Properties();
        if (kafkaOneWayTlsEnable) {
            sourceProps.put(PropertyNames.KAFKA_SASL_MECHANISM, saslMechanism);
            sourceProps.put(PropertyNames.KAFKA_SASL_JAAS_CONFIG, saslJaasConfig);
            sourceProps.put(PropertyNames.KAFKA_ONE_WAY_TLS_ENABLE, Constants.TRUE);
        }
        if (kafkaSslEnable) {
            sourceProps.put(PropertyNames.KAFKA_CLIENT_KEYSTORE, keystore);
            sourceProps.put(PropertyNames.KAFKA_CLIENT_KEYSTORE_PASSWORD, keystorePwd);
            sourceProps.put(PropertyNames.KAFKA_CLIENT_KEY_PASSWORD, keyPwd);
            sourceProps.put(PropertyNames.KAFKA_SSL_ENABLE, Constants.TRUE);
        }
        sourceProps.put(PropertyNames.KAFKA_CLIENT_TRUSTSTORE, truststore);
        sourceProps.put(PropertyNames.KAFKA_CLIENT_TRUSTSTORE_PASSWORD, truststorePwd);
        sourceProps.put(PropertyNames.KAFKA_SSL_CLIENT_AUTH, sslClientAuth);
        sourceProps.put(PropertyNames.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, sslEndpointAlgo);
        KafkaSslUtils.applySslProperties(targetProps, sourceProps);
    }
}