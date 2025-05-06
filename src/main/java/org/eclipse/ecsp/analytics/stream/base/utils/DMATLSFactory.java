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

import org.eclipse.ecsp.analytics.stream.base.exception.DeviceMessagingMqttClientTrustStoreException;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is responsible for instantiating KeyStore(s). A KeyStore instance
 * can contain either information about just the key-store or information about
 * both, the key-store as well as the trust-store, depending upon whether the
 * authentication mechanism is one-way-tls or two-way-tls.
 *
 * @author NeKhan
 */

public class DMATLSFactory {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DMATLSFactory.class);
    
    /** The platform trust manager map. */
    private static ConcurrentHashMap<String, TrustManagerFactory> platformTrustManagerMap = new ConcurrentHashMap<>();

    /**
     * Gets the trust manager factory.
     *
     * @param platformId the platform id
     * @param mqttConfig the mqtt config
     * @return the trust manager factory
     */
    public static TrustManagerFactory getTrustManagerFactory(String platformId, MqttConfig mqttConfig) {
        platformTrustManagerMap.putIfAbsent(platformId, createTrustManagerFactory(mqttConfig));
        return platformTrustManagerMap.get(platformId);
    }
    
    /**
     * Instantiates a new DMATLS factory.
     */
    private DMATLSFactory() {
        //Default private constructor to avoid instantiating it using new operator.
    }
    
    /**
     * Initializes {@link TrustManagerFactory}.
     *
     * @param mqttConfig {@link MqttConfig}
     * @return TrustManagerFactory instance.
     */
    public static TrustManagerFactory createTrustManagerFactory(MqttConfig mqttConfig) {
        try {
            KeyStore trustStore = KeyStore.getInstance(mqttConfig.getMqttServiceTrustStoreType());
            InputStream inputStream = new FileInputStream(mqttConfig.getMqttServiceTrustStorePath());
            trustStore.load(inputStream, mqttConfig.getMqttServiceTrustStorePassword().toCharArray());
            logger.info("TrustStore: {} loaded successfully.", mqttConfig.getMqttServiceTrustStorePath());

            TrustManagerFactory trustManagerFactory = TrustManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            logger.info("TrustManagerFactory successfully instantiated using algorithm: {}", 
                    TrustManagerFactory.getDefaultAlgorithm());
            return trustManagerFactory;
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException exception) {
            throw new DeviceMessagingMqttClientTrustStoreException(
                "Error encountered either while loading the TrustStore: " + mqttConfig.getMqttServiceTrustStorePath()
                    + " with truststore type: " + mqttConfig.getMqttServiceTrustStoreType() + " or in "
                            + "initializing the TrustManagerFactory. Exception is: " + exception,
                exception.getCause());
        }
    }
}