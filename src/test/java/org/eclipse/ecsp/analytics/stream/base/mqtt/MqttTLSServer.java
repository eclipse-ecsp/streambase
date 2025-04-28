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

package org.eclipse.ecsp.analytics.stream.base.mqtt;

import io.moquette.broker.Server;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.junit.rules.ExternalResource;
import java.io.File;



/**
 * Embedded MQTT Server with TLS Enabled.
 */
public class MqttTLSServer extends ExternalResource {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(MqttTLSServer.class);

    /** The mqtt server. */
    private final Server mqttServer;
    
    /** The config file. */
    private final String configFile;
    
    /**
     * Initialize the server.
     */
    public MqttTLSServer() {
        logger.info("Loading mqtt Server Config:{}", "/mqtt_ssl.conf");
        this.mqttServer = new Server();
        this.configFile = this.getClass().getResource("/mqtt_ssl.conf").getFile();
    }

    /**
     * Before.
     *
     * @throws Throwable the throwable
     */
    @Override
    protected void before() throws Throwable {
        logger.info("Starting (Embedded) Mqtt Server.");
        this.mqttServer.startServer(new File(configFile));
    }

    /**
     * After.
     */
    @Override
    protected void after() {
        logger.info("Stopping (Embedded) Mqtt Server.");
        this.mqttServer.stopServer();
    }
}
