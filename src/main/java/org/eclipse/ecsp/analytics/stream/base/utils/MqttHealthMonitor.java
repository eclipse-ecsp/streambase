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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.healthcheck.HealthMonitor;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


/**
 * class MqttHealthMonitor implements HealthMonitor.
 */
@Component
public class MqttHealthMonitor implements HealthMonitor {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(MqttHealthMonitor.class);

    /** The mqtt health monitor enabled. */
    @Value("${" + PropertyNames.HEALTH_MQTT_MONITOR_ENABLED + ": true }")
    private boolean mqttHealthMonitorEnabled;

    /** The Constant HEALTH_MQTT_MONITOR_NAME. */
    public static final String HEALTH_MQTT_MONITOR_NAME = "MQTT_HEALTH_MONITOR";
    
    /** The Constant HEALTH_MQTT_MONTIOR_GUAGE. */
    public static final String HEALTH_MQTT_MONTIOR_GUAGE = "MQTT_HEALTH_GUAGE";

    /** The mqtt restart on failure. */
    @Value("${" + PropertyNames.HEALTH_MQTT_MONITOR_RESTART_ON_FAILURE + ": true }")
    private boolean mqttRestartOnFailure;

    /** The dispatchers. */
    private List<MqttDispatcher> dispatchers = new ArrayList<>();

    /**
     * Checks if is healthy.
     *
     * @param forceHealthCheck the force health check
     * @return true, if is healthy
     */
    @Override
    public boolean isHealthy(boolean forceHealthCheck) {
        if (dispatchers.isEmpty()) {
            logger.error("No mqtt dispatchers have been registered with MqttHealthMonitor. "
                    + "This error should be ignored if its part of initial health check.");
            return false;
        }
        boolean healthy = true;
        for (MqttDispatcher dispatcher : dispatchers) {
            healthy = healthy && dispatcher.isHealthy(forceHealthCheck);
        }
        return healthy;
    }

    /**
     * Monitor name.
     *
     * @return the string
     */
    @Override
    public String monitorName() {
        return HEALTH_MQTT_MONITOR_NAME;
    }

    /**
     * Needs restart on failure.
     *
     * @return true, if successful
     */
    @Override
    public boolean needsRestartOnFailure() {
        return mqttRestartOnFailure;
    }

    /**
     * Metric name.
     *
     * @return the string
     */
    @Override
    public String metricName() {
        return HEALTH_MQTT_MONTIOR_GUAGE;
    }

    /**
     * Checks if is enabled.
     *
     * @return true, if is enabled
     */
    @Override
    public boolean isEnabled() {
        return mqttHealthMonitorEnabled;
    }

    /**
     * Gets the dispatchers.
     *
     * @return the dispatchers
     */
    List<MqttDispatcher> getDispatchers() {
        return dispatchers;
    }

    /**
     * Register.
     *
     * @param dispatcher the dispatcher
     */
    public void register(MqttDispatcher dispatcher) {
        dispatchers.add(dispatcher);
    }

}
