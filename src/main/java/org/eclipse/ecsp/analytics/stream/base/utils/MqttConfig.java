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


/**
 * Class for all the configurations for an MqttClient.
 */
public class MqttConfig {

    /** The mqtt user name. */
    private String mqttUserName;
    
    /** The mqtt user password. */
    private String mqttUserPassword;
    
    /** The clean session. */
    private boolean cleanSession;
    
    /** The keep alive interval. */
    private int keepAliveInterval;
    
    /** The max inflight. */
    private int maxInflight;
    
    /** The broker url. */
    private String brokerUrl;
    
    /** The mqtt broker port. */
    private int mqttBrokerPort;
    
    /** The mqtt timeout in millis. */
    private int mqttTimeoutInMillis;
    
    /** The mqtt qos value. */
    private int mqttQosValue;
    
    /** The mqtt client auth mechanism. */
    private String mqttClientAuthMechanism;
    
    /** The mqtt service trust store path. */
    private String mqttServiceTrustStorePath;

    /** The mqtt service trust store password. */
    private String mqttServiceTrustStorePassword;

    /** The mqtt service trust store type. */
    private String mqttServiceTrustStoreType;

    /**
     * Gets the mqtt user name.
     *
     * @return the mqtt user name
     */
    public String getMqttUserName() {
        return mqttUserName;
    }

    /**
     * Sets the mqtt user name.
     *
     * @param mqttUserName the new mqtt user name
     */
    public void setMqttUserName(String mqttUserName) {
        this.mqttUserName = mqttUserName;
    }

    /**
     * Gets the mqtt user password.
     *
     * @return the mqtt user password
     */
    public String getMqttUserPassword() {
        return mqttUserPassword;
    }

    /**
     * Sets the mqtt user password.
     *
     * @param mqttUserPassword the new mqtt user password
     */
    public void setMqttUserPassword(String mqttUserPassword) {
        this.mqttUserPassword = mqttUserPassword;
    }

    /**
     * Checks if is clean session.
     *
     * @return true, if is clean session
     */
    public boolean isCleanSession() {
        return cleanSession;
    }

    /**
     * Sets the clean session.
     *
     * @param cleanSession the new clean session
     */
    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    /**
     * Gets the keep alive interval.
     *
     * @return the keep alive interval
     */
    public int getKeepAliveInterval() {
        return keepAliveInterval;
    }

    /**
     * Sets the keep alive interval.
     *
     * @param keepAliveInterval the new keep alive interval
     */
    public void setKeepAliveInterval(int keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    /**
     * Gets the max inflight.
     *
     * @return the max inflight
     */
    public int getMaxInflight() {
        return maxInflight;
    }

    /**
     * Sets the max inflight.
     *
     * @param maxInflight the new max inflight
     */
    public void setMaxInflight(int maxInflight) {
        this.maxInflight = maxInflight;
    }

    /**
     * Gets the broker url.
     *
     * @return the broker url
     */
    public String getBrokerUrl() {
        return brokerUrl;
    }

    /**
     * Sets the broker url.
     *
     * @param brokerUrl the new broker url
     */
    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    /**
     * Gets the mqtt broker port.
     *
     * @return the mqtt broker port
     */
    public int getMqttBrokerPort() {
        return mqttBrokerPort;
    }

    /**
     * Sets the mqtt broker port.
     *
     * @param mqttBrokerPort the new mqtt broker port
     */
    public void setMqttBrokerPort(int mqttBrokerPort) {
        this.mqttBrokerPort = mqttBrokerPort;
    }

    /**
     * Gets the mqtt timeout in millis.
     *
     * @return the mqtt timeout in millis
     */
    public int getMqttTimeoutInMillis() {
        return mqttTimeoutInMillis;
    }

    /**
     * Sets the mqtt timeout in millis.
     *
     * @param mqttTimeoutInMillis the new mqtt timeout in millis
     */
    public void setMqttTimeoutInMillis(int mqttTimeoutInMillis) {
        this.mqttTimeoutInMillis = mqttTimeoutInMillis;
    }

    /**
     * Gets the mqtt qos value.
     *
     * @return the mqtt qos value
     */
    public int getMqttQosValue() {
        return mqttQosValue;
    }

    /**
     * Sets the mqtt qos value.
     *
     * @param mqttQosValue the new mqtt qos value
     */
    public void setMqttQosValue(int mqttQosValue) {
        this.mqttQosValue = mqttQosValue;
    }

    /**
     * Gets the mqtt client auth mechanism.
     *
     * @return the mqtt client auth mechanism
     */
    public String getMqttClientAuthMechanism() {
        return mqttClientAuthMechanism;
    }

    /**
     * Sets the mqtt client auth mechanism.
     *
     * @param mqttClientAuthMechanism the new mqtt client auth mechanism
     */
    public void setMqttClientAuthMechanism(String mqttClientAuthMechanism) {
        this.mqttClientAuthMechanism = mqttClientAuthMechanism;
    }

    /**
     * Gets the mqtt service trust store path.
     *
     * @return the mqtt service trust store path
     */
    public String getMqttServiceTrustStorePath() {
        return mqttServiceTrustStorePath;
    }

    /**
     * Sets the mqtt service trust store path.
     *
     * @param mqttServiceTrustStorePath the new mqtt service trust store path
     */
    public void setMqttServiceTrustStorePath(String mqttServiceTrustStorePath) {
        this.mqttServiceTrustStorePath = mqttServiceTrustStorePath;
    }

    /**
     * Gets the mqtt service trust store password.
     *
     * @return the mqtt service trust store password
     */
    public String getMqttServiceTrustStorePassword() {
        return mqttServiceTrustStorePassword;
    }

    /**
     * Sets the mqtt service trust store password.
     *
     * @param mqttServiceTrustStorePassword the new mqtt service trust store password
     */
    public void setMqttServiceTrustStorePassword(String mqttServiceTrustStorePassword) {
        this.mqttServiceTrustStorePassword = mqttServiceTrustStorePassword;
    }

    /**
     * Gets the mqtt service trust store type.
     *
     * @return the mqtt service trust store type
     */
    public String getMqttServiceTrustStoreType() {
        return mqttServiceTrustStoreType;
    }

    /**
     * Sets the mqtt service trust store type.
     *
     * @param mqttServiceTrustStoreType the new mqtt service trust store type
     */
    public void setMqttServiceTrustStoreType(String mqttServiceTrustStoreType) {
        this.mqttServiceTrustStoreType = mqttServiceTrustStoreType;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "MqttConfig{" 
                + "mqttUserName='" + mqttUserName + '\'' 
                + ", cleanSession=" + cleanSession 
                + ", keepAliveInterval=" + keepAliveInterval 
                + ", maxInflight=" + maxInflight 
                + ", brokerUrl='" + brokerUrl + '\'' 
                + ", mqttBrokerPort=" + mqttBrokerPort 
                + ", mqttTimeoutInMillis=" + mqttTimeoutInMillis 
                + ", mqttQosValue=" + mqttQosValue 
                + ", mqttClientAuthMechanism='" + mqttClientAuthMechanism + '\'' 
                + ", mqttServiceTrustStorePath='" + mqttServiceTrustStorePath + '\'' 
                + ", mqttServiceTrustStoreType='" + mqttServiceTrustStoreType + '\'' 
                + '}';
    }
}
