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

package org.eclipse.ecsp.stream.dma.handler;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams.State;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaConsumer;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.ThreadUtils;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DeviceConnStatusDAO;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;


/**
 * This is a singleton implementation responsible soley for starting back door kafka
 * comsumer for DMA consuming from device-status-{@code <}service{@code >}
 * topic.
 *
 * @author avadakkootko
 */
@Lazy
@Component
public class DeviceStatusBackDoorKafkaConsumer extends BackdoorKafkaConsumer {

    /** The Constant DEVICE_STATUS_BACKDOOR_HEALTH_MONITOR. */
    public static final String DEVICE_STATUS_BACKDOOR_HEALTH_MONITOR = "DEVICE_STATUS_BACKDOOR_HEALTH_MONITOR";
    
    /** The Constant DEVICE_STATUS_BACKDOOR_HEALTH_GUAGE. */
    public static final String DEVICE_STATUS_BACKDOOR_HEALTH_GUAGE = "DEVICE_STATUS_BACKDOOR_HEALTH_GUAGE";
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceStatusBackDoorKafkaConsumer.class);

    /** The dma consumer poll. */
    @Value("${" + PropertyNames.DMA_KAFKA_CONSUMER_POLL + ":1000}")
    private long dmaConsumerPoll;
    
    /** The dma auto offset reset. */
    @Value("${" + PropertyNames.DMA_AUTO_OFFSET_RESET_CONFIG + ":latest}")
    private String dmaAutoOffsetReset;
    
    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;
    
    /** The health monitor enabled. */
    @Value("${" + PropertyNames.HEALTH_DEVICE_STATUS_BACKDOOR_MONITOR_ENABLED + ":false}")
    private boolean healthMonitorEnabled;
    
    /** The needs restart. */
    @Value("${" + PropertyNames.HEALTH_DEVICE_STATUS_BACKDOOR_MONITOR_RESTART_ON_FAILURE + ":true}")
    private boolean needsRestart;

    /**
     * dma.enabled flag is linked with devicestatuskafkaconsumer as both are components of dma.
     */
    @Value("${" + PropertyNames.DMA_ENABLED + ":true}")
    private boolean isDmaEnabled;

    /** The connection status dao. */
    @Autowired
    private DeviceConnStatusDAO connectionStatusDao;

    /**
     * Added as part of 153542: Acknowledge for policy data publish .
     * This variable is added if a component would like to overwrite the default device status connection topic.
     * In DMF(DataMonetizationFeed) component, we have encountered a situation where
     * dmf-control-sp sends policy to vehicle but policy
     * acknowledgement comes to different topic which was being listened t
     * by PolicyDataStreamProcessor (another stream processor). Now if
     * this PolicyDataStreamProcessor has to send the acknowledgement back to the vehicle,
     * its device status handler should know the status
     * of the vehicle(active or inactive), but vehicle was not publishing the status to its default-topic.
     * Hence in PolicyDataStreamProcessor, we will overwrite its default device-status topic to
     * device-status-dmf-control-sp and it can get
     * the vehicle status
     */

    @Value("${device.status.kafka.topic:}")
    private String deviceConnStatusTopic;

    /** The dma consumer group id. */
    private String dmaConsumerGroupId;

    /** The reset offsets. */
    private boolean resetOffsets = true;

    /** The stream state. */
    private volatile State streamState;

    /**
     * Sets the device status topic name.
     *
     * @param deviceConnStatusTopic the new device status topic name
     */
    public void setDeviceStatusTopicName(String deviceConnStatusTopic) {
        this.deviceConnStatusTopic = deviceConnStatusTopic;
    }

    /**
     * Sets the dma consumer poll.
     *
     * @param dmaConsumerPoll the new dma consumer poll
     */
    public void setDmaConsumerPoll(long dmaConsumerPoll) {
        this.dmaConsumerPoll = dmaConsumerPoll;
    }

    /**
     * Sets the service name.
     *
     * @param serviceName the new service name
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Sets the dma consumer group id.
     *
     * @param dmaConsumerGroupId the new dma consumer group id
     */
    public void setDmaConsumerGroupId(String dmaConsumerGroupId) {
        this.dmaConsumerGroupId = dmaConsumerGroupId;
    }

    /**
     * Sets the dma auto offset reset.
     *
     * @param dmaAutoOffsetReset the new dma auto offset reset
     */
    public void setDmaAutoOffsetReset(String dmaAutoOffsetReset) {
        this.dmaAutoOffsetReset = dmaAutoOffsetReset;
    }

    /**
     * init(): to initialize the DMA kafka consumer group-id.
     */
    @PostConstruct
    public void init() {
        if (StringUtils.isEmpty(serviceName)) {
            throw new IllegalArgumentException(PropertyNames.SERVICE_NAME + " unavailable in property file");
        }
        logger.debug("Service Name initialized is {}", serviceName);

        // Added as part of 153542: Acknowledge for policy data publish .
        // check of customDeviceConnectionTopic is empty or not. if not null,
        // then use this as device-status topic name
        if (StringUtils.isEmpty(deviceConnStatusTopic)) {
            deviceConnStatusTopic = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName;
        }
        logger.info("Device Connection status Topic initialized is {}", deviceConnStatusTopic);

        StringBuilder dmaConsumerGroupIdBuilder = new StringBuilder();
        dmaConsumerGroupIdBuilder.append(DMAConstants.DMA).append(DMAConstants.HYPHEN)
                .append(serviceName).append(DMAConstants.HYPHEN)
                .append(System.currentTimeMillis()).append(DMAConstants.HYPHEN)
                .append(new SecureRandom().nextInt(Constants.THREAD_SLEEP_TIME_1000));
        dmaConsumerGroupId = dmaConsumerGroupIdBuilder.toString();
        logger.info("DMA kafka consumer group-id initialized is {}", dmaConsumerGroupId);
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    @Override
    public String getName() {
        return (DMAConstants.DMA + serviceName);
    }

    /**
     * Gets the kafka consumer group id.
     *
     * @return the kafka consumer group id
     */
    @Override
    public String getKafkaConsumerGroupId() {
        return dmaConsumerGroupId;
    }

    /**
     * Gets the kafka consumer topic.
     *
     * @return the kafka consumer topic
     */
    @Override
    public String getKafkaConsumerTopic() {
        return deviceConnStatusTopic;
    }

    /**
     * Gets the poll.
     *
     * @return the poll
     */
    @Override
    public long getPoll() {
        return dmaConsumerPoll;
    }

    /**
     * Checks if is offsets reset complete.
     *
     * @return true, if is offsets reset complete
     */
    @Override
    public boolean isOffsetsResetComplete() {
        return this.resetOffsets;
    }

    /**
     * Sets the reset offsets.
     *
     * @param reset the new reset offsets
     */
    @Override
    public void setResetOffsets(boolean reset) {
        this.resetOffsets = reset;

    }

    /**
     * Gets the stream state.
     *
     * @return the stream state
     */
    @Override
    public State getStreamState() {
        return streamState;
    }

    /**
     * Sets the stream state.
     *
     * @param state the new stream state
     */
    @Override
    public void setStreamState(State state) {
        streamState = state;
    }

    /**
     * Monitor name.
     *
     * @return the string
     */
    @Override
    public String monitorName() {
        return DEVICE_STATUS_BACKDOOR_HEALTH_MONITOR;
    }

    /**
     * Needs restart on failure.
     *
     * @return true, if successful
     */
    @Override
    public boolean needsRestartOnFailure() {
        return needsRestart;
    }

    /**
     * Metric name.
     *
     * @return the string
     */
    @Override
    public String metricName() {
        return DEVICE_STATUS_BACKDOOR_HEALTH_GUAGE;
    }

    /**
     * Checks if is enabled.
     *
     * @return true, if is enabled
     */
    @Override
    public boolean isEnabled() {
        // If device status is disabled then the monitor should also be
        // disabled.
        if (!isDmaEnabled) {
            return false;
        } else {
            return healthMonitorEnabled;
        }
    }

    /**
     * startDMABackDoorConsumer().
     */
    public void startDMABackDoorConsumer() {
        connectionStatusDao.close();
        logger.info("Cleared Device status cache as request to start Back Door Kafka Consumer was triggered");
        connectionStatusDao.initialize();
        logger.info("Synced Device status cache from redis as request to start Back Door Kafka Consumer was triggered");
        super.startBackDoorKafkaConsumer();
    }

    /**
     * Shut down kafka consumer. This method is invoked when Stream closes.
     *
     * @param spc the spc
     */
    public void shutdown(StreamProcessingContext<?, ?> spc) {
        // RTC-192213 - Added to clear the cache held by
        // IntegrationFeedCacheUpdateCallBack. More specifically added to
        // ensure that the third party kafka broker producers are flushed
        // before the application shuts down. This will ensure that the data
        // are flushed immediately in case of kafka batching.
        callBackMap.forEach((k, v) -> v.close());

        //RTC-394242 - Added to remove only the callbacks for the partitions which went into rebalancing
        // Moved out this piece of code from the shutdown consumer process cause both process are independent
        // Callback has to be removed during each rebalance regardless pf the consumer shutodown process
        int partition = Integer.parseInt(spc.getTaskID().split("_")[1]);
        callBackMap.remove(partition);
        logger.info("Cleared Callback map for partition: {} and taskID: {}", partition, spc.getTaskID());

        if (getStartedConsumer().get() && !getClosed().get()) {
            closed.set(true);
            startedConsumer.set(false);
            if (consumer != null) {
                consumer.wakeup();
            }
            ThreadUtils.shutdownExecutor(kafkaConsumerRunExecutor, Constants.THREAD_SLEEP_TIME_10000, false);
            ThreadUtils.shutdownExecutor(offsetsMgmtExecutor, Constants.THREAD_SLEEP_TIME_10000, false);
            removeConsumerGroup(getKafkaConsumerGroupId());
            logger.info("Closed Backdoor Kafka Consumer");
        }
    }

    /**
     * Sets the needs restart on failure.
     *
     * @param needsRestart the new needs restart on failure
     */
    // Below methods are for supporting test cases
    void setNeedsRestartOnFailure(boolean needsRestart) {
        this.needsRestart = needsRestart;
    }

    /**
     * Sets the health monitor enabled.
     *
     * @param healthMonitorEnabled the new health monitor enabled
     */
    void setHealthMonitorEnabled(boolean healthMonitorEnabled) {
        this.healthMonitorEnabled = healthMonitorEnabled;
    }

    /**
     * Sets the checks if is dma enabled.
     *
     * @param isDmaEnabled the new checks if is dma enabled
     */
    // below method is for testing purpose
    void setIsDmaEnabled(boolean isDmaEnabled) {
        this.isDmaEnabled = isDmaEnabled;
    }

}
