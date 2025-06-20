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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.BackdoorKafkaConsumer;
import org.eclipse.ecsp.analytics.stream.base.offset.OffsetManager;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 *Listens to state changes in the {@link KafkaStreams} instance.
 *This class is responsible for:
 * <ul>
 * <li>Restarting {@link BackdoorKafkaConsumer} instances when the state changes
 * to RUNNING.</li>
 * <li>Notifying {@link OffsetManager} to repopulate offsets from MongoDB.</li>
 * </ul>
 */
@Component
public class KafkaStateListener implements KafkaStreams.StateListener {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaStateListener.class);

    /** The {@link OffsetManager} instance. */
    @Autowired
    private OffsetManager offsetManager;

    /** The Spring's ApplicationContext. */
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Instantiates a new kafka state listener.
     */
    public KafkaStateListener() {
        // default constructor
    }

    /**
     * Handles state changes in the {@link KafkaStreams} instance.
     *
     * <p>This method performs the following actions:
     * <ul>
     * <li>Monitors the NON RUNNING states and logs respective state for respective stream thread.</li>
     * <li>Notifies {@link OffsetManager} to repopulate offsets when
     * transitioning from REBALANCING to RUNNING.</li>
     * <li>Invokes any registered {@link KafkaStateAgentListener} instances when
     * transitioning from REBALANCING to RUNNING.</li>
     * </ul>
     *
     * @param newState
     *            the new state of the {@link KafkaStreams}.
     * @param oldState
     *            the previous state of the {@link KafkaStreams}.
     */
    @Override
    public void onChange(State newState, State oldState) {
        logger.info("Stream state changed from {} to {}", oldState, newState);
        logState(newState);
        if (State.REBALANCING == oldState && State.RUNNING == newState) {
            Map<String, KafkaStateAgentListener> kafkaAgentListeners = applicationContext
                    .getBeansOfType(KafkaStateAgentListener.class);

            kafkaAgentListeners.values().forEach(listner -> listner.onChange(newState, oldState));
            offsetManager.setUp();
        }
    }
    
    /**
     * Logs the state of the stream thread.
     *
     * @param newState The new state of the stream thread.
     * @param oldState The old state of the stream thread.
     * @param streamsThreadName The name of the stream thread.
     */
    private void logState(State newState) {
        String streamsThreadName = Thread.currentThread().getName();
        if (newState == State.REBALANCING || newState == State.PENDING_SHUTDOWN
                || newState == State.NOT_RUNNING || newState == State.ERROR) {
            logger.error("Stream thread {} is in state: {}", streamsThreadName, newState.toString());
        } else if (newState == State.RUNNING) {
            logger.info("Stream thread {} is now RUNNING", streamsThreadName);
        }
    }
}
