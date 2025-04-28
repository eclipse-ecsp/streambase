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

import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;


/**
 * test class for KafkaStateListnerHealthMonitor.
 */
public class KafkaStateListnerHealthMonitorTest {

    /**
     * It should be healthy only if Current State is RUNNING .
     */
    @Test
    public void testKafkaStateListenerHealthMonitor() {
        KafkaStateListener kafkaStateListener = new KafkaStateListener();
        kafkaStateListener.setBackdoorConsumers(Collections.EMPTY_LIST);
        kafkaStateListener.onChange(State.RUNNING, State.CREATED);
        Assert.assertTrue(kafkaStateListener.isHealthy(true));
        kafkaStateListener.onChange(State.PENDING_SHUTDOWN, State.CREATED);
        Assert.assertFalse(kafkaStateListener.isHealthy(true));
    }

}
