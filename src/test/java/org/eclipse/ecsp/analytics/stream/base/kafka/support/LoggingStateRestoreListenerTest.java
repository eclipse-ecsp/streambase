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

package org.eclipse.ecsp.analytics.stream.base.kafka.support;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.ecsp.analytics.stream.base.kafka.support.LoggingStateRestoreListener;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;



/**
 * class {@link LoggingStateRestoreListenerTest}.
 */
public class LoggingStateRestoreListenerTest {

    /**
     * Test no exceptions.
     */
    @Test
    public void testNoExceptions() {
        LoggingStateRestoreListener lsrl = new LoggingStateRestoreListener();
        lsrl.onRestoreStart(new TopicPartition("abcd", 1), "store1", 
                Constants.THREAD_SLEEP_TIME_100, Constants.THREAD_SLEEP_TIME_1000);
        lsrl.onBatchRestored(new TopicPartition("abcd", 1), "store1", 
                Constants.THREAD_SLEEP_TIME_200, Constants.THREAD_SLEEP_TIME_100);

        Assertions.assertDoesNotThrow(() -> 
            lsrl.onRestoreEnd(new TopicPartition("abcd", 1), "store1", Constants.THREAD_SLEEP_TIME_100));
    }
}
