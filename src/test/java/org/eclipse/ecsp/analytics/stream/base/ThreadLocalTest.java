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


import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.threadlocal.ContextKey;
import org.eclipse.ecsp.analytics.stream.threadlocal.TaskContextHandler;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * {@link ThreadLocalTest} UT class for {@link ThreadLocal}.
 */
public class ThreadLocalTest {

    /**
     * Test thread local.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testThreadLocal() throws InterruptedException {
        String value1 = "ThreadTopic1";
        String value2 = "ThreadTopic2";
        ThreadClassImpl threadOne = new ThreadClassImpl(value1);
        threadOne.start();

        ThreadClassImpl threadTwo = new ThreadClassImpl(value2);
        threadTwo.start();
        await().atMost(TestConstants.THREAD_SLEEP_TIME_1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(value1, threadOne.getValue());
        Assert.assertEquals(value2, threadTwo.getValue());
    }

    /**
     * inner class {@link ThreadClassImpl} extends {@link Thread}.
     */
    public class ThreadClassImpl extends Thread {
        
        /** The handler. */
        private TaskContextHandler handler = TaskContextHandler.getTaskContextHandler();
        
        /** The value stored. */
        private String valueStored;
        
        /** The value. */
        private String value;

        /**
         * Instantiates a new thread class impl.
         *
         * @param value the value
         */
        public ThreadClassImpl(String value) {
            this.value = value;
        }

        /**
         * Run.
         */
        @Override
        public void run() {
            handler.setValue("Task", ContextKey.KAFKA_SINK_TOPIC, this.value);
            valueStored = handler.getValue("Task", ContextKey.KAFKA_SINK_TOPIC).get() + "";
        }

        /**
         * Gets the value.
         *
         * @return the value
         */
        public String getValue() {
            return valueStored;
        }

    }

}
