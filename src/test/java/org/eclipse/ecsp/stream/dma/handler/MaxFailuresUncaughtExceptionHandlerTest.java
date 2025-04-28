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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Before;
import org.junit.Test;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.junit.Assert.assertEquals;



/**
 * class MaxFailuresUncaughtExceptionHandlerTest.
 */
public class MaxFailuresUncaughtExceptionHandlerTest {
    
    /** The works on my box exception. */
    private final IllegalStateException worksOnMyBoxException =
            new IllegalStateException("Strange, It worked on my box");
    
    /** The exception handler. */
    private MaxFailuresUncaughtExceptionHandler exceptionHandler;

    /**
     * setUp().
     */
    @Before
    public void setUp() {
        long maxTimeMillis = Constants.THREAD_SLEEP_TIME_100;
        int maxFailures = Constants.TWO;
        exceptionHandler = new MaxFailuresUncaughtExceptionHandler(maxFailures, maxTimeMillis);
    }

    /**
     * Should replace thread when errors not within max time.
     *
     * @throws Exception the exception
     */
    @Test
    public void shouldReplaceThreadWhenErrorsNotWithinMaxTime() throws Exception {
        for (int i = 0; i < Constants.TEN; i++) {
            assertEquals(REPLACE_THREAD, exceptionHandler.handle(worksOnMyBoxException));
            Thread.sleep(Constants.THREAD_SLEEP_TIME_200);
        }
    }

}
