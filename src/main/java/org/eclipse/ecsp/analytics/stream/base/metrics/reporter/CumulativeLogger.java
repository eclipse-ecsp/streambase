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

package org.eclipse.ecsp.analytics.stream.base.metrics.reporter;

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregate and print given counters at configured time interval.
 */
public final class CumulativeLogger {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(CumulativeLogger.class);
    
    /** The Constant SPACE. */
    private static final String SPACE = " ";
    
    /** The Constant STATE. */
    private static final Map<String, AtomicLong> STATE = new ConcurrentHashMap<>();
    
    /** The log every X minute. */
    private static int logEveryXMinute = 5;

    /**
     * The Class CumulativeLoggerHolder.
     */
    private static class CumulativeLoggerHolder {
        
        /** The Constant C_LOGGER. */
        private static final CumulativeLogger C_LOGGER = new CumulativeLogger();

        /**
         * Instantiates a new cumulative logger holder.
         */
        private CumulativeLoggerHolder() {
        }
    }

    /**
     * Instantiates a new cumulative logger.
     *
     * @param logEveryXminute the log every xminute
     */
    private CumulativeLogger() {
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread t = Executors.defaultThreadFactory().newThread(runnable);
                    t.setDaemon(true);
                    t.setName("CumulativeLogger:" + Thread.currentThread().getName());
                    return t;
                });
        ses.scheduleAtFixedRate(CumulativeLogger::resetAndLog, logEveryXMinute, logEveryXMinute, TimeUnit.MINUTES);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            CumulativeLogger.resetAndLog();
            logger.info("Flushed Cumulative Logger state");
        }));
        logger.info("Cumulative logger initialized.");
    }

    /**
     * init() to setup logEveryXMinute property.
     *
     * @param properties Properties
     */
    public static void init(Properties properties) {
        logEveryXMinute = Integer.parseInt(properties.getProperty(PropertyNames.LOG_COUNTS_MINUTES, "5"));
        if (logEveryXMinute < 1) {
            throw new IllegalArgumentException("Log count must be greater than 0");
        }
    }

    /**
     * Gets the logger.
     *
     * @return the logger
     */
    public static final CumulativeLogger getLogger() {
        return CumulativeLoggerHolder.C_LOGGER;
    }

    /**
     * Reset and log.
     */
    private static void resetAndLog() {
        StringBuilder str = new StringBuilder();
        STATE.forEach((k, v) -> {
            str.delete(0, str.length());
            long count = v.getAndSet(0);
            if (count > 0) {
                str.append(k).append(SPACE).append(count);
                logger.info(str.toString());
            }
        });
    }

    /**
     * Increment by one.
     *
     * @param counter the counter
     */
    public void incrementByOne(String counter) {
        incrementBy(counter, 1);
    }

    /**
     * Increment by.
     *
     * @param counter the counter
     * @param count the count
     */
    public void incrementBy(String counter, int count) {
        STATE.putIfAbsent(counter, new AtomicLong(0));
        STATE.get(counter).addAndGet(count);
    }
}
