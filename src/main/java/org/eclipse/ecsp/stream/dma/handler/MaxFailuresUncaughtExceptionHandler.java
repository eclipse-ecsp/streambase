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

import io.prometheus.client.Counter;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;


/**
 *  class MaxFailuresUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler.
 */
public class MaxFailuresUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

    /** The Constant LOGGER. */
    private static final IgniteLogger LOGGER = IgniteLoggerFactory.getLogger(MaxFailuresUncaughtExceptionHandler.class);
    
    /** The thread recovery total. */
    private static Counter threadRecoveryTotal;
    
    /** The client shutdown total. */
    private static Counter clientShutdownTotal;
    
    /** The uncaught exception total. */
    private static Counter uncaughtExceptionTotal;
    
    /** The max failures. */
    final int maxFailures;
    
    /** The max time interval millis. */
    final long maxTimeIntervalMillis;
    
    /** The previous error time. */
    private Instant previousErrorTime;
    
    /** The current failure count. */
    private int currentFailureCount;

    /**
     * Instantiates a new max failures uncaught exception handler.
     *
     * @param maxFailures the max failures
     * @param maxTimeIntervalMillis the max time interval millis
     */
    public MaxFailuresUncaughtExceptionHandler(final int maxFailures, final long maxTimeIntervalMillis) {
        this.maxFailures = maxFailures;
        this.maxTimeIntervalMillis = maxTimeIntervalMillis;
    }

    static {
        threadRecoveryTotal = Counter.build()
                .name("total_number_of_times_thread_recovered")
                .help("Total number of times thread recovered")
                .register();
        clientShutdownTotal = Counter.build()
                .name("total_number_of_times_client_shuts_down")
                .help("Total number of times client shuts down")
                .register();
        uncaughtExceptionTotal = Counter.build()
                .name("total_number_of_times_uncaught_exception_occurs")
                .help("Total number of times uncaught exception occurs")
                .register();
    }
    
    /**
     * Gets the thread recovery total.
     *
     * @return the thread recovery total
     */
    public static Counter getThreadRecoveryTotal() {
        return threadRecoveryTotal;
    }

    /**
     * Gets the client shutdown total.
     *
     * @return the client shutdown total
     */
    public static Counter getClientShutdownTotal() {
        return clientShutdownTotal;
    }

    /**
     * Handle.
     *
     * @param throwable the throwable
     * @return the stream thread exception response
     */
    @Override
    public StreamThreadExceptionResponse handle(final Throwable throwable) {
        uncaughtExceptionTotal.inc();
        currentFailureCount++;
        Instant currentErrorTime = Instant.now();

        // Log the error for RCA.
        LOGGER.error("Uncaught stream exception stacktrace ", throwable);
        LOGGER.info("currentFailureCount is {}, previousErrorTime is {}, currentErrorTime is {}", currentFailureCount,
                previousErrorTime, currentErrorTime);

        if (previousErrorTime == null) {
            previousErrorTime = currentErrorTime;
        }

        long millisBetweenFailure = ChronoUnit.MILLIS.between(previousErrorTime, currentErrorTime);
        if (currentFailureCount >= maxFailures) {
            if (millisBetweenFailure <= maxTimeIntervalMillis) {
                // Following return value will shutdown the client.
                LOGGER.info("Shutting down the client as millisBetweenFailure is less than maxTimeIntervalMillis");
                clientShutdownTotal.inc();
                return SHUTDOWN_CLIENT;
            } else {
                LOGGER.info("Resetting the value of currentFailureCount and previousErrorTime ");

                currentFailureCount = 0;
                previousErrorTime = null;
            }
        }
        // replaces the thread with the new one.
        threadRecoveryTotal.inc();
        return REPLACE_THREAD;
    }
}
