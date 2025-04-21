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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * ThreadUtils: Utility class.
 */
public class ThreadUtils {

    /**
     * Instantiates a new thread utils.
     */
    private ThreadUtils() {

    }

    /** The Constant LOGGER. */
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ThreadUtils.class);

    /**
     * Shuts down an executor reliably. Optionally allows shutting down the JVM
     * if executor doesn't shutdown.
     *
     * @param exec exec
     * @param waitTimeMs waitTimeMs
     * @param exitOnFailure exitOnFailure
     */
    public static void shutdownExecutor(ExecutorService exec, int waitTimeMs, boolean exitOnFailure) {
        if (!exec.isShutdown()) {
            LOGGER.info("Shutting down executor service");
            exec.shutdown(); // Disable new tasks from being submitted
            try {
                // Wait a while for existing tasks to terminate
                if (!exec.awaitTermination(waitTimeMs, TimeUnit.MILLISECONDS)) {
                    LOGGER.info("Shutting down executor service forcefully as it has not "
                            + "responded to graceful shutdown");
                    exec.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!exec.awaitTermination(waitTimeMs, TimeUnit.MILLISECONDS)) {
                        LOGGER.error("Executor service not closed after waiting {} ms", waitTimeMs);
                        if (exitOnFailure) {
                            LOGGER.error("Executor service not closed after waiting {} ms . "
                                    + "Exiting application", waitTimeMs);
                            System.exit(1);
                        }
                    }

                }
            } catch (InterruptedException ie) {
                handleInterruptedException(exec, waitTimeMs, exitOnFailure);
            }
        }
    }

    /**
     * Handle interrupted exception.
     *
     * @param exec the exec
     * @param waitTimeMs the wait time ms
     * @param exitOnFailure the exit on failure
     */
    private static void handleInterruptedException(ExecutorService exec, int waitTimeMs, boolean exitOnFailure) {
        // (Re-)Cancel if current thread also interrupted
        exec.shutdownNow();
        try {
            if (!exec.awaitTermination(waitTimeMs, TimeUnit.MILLISECONDS)) {
                LOGGER.error("Executor service not closed after waiting {} ms", waitTimeMs);
                if (exitOnFailure) {
                    LOGGER.error("Executor service not closed after waiting {} ms . Exiting application", waitTimeMs);
                    System.exit(1);
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted when waiting on executor");
            Thread.currentThread().interrupt();
            if (exitOnFailure) {
                LOGGER.error("Executor service shutdown failed. Interrupted. Exiting application");
                System.exit(1);
            }
        }
    }
}
