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

import org.eclipse.ecsp.analytics.stream.base.exception.MaxRetriesFailedException;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.function.Function;


/**
 * RetryUtils: utility classs for {@link reactor.util.retry.Retry}.
 */
public class RetryUtils {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(RetryUtils.class);

    /**
     * Instantiates a new retry utils.
     */
    private RetryUtils() {

    }
    
    /**
     * Retries a function call and returns result or throws
     * exception if function didn't return a non-null response for all attempts. Retry
     * interval is 250ms.
     *
     * @param <R> the generic type
     * @param n         - number of retries to attempt
     * @param f         - function that should return a result if it is successful
     * @return result from function
     */
    
    public static <R> R retryWithException(int n, Function<Void, R> f) {
        for (int i = 0; i < n; i++) {
            logger.info("attempt {}", i);
            R r = f.apply(null);
            if (r != null) {
                logger.info("Received non-null from function. Returning");
                return r;
            }
            logger.info("Received null from function. Will sleep and retry");
            try {
                Thread.sleep(Constants.THREAD_SLEEP_TIME_250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new MaxRetriesFailedException("Max retries failed");
    }

    /**
     * Retries a function call and returns result or returns null if
     * function didn't return a non-null response for all attempts. Retry
     * interval is 250ms.
     *
     * @param <R> the generic type
     * @param n         - number of retries to attempt
     * @param f         - function that should return a result if it is successful
     * @return result from function or null
     */
    public static <R> R retry(int n, Function<Void, R> f) {
        for (int i = 0; i < n; i++) {
            logger.info("attempt {}", i);
            R r = f.apply(null);
            if (r != null) {
                logger.info("Received non-null from function. Returning");
                return r;
            }
            logger.info("Received null from function. Will sleep and retry");
            try {
                Thread.sleep(Constants.THREAD_SLEEP_TIME_250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return null;
    }
}
