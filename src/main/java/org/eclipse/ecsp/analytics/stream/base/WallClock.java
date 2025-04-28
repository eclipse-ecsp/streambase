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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps track of ticks. Each tick represents 1 second. To subscribe to ticks,
 * {@link TickListener} implementation must be defined to which ticks count will be given.
 */
public class WallClock {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(WallClock.class);
    
    /** The WallClock Instance. */
    public static final WallClock INSTANCE = new WallClock();
    
    /** The list of all the subscribed listeners. */
    private List<TickListener> listeners = new ArrayList<>();
    
    /** {@link ScheduledExecutorService} instance. */
    private ScheduledExecutorService exec = null;

    /**
     * Starts a scheduled thread which notifies the listeners about the ticks.
     */
    private WallClock() {
        exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        });
        exec.scheduleWithFixedDelay(new Runnable() {
            private long ticks = 0;

            @Override
            public void run() {
                notifyListeners(++ticks);
            }
        }, Constants.INT_45, 1, TimeUnit.SECONDS);
    }

    /**
     * Gets the single instance of WallClock.
     *
     * @return single instance of WallClock
     */
    public static final WallClock getInstance() {
        return INSTANCE;
    }

    /**
     * Subscribe to WallClock.
     *
     * @param wcl the implementation of {@link TickListener}.
     */
    public synchronized void subscribe(TickListener wcl) {
        listeners.add(wcl);
        logger.info("Added listener: {} to wall clock listeners list", wcl);
    }

    /**
     * Unsubscribe from WallClock.
     *
     * @param wcl the instance of the implementation of {@link TickListener}
     */
    public synchronized void unsubscribe(TickListener wcl) {
        listeners.remove(wcl);
        logger.info("Removed listener: {} from wall clock listeners list", wcl);
    }

    /**
     * Notify listeners of the ticks passed.
     *
     * @param ticks the ticks
     */
    private void notifyListeners(long ticks) {
        List<TickListener> immutableList = null;
        synchronized (this) {
            immutableList = new ArrayList<>(listeners);
        }
        immutableList.forEach(wcl -> {
            try {
                wcl.tick(ticks);
            } catch (Exception e) {
                logger.error("Listener failed in tick()", e);
            }
        });
    }

}
