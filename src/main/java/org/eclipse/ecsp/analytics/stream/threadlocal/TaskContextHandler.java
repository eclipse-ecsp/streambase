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

package org.eclipse.ecsp.analytics.stream.threadlocal;

import jakarta.annotation.PreDestroy;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 *Singleton class to handle context data that needs to be forwarded from one service to another.
 * It contains ThreadLocal which will will be keyed by taskId and ContextType and value will be the object.
 * For example : For a taskId T1 and key KAFKA_SINK_TOPIC value can be eventsTopic.
 *
 * @author avadakkootko
 */
public class TaskContextHandler {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(TaskContextHandler.class);

    /** The task context handler. */
    private static TaskContextHandler taskContextHandler = new TaskContextHandler();
    
    /** The thread local. */
    private ThreadLocal<Map<String, Map<ContextKey, Object>>> threadLocal;

    /**
     * Instantiates a new task context handler.
     */
    private TaskContextHandler() {
        threadLocal = ThreadLocal.withInitial(HashMap::new);
        logger.info("Initialised threadLocal inside TaskContextHandler");
    }

    /**
     * Gets the task context handler.
     *
     * @return the task context handler
     */
    public static TaskContextHandler getTaskContextHandler() {
        return taskContextHandler;
    }

    /**
     * Destroy.
     */
    @PreDestroy()
    private void destroy() {
        threadLocal.remove();
    }
    /**
     * Set value per taskId per context type.
     *
     * @param taskId taskId
     * @param key key
     * @param value value
     */
    
    public void setValue(String taskId, ContextKey key, Object value) {
        Map<String, Map<ContextKey, Object>> map = threadLocal.get();
        if (map.containsKey(taskId)) {
            map.get(taskId).put(key, value);
        } else {
            Map<ContextKey, Object> sc = new EnumMap<>(ContextKey.class);
            sc.put(key, value);
            map.put(taskId, sc);
        }
        logger.debug("Inserted value {} for taskId {}  and key {} from ThreadLocal Storage", value, taskId, key.name());
    }

    /**
     * reset value per taskId.
     *
     * @param taskId taskId
     */
    public void resetTaskContextValue(String taskId) {
        Map<String, Map<ContextKey, Object>> map = threadLocal.get();
        if (map.containsKey(taskId)) {
            map.remove(taskId);
        }
        logger.debug("Reset ThreadLocal Storage for taskId", taskId);
    }

    /**
     * Get value per taskId per context type.
     *
     * @param taskId taskId
     * @param key key
     * @return Optional
     */
    public Optional<Object> getValue(String taskId, ContextKey key) {
        Map<String, Map<ContextKey, Object>> map = threadLocal.get();
        if (map.containsKey(taskId)) {
            Object value = map.get(taskId).get(key);
            logger.debug("Returning value {} for taskId {} and key {} from ThreadLocal Storage", 
                    value, taskId, key.name());
            return Optional.ofNullable(value);
        } else {
            logger.trace("Context unavailable in threadlocal storage for taskId: {}", taskId);
            return Optional.empty();
        }
    }

}
