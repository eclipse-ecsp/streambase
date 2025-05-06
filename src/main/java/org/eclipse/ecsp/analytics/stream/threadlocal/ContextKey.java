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


/**
 * {@link ContextKey} enum.
 */
public enum ContextKey {
    
    /** The kafka sink topic. */
    KAFKA_SINK_TOPIC("kafka_sink_topic");

    /** The context key value. */
    private String contextKeyValue;

    /**
     * Gets the context key.
     *
     * @return the context key
     */
    public String getContextKey() {
        return contextKeyValue;
    }

    /**
     * Instantiates a new context key.
     *
     * @param contextKey the context key
     */
    ContextKey(String contextKey) {
        this.contextKeyValue = contextKey;
    }
}
