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

package org.eclipse.ecsp.analytics.stream.base.stores;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

/**
 * interface {@link SortedKeyValueStore} extends {@link KeyValueStore}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface SortedKeyValueStore<K, V> extends KeyValueStore<K, V> {
    
    /** The logger. */
    public static IgniteLogger LOGGER = IgniteLoggerFactory.getLogger(SortedKeyValueStore.class);

    /**
     * Returns a view of the portion of statestore whose keys are less than (or equal to) toKey.
     *
     * @param toKey toKey
     * @return KeyValueIterator
     */
    public default KeyValueIterator<K, V> getHead(K toKey) {
        LOGGER.error("Implementation of Method getHeadMap unavailable");
        return null;
    }

    /**
     * Returns a view of the portion of statestore whose keys are greater than (or equal to) fromKey.
     *
     * @param fromKey fromKey
     * @return KeyValueIterator
     */
    public default KeyValueIterator<K, V> getTail(K fromKey) {
        LOGGER.error("Implementation of Method getTailMap unavailable");
        return null;
    }

}
