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

package org.eclipse.ecsp.stream.dma.dao.key;

import org.eclipse.ecsp.analytics.stream.base.stores.CacheKeyConverter;


/**
 * AbstractRetryBucketKey implements {@link Comparable} and {@link CacheKeyConverter}.
 *
 * @param <T> the generic type
 */

public abstract class AbstractRetryBucketKey<T> implements Comparable<Object>, CacheKeyConverter<T> {

    /** The timestamp. */
    private long timestamp;

    /**
     * Instantiates a new abstract retry bucket key.
     */
    protected AbstractRetryBucketKey() {
    }

    /**
     * Instantiates a new abstract retry bucket key.
     *
     * @param timestamp the timestamp
     */
    protected AbstractRetryBucketKey(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets the timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the timestamp.
     *
     * @param timestamp the new timestamp
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Convert to string.
     *
     * @return the string
     */
    @Override
    public String convertToString() {
        return String.valueOf(getTimestamp());
    }

}
