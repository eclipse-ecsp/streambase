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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.NoSuchElementException;

/**
 * The Class SerializedKVIterator.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
class SerializedKVIterator<K, V> implements KeyValueIterator<K, V> {

    /** The bytes iterator. */
    private final KeyValueIterator<Bytes, byte[]> bytesIterator;
    
    /** The serdes. */
    private final StateSerdes<K, V> serdes;

    /**
     * Instantiates a new serialized KV iterator.
     *
     * @param bytesIterator the bytes iterator
     * @param serdes the serdes
     */
    SerializedKVIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator,
            final StateSerdes<K, V> serdes) {

        this.bytesIterator = bytesIterator;
        this.serdes = serdes;
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        bytesIterator.close();
    }

    /**
     * Peek next key.
     *
     * @return the k
     */
    @Override
    public K peekNextKey() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Bytes bytes = bytesIterator.peekNextKey();
        return serdes.keyFrom(bytes.get());
    }

    /**
     * Checks for next.
     *
     * @return true, if successful
     */
    @Override
    public boolean hasNext() {
        return bytesIterator.hasNext();
    }

    /**
     * Next.
     *
     * @return the key value
     */
    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final KeyValue<Bytes, byte[]> next = bytesIterator.next();
        return KeyValue.pair(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value));
    }

    /**
     * Removes the.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove not supported by SerializedKeyValueIterator");
    }
}
