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

import org.eclipse.ecsp.analytics.stream.base.dao.CacheBackedInMemoryBatchCompleteCallBack;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.entities.IgniteEntity;

import java.util.Optional;

/**
 *  interface MutableKeyValueStore extends IgniteEntity.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface MutableKeyValueStore<K extends CacheKeyConverter<K>, V extends IgniteEntity> {

    /**
     * Stores key value pair. The key value may be persisted to cache backend asynchronously.
     * The given mutation id will be provided to the
     * callback when this operation has been committed to the cache backend.
     *
     * @param key key
     * @param value value
     * @param mutationId mutationId
     * @param cacheType the cache type
     */
    public void put(K key, V value, Optional<MutationId> mutationId, String cacheType);

    /**
     * Put if absent.
     *
     * @param key the key
     * @param value the value
     * @param mutationId the mutation id
     * @param cacheType the cache type
     * @return the v
     */
    public V putIfAbsent(K key, V value, Optional<MutationId> mutationId, String cacheType);

    /**
     * Deletes key value pair. The key may be deleted from backend asynchronously.
     * The given mutation id will be provided to the callback
     * when this operation has been committed to the cache backend.
     *
     * @param key key
     * @param mutationId mutationId
     * @param cacheType the cache type
     */
    public void delete(K key, Optional<MutationId> mutationId, String cacheType);

    /**
     * Sets the call back.
     *
     * @param callBack the new call back
     */
    public void setCallBack(CacheBackedInMemoryBatchCompleteCallBack callBack);

    /**
     * Sync withcache.
     *
     * @param regex the regex
     * @param converter the converter
     */
    public void syncWithcache(String regex, K converter);

    /**
     * put to in-memory hashmap is backed by RMap in cache.
     *
     * @param mapKey mapKey
     * @param mapEntryKey mapEntryKey
     * @param mapEntryValue mapEntryValue
     * @param mutationId mutationId
     * @param cacheType the cache type
     */
    public void putToMap(String mapKey, K mapEntryKey, V mapEntryValue,
            Optional<MutationId> mutationId, String cacheType);

    /**
     * Put to map if absent.
     *
     * @param mapKey the map key
     * @param mapEntryKey the map entry key
     * @param mapEntryValue the map entry value
     * @param mutationId the mutation id
     * @param cacheType the cache type
     * @return the v
     */
    public V putToMapIfAbsent(String mapKey, K mapEntryKey, V mapEntryValue,
            Optional<MutationId> mutationId, String cacheType);

    /**
     * delete from in-memory hashmap is backed by RMap in cache.
     *
     * @param mapKey mapKey
     * @param mapEntryKey the map entry key
     * @param mutationId mutationId
     * @param cacheType the cache type
     */
    public void deleteFromMap(String mapKey, K mapEntryKey, Optional<MutationId> mutationId, String cacheType);

    /**
     * Sync will read all key value pairs from cache for the parent key and populate the in-memory map.
     *
     * @param mapKey mapKey
     * @param converter converter
     * @param cacheType the cache type
     */
    public void syncWithMapCache(String mapKey, K converter, String cacheType);

    /**
     * As per the implementatation of cache read should happen only from in memory key store.
     * Force get bypasses the in-memory key store and
     * reads from redis.
     *
     * @param mapKey mapKey
     * @param mapEntryKey mapEntryKey
     * @return the v
     */
    public V forceGet(String mapKey, K mapEntryKey);

}
