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

import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.entities.AbstractIgniteEntity;
import org.eclipse.ecsp.entities.IgniteEntity;

import java.util.Optional;
import java.util.UUID;

/**
 * Captures the idea of the events/RetryRecords whether to write to/delete from redis along with other information.
 *
 * @author hbadshah
 * @param <K> the key type
 * @param <V> the value type
 */
@Entity
public class CacheEntity<K extends CacheKeyConverter<K>, V extends IgniteEntity> extends AbstractIgniteEntity {

    /** The id. */
    // to uniquely identify a CacheEntity if required
    @Id
    private String id;

    /** The key. */
    private K key;
    
    /** The value. */
    private V value;
    
    /** The map key. */
    private String mapKey;
    
    /** The op. */
    private Operation op;
    
    /** The mutation id. */
    private MutationId mutationId;

    /**
     * Instantiates a new cache entity.
     */
    public CacheEntity() {
        StringBuilder stringBuilder = new StringBuilder();
        id = stringBuilder.append(UUID.randomUUID().toString()).append(System.currentTimeMillis()).toString();
    }

    // this key corresponds to the key whose type is K in CachedMapStateStore and CacheSortedMapStateStore
    // and corresponds to the mapEntryKey in PutMapOfEntitiesRequest as well as in DeleteMapOfEntitiesRequest.
    /**
     * With key.
     *
     * @param key the key
     * @return the cache entity
     */
    // This is the childKey.
    public CacheEntity<K, V> withKey(K key) {
        this.key = key;
        return this;
    }

    /**
     * With value.
     *
     * @param value the value
     * @return the cache entity
     */
    public CacheEntity<K, V> withValue(V value) {
        this.value = value;
        return this;
    }

    // this mapKey corresponds to the mapKey of PutMapOfEntitiesRequest and
    /**
     * With map key.
     *
     * @param mapKey the map key
     * @return the cache entity
     */
    // DeleteMapOfEntities. This is the parentKey
    public CacheEntity<K, V> withMapKey(String mapKey) {
        this.mapKey = mapKey;
        return this;
    }

    /**
     * method to set mutationId.
     *
     * @param mutationId Optional{@code <}MutationId{@code >}
     * @return org.eclipse.ecsp.analytics.stream.base.stores.CacheEntity
     */
    public CacheEntity<K, V> withMutationId(Optional<MutationId> mutationId) {
        if (mutationId.isPresent()) {
            this.mutationId = mutationId.get();
        } else {
            this.mutationId = null;
        }
        return this;
    }

    /**
     * With operation.
     *
     * @param op the op
     * @return the cache entity
     */
    public CacheEntity<K, V> withOperation(Operation op) {
        this.op = op;
        return this;
    }

    /**
     * Gets the key.
     *
     * @return the key
     */
    public K getKey() {
        return this.key;
    }

    /**
     * Gets the value.
     *
     * @return the value
     */
    public V getValue() {
        return this.value;
    }

    /**
     * Gets the map key.
     *
     * @return the map key
     */
    public String getMapKey() {
        return this.mapKey;
    }

    /**
     * Gets the mutation id.
     *
     * @return the mutation id
     */
    public Optional<MutationId> getMutationId() {
        return Optional.ofNullable(this.mutationId);
    }

    /**
     * Gets the operation.
     *
     * @return the operation
     */
    public Operation getOperation() {
        return this.op;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "CacheEntity[ Key: " + key + "," + "Value:" + value + ","
                + "Map Key:" + mapKey + "," + "Mutation Id:" + mutationId + "," + "Operation:" + op + "]";
    }

}
