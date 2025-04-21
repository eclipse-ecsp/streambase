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

import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.MutationId;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;



/**
 * UT class {@link CacheEntityTest} for {@link CacheEntity}.
 */
public class CacheEntityTest {
    
    /** The key 1. */
    private String key1 = "xyz";
    
    /** The key 2. */
    private String key2 = "pqr";
    
    /** The string key 1. */
    private StringKey stringKey1;
    
    /** The string key 2. */
    private StringKey stringKey2;
    
    /** The event 1. */
    private IgniteEventImpl event1;
    
    /** The event 2. */
    private IgniteEventImpl event2;
    
    /** The map key 1. */
    private String mapKey1 = "abc_1";
    
    /** The map key 2. */
    private String mapKey2 = "abc_2";
    
    /** The mutation id. */
    private Optional<MutationId> mutationId = Optional.empty();
    
    /** The op 1. */
    private Operation op1 = Operation.PUT;
    
    /** The op 2. */
    private Operation op2 = Operation.DEL_FROM_MAP;
    
    /** The entity 1. */
    private CacheEntity<StringKey, IgniteEventImpl> entity1;
    
    /** The entity 2. */
    private CacheEntity<StringKey, IgniteEventImpl> entity2;

    /**
     * setup method to create {@link CacheEntity}.
     */
    @Before
    public void setup() {
        stringKey1 = new StringKey(key1);
        stringKey2 = new StringKey(key2);
        event1 = new IgniteEventImpl();
        event1.setEventId("test");
        event2 = new IgniteEventImpl();
        event2.setEventId("test2");
        entity1 = new CacheEntity<>();
        entity2 = new CacheEntity<>();
    }

    /**
     * Test with key.
     */
    @Test
    public void testWithKey() {
        entity1.withKey(stringKey1);
        Assert.assertEquals(key1, entity1.getKey().convertToString());
    }

    /**
     * Test with value.
     */
    @Test
    public void testWithValue() {
        entity1.withValue(event1);
        Assert.assertEquals(event1.getEventId(), entity1.getValue().getEventId());
    }

    /**
     * Test with map key.
     */
    @Test
    public void testWithMapKey() {
        entity1.withMapKey(mapKey1);
        Assert.assertEquals(mapKey1, entity1.getMapKey());
    }

    /**
     * Test with mutation id.
     */
    @Test
    public void testWithMutationId() {
        OffsetMetadata metadata = new OffsetMetadata(null, TestConstants.THREAD_SLEEP_TIME_5000);
        entity1.withMutationId(Optional.of(metadata));
        Assert.assertTrue(entity1.getMutationId().isPresent());
    }

    /**
     * Test with operation.
     */
    @Test
    public void testWithOperation() {
        entity1.withOperation(Operation.PUT);
        Assert.assertSame(Operation.PUT, entity1.getOperation());

        entity1.withOperation(Operation.PUT_TO_MAP);
        Assert.assertSame(Operation.PUT_TO_MAP, entity1.getOperation());

        entity1.withOperation(Operation.DEL);
        Assert.assertSame(Operation.DEL, entity1.getOperation());

        entity1.withOperation(Operation.DEL_FROM_MAP);
        Assert.assertSame(Operation.DEL_FROM_MAP, entity1.getOperation());
    }

    /**
     * Test all fields.
     */
    @Test
    public void testAllFields() {
        entity2.withKey(stringKey2).withValue(event2).withMutationId(mutationId).withOperation(Operation.DEL);
        Assert.assertTrue(entity2.getKey() == stringKey2 && entity2.getValue().getEventId() == event2.getEventId()
                && entity2.getMutationId().isEmpty() && entity2.getOperation() == Operation.DEL);
    }

    /**
     * Test get key.
     */
    @Test
    public void testGetKey() {
        entity1.withKey(stringKey1);
        entity2.withKey(stringKey2);
        Assert.assertEquals("xyz", entity1.getKey().convertToString());
        Assert.assertEquals("pqr", entity2.getKey().convertToString());
    }

    /**
     * Test get value.
     */
    @Test
    public void testGetValue() {
        entity1.withValue(event1);
        entity2.withValue(event2);
        Assert.assertEquals(event1.getEventId(), entity1.getValue().getEventId());
        Assert.assertEquals(event2.getEventId(), entity2.getValue().getEventId());
    }

    /**
     * Test get map key.
     */
    @Test
    public void testGetMapKey() {
        entity1.withMapKey(mapKey1);
        entity2.withMapKey(mapKey2);
        Assert.assertEquals("abc_1", entity1.getMapKey());
        Assert.assertEquals("abc_2", entity2.getMapKey());
    }

    /**
     * Test get mutation id.
     */
    @Test
    public void testGetMutationId() {
        entity1.withMutationId(mutationId);
        Assert.assertEquals(Optional.empty(), mutationId);
    }

    /**
     * Test get operation.
     */
    @Test
    public void testGetOperation() {
        entity1.withOperation(op1);
        entity2.withOperation(op2);
        Assert.assertEquals(Operation.PUT, entity1.getOperation());
        Assert.assertEquals(Operation.DEL_FROM_MAP, entity2.getOperation());
    }

    /**
     * StringKey implements {@link CacheKeyConverter}.
     */
    public class StringKey implements CacheKeyConverter<StringKey> {

        /** The key. */
        private String key;

        /**
         * Instantiates a new string key.
         */
        public StringKey() {
        }

        /**
         * Instantiates a new string key.
         *
         * @param key the key
         */
        public StringKey(String key) {
            this.key = key;
        }

        /**
         * Gets the key.
         *
         * @return the key
         */
        public String getKey() {
            return key;
        }

        /**
         * Sets the key.
         *
         * @param key the new key
         */
        public void setKey(String key) {
            this.key = key;
        }

        /**
         * Convert from.
         *
         * @param key the key
         * @return the string key
         */
        @Override
        public StringKey convertFrom(String key) {
            return new StringKey(key);
        }

        /**
         * Convert to string.
         *
         * @return the string
         */
        @Override
        public String convertToString() {
            return key;
        }

    }

}
