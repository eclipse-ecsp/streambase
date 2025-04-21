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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.GenericSortedMapStateStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 *  class {@link GenericSortedMapStateStoreTest}.
 */
public class GenericSortedMapStateStoreTest {

    /** The map store. */
    private GenericSortedMapStateStoreImpl mapStore = new GenericSortedMapStateStoreImpl();

    /**
     * Test name.
     */
    @Test
    public void testName() {
        String storeName = "GenericSortedMapStateStoreName";
        Assert.assertEquals(storeName, mapStore.name());
    }

    /**
     * Test persistent.
     */
    @Test
    public void testPersistent() {
        Assert.assertFalse(mapStore.persistent());
    }

    /**
     * Test is open.
     */
    @Test
    public void testIsOpen() {
        Assert.assertTrue(mapStore.isOpen());
    }

    /**
     * Test get.
     */
    @Test
    public void testGet() {
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        Assert.assertEquals("abc", mapStore.get(TestConstants.THREAD_SLEEP_TIME_123));
    }

    /**
     * Test range.
     */
    @Test
    public void testRange() {
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_223, "bcd");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_223, "cde");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_423, "def");

        Set<Long> keys = new HashSet<Long>();
        keys.add(TestConstants.THREAD_SLEEP_TIME_223);
        keys.add(TestConstants.THREAD_SLEEP_TIME_323);
        keys.add(TestConstants.THREAD_SLEEP_TIME_423);

        Set<Long> actualKeys = new HashSet<Long>();

        KeyValueIterator<Long, String> keyValItr = mapStore
                .range(TestConstants.THREAD_SLEEP_TIME_200, TestConstants.THREAD_SLEEP_TIME_500);
        while (keyValItr.hasNext()) {
            KeyValue<Long, String> keyValue = keyValItr.next();
            actualKeys.add(keyValue.key);
        }
        Assert.assertEquals(keys.size(), actualKeys.size());
        Assert.assertTrue(actualKeys.containsAll(keys));
    }

    /**
     * Test all.
     */
    @Test
    public void testAll() {
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_223, "bcd");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_323, "cde");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_423, "def");

        Set<Long> keys = new HashSet<Long>();
        keys.add(TestConstants.THREAD_SLEEP_TIME_123);
        keys.add(TestConstants.THREAD_SLEEP_TIME_223);
        keys.add(TestConstants.THREAD_SLEEP_TIME_323);
        keys.add(TestConstants.THREAD_SLEEP_TIME_423);

        Set<Long> actualKeys = new HashSet<Long>();

        KeyValueIterator<Long, String> keyValItr = mapStore.all();
        while (keyValItr.hasNext()) {
            KeyValue<Long, String> keyValue = keyValItr.next();
            actualKeys.add(keyValue.key);
        }
        Assert.assertEquals(keys.size(), actualKeys.size());
        Assert.assertTrue(actualKeys.containsAll(keys));
    }

    /**
     * Test approximate num entries.
     */
    @Test
    public void testApproximateNumEntries() {
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_223, "bcd");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_323, "cde");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_423, "def");
        Assert.assertEquals(TestConstants.FOUR, mapStore.approximateNumEntries());
    }

    /**
     * Test put if absent.
     */
    @Test
    public void testPutIfAbsent() {
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        mapStore.putIfAbsent(TestConstants.THREAD_SLEEP_TIME_123, "def");
        Assert.assertEquals("abc", mapStore.get(TestConstants.THREAD_SLEEP_TIME_123));
    }

    /**
     * Test put all.
     */
    @Test
    public void testPutAll() {
        KeyValue<Long, String> keyVal1 = new KeyValue<Long, String>(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        KeyValue<Long, String> keyVal2 = new KeyValue<Long, String>(TestConstants.THREAD_SLEEP_TIME_223, "def");
        List<KeyValue<Long, String>> list = new ArrayList<KeyValue<Long, String>>();
        list.add(keyVal1);
        list.add(keyVal2);
        mapStore.putAll(list);
        KeyValueIterator<Long, String> keyValItr = mapStore.all();
        Set<Long> actualKeys = new HashSet<Long>();
        while (keyValItr.hasNext()) {
            KeyValue<Long, String> keyValue = keyValItr.next();
            actualKeys.add(keyValue.key);
        }
        Assert.assertEquals(TestConstants.TWO, actualKeys.size());
    }

    /**
     * Test delete.
     */
    @Test
    public void testDelete() {
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        mapStore.delete(TestConstants.THREAD_SLEEP_TIME_123);
        Assert.assertNull(mapStore.get(TestConstants.THREAD_SLEEP_TIME_123));
    }

    /**
     * Test get head.
     */
    @Test
    public void testGetHead() {
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_223, "bcd");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_323, "cde");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_423, "def");

        Set<Long> keys = new HashSet<Long>();
        keys.add(TestConstants.THREAD_SLEEP_TIME_123);
        keys.add(TestConstants.THREAD_SLEEP_TIME_223);

        KeyValueIterator<Long, String> keyValItr = mapStore.getHead(TestConstants.THREAD_SLEEP_TIME_300);
        Set<Long> actualKeys = new HashSet<Long>();
        while (keyValItr.hasNext()) {
            KeyValue<Long, String> keyValue = keyValItr.next();
            actualKeys.add(keyValue.key);
        }
        Assert.assertEquals(TestConstants.TWO, actualKeys.size());
        Assert.assertTrue(actualKeys.containsAll(keys));
    }

    /**
     * Test get tail.
     */
    @Test
    public void testGetTail() {
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_123, "abc");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_223, "bcd");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_323, "cde");
        mapStore.put(TestConstants.THREAD_SLEEP_TIME_423, "def");

        Set<Long> keys = new HashSet<Long>();
        keys.add(TestConstants.THREAD_SLEEP_TIME_323);
        keys.add(TestConstants.THREAD_SLEEP_TIME_423);

        KeyValueIterator<Long, String> keyValItr = mapStore.getTail(TestConstants.THREAD_SLEEP_TIME_300);
        Set<Long> actualKeys = new HashSet<Long>();
        while (keyValItr.hasNext()) {
            KeyValue<Long, String> keyValue = keyValItr.next();
            actualKeys.add(keyValue.key);
        }
        Assert.assertEquals(TestConstants.TWO, actualKeys.size());
        Assert.assertTrue(actualKeys.containsAll(keys));
    }

    /**
     * class GenericSortedMapStateStoreImpl extends GenericSortedMapStateStore.
     */
    public class GenericSortedMapStateStoreImpl extends GenericSortedMapStateStore<Long, String> {

    }

}
