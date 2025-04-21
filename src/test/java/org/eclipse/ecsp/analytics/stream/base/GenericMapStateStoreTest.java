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
import org.eclipse.ecsp.analytics.stream.base.stores.GenericMapStateStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * UT class {@link GenericMapStateStoreTest}.
 */
public class GenericMapStateStoreTest {

    /** The map store. */
    private GenericMapStateStoreImpl mapStore = new GenericMapStateStoreImpl();

    /**
     * Test name.
     */
    @Test
    public void testName() {
        String storeName = "GenericMapStateStoreName";
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
        mapStore.put("abc", "abc");
        Assert.assertEquals("abc", mapStore.get("abc"));
    }

    /**
     * Test range.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testRange() {
        mapStore.range("abc", "bcd");
    }

    /**
     * Test all.
     */
    @Test
    public void testAll() {
        mapStore.put("abc", "abc");
        mapStore.put("bcd", "bcd");
        mapStore.put("cde", "cde");
        mapStore.put("def", "def");

        Set<String> keys = new HashSet<String>();
        keys.add("abc");
        keys.add("bcd");
        keys.add("cde");
        keys.add("def");

        Set<String> actualKeys = new HashSet<String>();

        KeyValueIterator<String, String> keyValItr = mapStore.all();
        while (keyValItr.hasNext()) {
            KeyValue<String, String> keyValue = keyValItr.next();
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
        mapStore.put("abc", "abc");
        mapStore.put("bcd", "bcd");
        mapStore.put("cde", "cde");
        mapStore.put("def", "def");
        Assert.assertEquals(Constants.FOUR, mapStore.approximateNumEntries());
    }

    /**
     * Test put if absent.
     */
    @Test
    public void testPutIfAbsent() {
        mapStore.put("abc", "abc");
        mapStore.putIfAbsent("abc", "def");
        Assert.assertEquals("abc", mapStore.get("abc"));
    }

    /**
     * Test put all.
     */
    @Test
    public void testPutAll() {
        KeyValue<String, String> keyVal1 = new KeyValue<String, String>("abc", "abc");
        KeyValue<String, String> keyVal2 = new KeyValue<String, String>("def", "def");
        List<KeyValue<String, String>> list = new ArrayList<KeyValue<String, String>>();
        list.add(keyVal1);
        list.add(keyVal2);
        mapStore.putAll(list);
        KeyValueIterator<String, String> keyValItr = mapStore.all();
        Set<String> actualKeys = new HashSet<String>();
        while (keyValItr.hasNext()) {
            KeyValue<String, String> keyValue = keyValItr.next();
            actualKeys.add(keyValue.key);
        }
        Assert.assertEquals(Constants.TWO, actualKeys.size());
    }

    /**
     * Test delete.
     */
    @Test
    public void testDelete() {
        mapStore.put("abc", "abc");
        mapStore.delete("abc");
        Assert.assertNull(mapStore.get("abc"));
    }

    /**
     * inner class GenericMapStateStoreImpl extends GenericMapStateStore.
     */
    public class GenericMapStateStoreImpl extends GenericMapStateStore<String, String> {

    }

}
