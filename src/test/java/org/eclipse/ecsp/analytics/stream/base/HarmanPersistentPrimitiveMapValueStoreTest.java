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
import org.eclipse.ecsp.analytics.stream.base.kafka.SingleNodeKafkaCluster;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentPrimitiveMapValueStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * test class {@link HarmanPersistentPrimitiveMapValueStoreTest}.
 */
@RunWith(MockitoJUnitRunner.class)
public class HarmanPersistentPrimitiveMapValueStoreTest {

    /** The Constant KAFKA_CLUSTER. */
    @ClassRule
    public static final SingleNodeKafkaCluster KAFKA_CLUSTER = new SingleNodeKafkaCluster();
    
    /** The ks props. */
    protected Properties ksProps;
    
    /** The consumer props. */
    protected Properties consumerProps;
    
    /** The producer props. */
    protected Properties producerProps;
    
    /** The harman persistent primitive map value store. */
    @Mock
    private HarmanPersistentPrimitiveMapValueStore harmanPersistentPrimitiveMapValueStore;

    /**
     * Setup.
     *
     * @throws Exception the exception
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        harmanPersistentPrimitiveMapValueStore = Mockito.spy(new HarmanPersistentPrimitiveMapValueStore("test", true));
    }

    /**
     * Test name.
     */
    @Test
    public void testName() {
        String storeName = "test";
        Assert.assertEquals(storeName, harmanPersistentPrimitiveMapValueStore.name());
    }

    /**
     * Test persistant.
     */
    @Test
    public void testPersistant() {
        Assert.assertTrue(harmanPersistentPrimitiveMapValueStore.persistent());
    }

    /**
     * Test close.
     */
    @Test
    public void testClose() {
        harmanPersistentPrimitiveMapValueStore = new HarmanPersistentPrimitiveMapValueStore("test", false);
        harmanPersistentPrimitiveMapValueStore.close();
        Assert.assertFalse(harmanPersistentPrimitiveMapValueStore.isOpen());
    }

    /**
     * Test is open.
     */
    @Test
    public void testIsOpen() {
        harmanPersistentPrimitiveMapValueStore = new HarmanPersistentPrimitiveMapValueStore("test", false);
        harmanPersistentPrimitiveMapValueStore.flush();
        Assert.assertFalse(harmanPersistentPrimitiveMapValueStore.isOpen());
    }

    /**
     * Test put all.
     */
    @Test
    public void testPutAll() {
        Map myMap = new HashMap<String, Integer>();
        myMap.put("1", 1);
        KeyValue<String, Map<?, ?>> keyVal1 = new KeyValue<>("abc", myMap);
        List<KeyValue<String, Map<?, ?>>> list = new ArrayList<>();
        list.add(keyVal1);
        Assert.assertThrows(NullPointerException.class,
                () -> harmanPersistentPrimitiveMapValueStore.putAll(list));
        Assert.assertThrows(NullPointerException.class,
                () -> harmanPersistentPrimitiveMapValueStore.get("abc"));

    }

    /**
     * Test approximate num entries.
     */
    @Test()
    public void testApproximateNumEntries() {
        Map myMap = new HashMap<String, Integer>();
        myMap.put("1", 1);
        KeyValue<String, Map> keyVal1 = new KeyValue<String, Map>("abc", myMap);
        List<KeyValue<String, Map>> list = new ArrayList<KeyValue<String, Map>>();
        list.add(keyVal1);
        Assert.assertThrows(NullPointerException.class,
                () -> harmanPersistentPrimitiveMapValueStore.approximateNumEntries());
    }

}