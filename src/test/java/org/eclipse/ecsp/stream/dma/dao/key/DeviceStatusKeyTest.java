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

import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test class for {@link DeviceStatusKey}.
 */
public class DeviceStatusKeyTest {

    /**
     * Test device status key string.
     */
    @Test
    public void testDeviceStatusKeyString() {
        DeviceStatusKey key = new DeviceStatusKey("key");
        Assert.assertEquals("key", key.getKey());
    }

    /**
     * Test convert from string.
     */
    @Test
    public void testConvertFromString() {
        DeviceStatusKey deviceStatusKey = new DeviceStatusKey();
        DeviceStatusKey converted = deviceStatusKey.convertFrom("key");
        Assert.assertEquals("key", converted.getKey());
    }

    /**
     * Test hash code.
     */
    @Test
    public void testHashCode() {
        DeviceStatusKey key1 = new DeviceStatusKey("key");
        DeviceStatusKey key2 = new DeviceStatusKey("key");
        Assert.assertEquals(key1.hashCode(), key2.hashCode());
        DeviceStatusKey key3 = new DeviceStatusKey("key3");
        Assert.assertNotEquals(key1.hashCode(), key3.hashCode());
    }

    /**
     * Test get key.
     */
    @Test
    public void testGetKey() {
        DeviceStatusKey key = new DeviceStatusKey("key");
        Assert.assertEquals("key", key.getKey());
    }

    /**
     * Test set key.
     */
    @Test
    public void testSetKey() {
        DeviceStatusKey key = new DeviceStatusKey();
        key.setKey("keyNew");
        Assert.assertEquals("keyNew", key.getKey());
    }

    /**
     * Test convert to string.
     */
    @Test
    public void testConvertToString() {
        DeviceStatusKey key = new DeviceStatusKey("keyConvert");
        Assert.assertEquals("keyConvert", key.convertToString());
    }

    /**
     * Test equals object.
     */
    @Test
    public void testEqualsObject() {
        DeviceStatusKey key1 = new DeviceStatusKey("key");
        DeviceStatusKey key2 = new DeviceStatusKey("key");
        Assert.assertEquals(key1, key2);
        DeviceStatusKey key3 = new DeviceStatusKey("key3");
        Assert.assertNotEquals(key1, key3);
    }

}
