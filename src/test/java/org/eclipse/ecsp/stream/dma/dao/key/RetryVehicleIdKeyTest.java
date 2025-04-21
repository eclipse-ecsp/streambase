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

import org.eclipse.ecsp.stream.dma.dao.key.RetryVehicleIdKey;
import org.junit.Assert;
import org.junit.Test;


/**
 * UT class RetryVehicleIdKeyTest.
 */
public class RetryVehicleIdKeyTest {    
    
    /** The key. */
    String key = "retry_test_key";

    /**
     * Test get key.
     */
    @Test
    public void testGetKey() {
        RetryVehicleIdKey retryKey = new RetryVehicleIdKey(key);
        Assert.assertEquals(key, retryKey.getKey());
        retryKey.setKey(key + "1");
        Assert.assertEquals(key + "1", retryKey.getKey());
    }

    /**
     * Test convert from.
     */
    @Test
    public void testConvertFrom() {
        RetryVehicleIdKey retryKey = new RetryVehicleIdKey();
        retryKey = retryKey.convertFrom(key);
        Assert.assertEquals(key, retryKey.getKey());
    }

    /**
     * Test hash code.
     */
    @Test
    public void testHashCode() {
        RetryVehicleIdKey retryKey = new RetryVehicleIdKey(key);
        int result = retryKey.hashCode();
        Assert.assertNotEquals(0, result);
    }

    /**
     * Test equals.
     */
    @Test
    public void testEquals() {
        RetryVehicleIdKey retryKey = new RetryVehicleIdKey(key);
        Assert.assertEquals(retryKey, retryKey);
        Assert.assertNotEquals(null, retryKey);

        String key = "abc";
        Assert.assertNotEquals(retryKey, key);

        RetryVehicleIdKey retryKey2 = new RetryVehicleIdKey();
        Assert.assertNotEquals(retryKey2, retryKey);
        
        retryKey2.setKey(key);
        Assert.assertNotEquals(retryKey2, retryKey);

        retryKey2.setKey(this.key);
        Assert.assertEquals(retryKey2, retryKey);
    }
}
