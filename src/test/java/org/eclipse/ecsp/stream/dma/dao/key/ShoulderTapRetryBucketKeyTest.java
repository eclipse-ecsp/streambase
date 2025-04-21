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

import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.stream.dma.dao.key.ShoulderTapRetryBucketKey;
import org.junit.Assert;
import org.junit.Test;


/**
 * {@link ShoulderTapRetryBucketKeyTest} test class for {@link ShoulderTapRetryBucketKey}.
 */
public class ShoulderTapRetryBucketKeyTest {

    /**
     * Test shoulder tap shoulder tap retry bucket key long string string.
     */
    @Test
    public void testShoulderTapShoulderTapRetryBucketKeyLongStringString() {
        ShoulderTapRetryBucketKey key = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_1000, key.getTimestamp());
    }

    /**
     * Test compare to.
     */
    @Test
    public void testCompareTo() {
        ShoulderTapRetryBucketKey key1 = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        ShoulderTapRetryBucketKey key2 = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        ShoulderTapRetryBucketKey key3 = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_2000);
        ShoulderTapRetryBucketKey key4 = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_500);
        Assert.assertEquals(0, key1.compareTo(key2));
        Assert.assertEquals(Constants.LONG_MINUS_ONE, key1.compareTo(key3));
        Assert.assertEquals(1, key1.compareTo(key4));
    }

    /**
     * Test convert from string.
     */
    @Test
    public void testConvertFromString() {
        String keyStr = "12345";
        ShoulderTapRetryBucketKey key = new ShoulderTapRetryBucketKey();
        ShoulderTapRetryBucketKey newkey = key.convertFrom(keyStr);
        Assert.assertEquals(TestConstants.LONG_12345, newkey.getTimestamp());
    }

    /**
     * Test convert to string.
     */
    @Test
    public void testConvertToString() {
        ShoulderTapRetryBucketKey key = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        String ts = key.convertToString();
        Assert.assertEquals("1000", ts);
    }

    /**
     * Test get timestamp.
     */
    @Test
    public void testGetTimestamp() {
        ShoulderTapRetryBucketKey key = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_1000, key.getTimestamp());
    }

    /**
     * Test set timestamp.
     */
    @Test
    public void testSetTimestamp() {
        ShoulderTapRetryBucketKey key = new ShoulderTapRetryBucketKey(TestConstants.THREAD_SLEEP_TIME_2000);
        key.setTimestamp(TestConstants.THREAD_SLEEP_TIME_1000);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_1000, key.getTimestamp());
    }

    /**
     * Test get map key.
     */
    @Test
    public void testGetMapKey() {
        Assert.assertEquals("SHOULDER_TAP_RETRY_BUCKET:service:task",
                ShoulderTapRetryBucketKey.getMapKey("service", "task"));
    }
}
