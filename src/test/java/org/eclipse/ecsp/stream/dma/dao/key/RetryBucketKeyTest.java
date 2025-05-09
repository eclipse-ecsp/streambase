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
import org.eclipse.ecsp.stream.dma.dao.key.RetryBucketKey;
import org.junit.Assert;
import org.junit.Test;



/**
 * {@link RetryBucketKeyTest}: UT class for {@link RetryBucketKey}.
 */
public class RetryBucketKeyTest {

    /**
     * Test retry bucket key long.
     */
    @Test
    public void testRetryBucketKeyLong() {
        RetryBucketKey key = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_1000, key.getTimestamp());
    }

    /**
     * Test compare to.
     */
    @Test
    public void testCompareTo() {
        RetryBucketKey key1 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        RetryBucketKey key2 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        RetryBucketKey key3 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_2000);
        RetryBucketKey key4 = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_500);
        Assert.assertEquals(0, key1.compareTo(key2));
        Assert.assertEquals(-TestConstants.ONE, key1.compareTo(key3));
        Assert.assertEquals(1, key1.compareTo(key4));
    }

    /**
     * Test convert from string.
     */
    @Test
    public void testConvertFromString() {
        RetryBucketKey key = new RetryBucketKey();
        RetryBucketKey newkey = key.convertFrom("12345");
        Assert.assertEquals(TestConstants.LONG_12345, newkey.getTimestamp());
    }

    /**
     * Test convert to string.
     */
    @Test
    public void testConvertToString() {
        RetryBucketKey key = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_1000);
        String ts = key.convertToString();
        Assert.assertEquals("1000", ts);
    }

    /**
     * Test set timestamp.
     */
    @Test
    public void testSetTimestamp() {
        RetryBucketKey key = new RetryBucketKey(TestConstants.THREAD_SLEEP_TIME_2000);
        key.setTimestamp(TestConstants.THREAD_SLEEP_TIME_1000);
        Assert.assertEquals(TestConstants.THREAD_SLEEP_TIME_1000, key.getTimestamp());
    }

    /**
     * Test get map key.
     */
    @Test
    public void testGetMapKey() {
        Assert.assertEquals("RETRY_BUCKET:service:task", RetryBucketKey.getMapKey("service", "task"));
    }
}
