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

import org.eclipse.ecsp.stream.dma.dao.key.RetryRecordKey;
import org.junit.Assert;
import org.junit.Test;



/**
 * class {@link RetryRecordKeyTest}.
 */
public class RetryRecordKeyTest {

    /**
     * Test hash code.
     */
    @Test
    public void testHashCode() {
        RetryRecordKey key1 = new RetryRecordKey("msg1", "taskId1");
        RetryRecordKey key2 = new RetryRecordKey("msg1", "taskId1");
        RetryRecordKey key3 = new RetryRecordKey("msg2", "taskId2");
        RetryRecordKey key4 = new RetryRecordKey("msg3", "taskId1");
        Assert.assertEquals(key1.hashCode(), key2.hashCode());
        Assert.assertNotEquals(key1, key3);
        Assert.assertNotEquals(key3, key4);
    }

    /**
     * Test retry record key string.
     */
    @Test
    public void testRetryRecordKeyString() {
        RetryRecordKey key1 = new RetryRecordKey("msg1", "taskId1");
        Assert.assertEquals("msg1", key1.getKey());
        Assert.assertEquals("taskId1", key1.getTaskID());
    }

    /**
     * Test get key.
     */
    @Test
    public void testGetKey() {
        RetryRecordKey key1 = new RetryRecordKey("msg1", "taskId1");
        Assert.assertEquals("msg1", key1.getKey());
    }

    /**
     * Test get task id.
     */
    @Test
    public void testGetTaskId() {
        RetryRecordKey key1 = new RetryRecordKey("msg1", "taskId1");
        Assert.assertEquals("taskId1", key1.getTaskID());
    }

    /**
     * Test set key.
     */
    @Test
    public void testSetKey() {
        RetryRecordKey key = new RetryRecordKey();
        key.setKey("keyNew");
        Assert.assertEquals("keyNew", key.getKey());
    }

    /**
     * Test set task id.
     */
    @Test
    public void testSetTaskId() {
        RetryRecordKey key = new RetryRecordKey();
        key.setTaskId("taskId1");
        Assert.assertEquals("taskId1", key.getTaskID());
    }

    /**
     * Test convert from string.
     */
    @Test
    public void testConvertFromString() {
        RetryRecordKey key1 = new RetryRecordKey();
        RetryRecordKey key = key1.convertFrom("taskId1:msg1");
        Assert.assertEquals("msg1", key.getKey());
        Assert.assertEquals("taskId1", key.getTaskID());
    }

    /**
     * Test convert to string.
     */
    @Test
    public void testConvertToString() {
        RetryRecordKey key = new RetryRecordKey("msg1", "taskId1");
        Assert.assertEquals("taskId1:msg1", key.convertToString());
    }

    /**
     * Test equals object.
     */
    @Test
    public void testEqualsObject() {
        RetryRecordKey key1 = new RetryRecordKey("msg1", "taskId1");
        RetryRecordKey key2 = new RetryRecordKey("msg1", "taskId1");
        RetryRecordKey key3 = new RetryRecordKey("msg2", "taskId1");
        RetryRecordKey key4 = new RetryRecordKey("msg3", "taskId1");
        Assert.assertEquals(key1, key2);
        Assert.assertNotEquals(key1, key3);
        Assert.assertNotEquals(key3, key4);
    }

    /**
     * Test get map key.
     */
    @Test
    public void testGetMapKey() {
        Assert.assertEquals("RETRY_MESSAGEID:service:task", RetryRecordKey.getMapKey("service", "task"));
    }

}
