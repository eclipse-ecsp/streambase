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

package org.eclipse.ecsp.analytics.stream.base.utils;

import org.eclipse.ecsp.analytics.stream.base.exception.ObjectUtilsException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;



/**
 * ObjectUtils test for different checking methods.
 *
 * @author Binoy
 */
public class ObjectUtilsTest {

    /**
     * Testing for non null object.
     */
    @Test
    public void testObjectUtilsForRequireNonEmpty() {
        // to cover default constructor
        String message = "test message";
        assertEquals(message, ObjectUtils.requireNonEmpty(message, "No error. Because object is not null or empty"));
        Object object = new Object();
        assertEquals(object, ObjectUtils.requireNonEmpty(object, "No error. Because object is not null or empty"));
    }

    /**
     * Testing for null object.
     */
    @Test(expected = RuntimeException.class)
    public void testObjectUtilsForRequireNonEmptyWithException() {
        String message = "";
        ObjectUtils.requireNonEmpty(message, "Error.Because String is empty");
    }

    /**
     * Testing for null object.
     */
    @Test(expected = RuntimeException.class)
    public void testObjectUtilsForRequireNonEmptyWithExceptionObject() {
        Object object = null;
        ObjectUtils.requireNonEmpty(object, "Error.Because object is null");
    }

    /**
     * Testing for collection's size.
     */
    @Test
    public void testRequireSizeOfCollection() {
        List<String> list = new ArrayList<>();
        list.add("test");
        assertTrue("Collection size is not matching", ObjectUtils.requireSizeOf(list,
                1, "No error. Because list size is 1(one)"));

    }

    /**
     * Testing for collection's size.
     */
    @Test
    public void testRequireSizeOfCollectionForInvalidCollectionSize() {
        List<String> list = new ArrayList<>();
        list.add("test");
        assertThrows(ObjectUtilsException.class, () -> ObjectUtils.requireSizeOf(list, 
                Constants.TEN, "Error. Because list contains 1 item"));
    }

    /**
     * Testing for non null.
     */
    @Test()
    public void testForRequireNonNull() {
        Object object = new Object();
        assertEquals(object, ObjectUtils.requireNonNull(object, "No Error. Because object is not null"));
        assertThrows(NullPointerException.class,
                () -> ObjectUtils.requireNonNull(null, "Error. Because object is null"));
    }

    /**
     * Testing for minimum size of collection.
     */
    @Test()
    public void testForRequireMinSize() {
        List<String> list = new ArrayList<>();
        list.add("item1");
        list.add("item2");
        assertTrue(ObjectUtils.requireMinSize(list, 1, "No Error. Because collection has more than expected item"));
        assertThrows(ObjectUtilsException.class, () -> ObjectUtils.requireMinSize(list, 
                Constants.THREE, "Error. Because collection has less than expected item"));
    }

    /**
     * Testing for non null and non empty collection.
     */
    @Test()
    public void testForRequiresNotNullAndNotEmpty() {
        List<String> list = new ArrayList<>();
        list.add("item1");
        list.add("item2");
        assertTrue(ObjectUtils.requiresNotNullAndNotEmpy(list,
                "No Error. Because collection is non null and non empty"));
        list.clear();
        assertThrows(ObjectUtilsException.class,
                () -> ObjectUtils.requiresNotNullAndNotEmpy(list, "Error. Because collection is empty"));
    }

    /**
     * Testing for non negative.
     */
    @Test
    public void testForRequireNonNegative() {
        Integer positiveNumber = Constants.TEN;
        assertEquals(positiveNumber, ObjectUtils.requireNonNegative(positiveNumber, 
                "No Error. Because number is positive"));
        String positiveStrNumber = "10";
        assertEquals(positiveStrNumber, ObjectUtils.requireNonNegative(positiveStrNumber, 
                " No Error. Because number is positive"));
    }

    /**
     * Testing for non negative for exception.
     */
    @Test(expected = RuntimeException.class)
    public void testForRequireNonNegativeForException() {
        Integer negativeNumber = Constants.NEGATIVE_ONE;
        ObjectUtils.requireNonNegative(negativeNumber, "Error. Because number is negative");
    }

    /**
     * Testing for non negative for exception.
     */
    @Test(expected = RuntimeException.class)
    public void testForRequireNonNegativeObjectForException() {
        Object object = new Object();
        ObjectUtils.requireNonNegative(object, "Error. Because only supported type is int and string");
    }
}
