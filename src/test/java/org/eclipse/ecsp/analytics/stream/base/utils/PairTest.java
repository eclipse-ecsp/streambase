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

import org.eclipse.ecsp.analytics.stream.base.utils.Pair;
import org.junit.Assert;
import org.junit.Test;



/**
 * {@link PairTest}.
 */
public class PairTest {

    /**
     * Pair getter setter test.
     */
    @Test
    public void pairGetterSetterTest() {
        Pair<String, String> pairObj = new Pair<>();
        pairObj.setA("obj1");
        pairObj.setB("obj2");
        Assert.assertEquals("obj1", pairObj.getA());
        Assert.assertEquals("obj2", pairObj.getB());
    }

    /**
     * Pair hash code tester.
     */
    @Test
    public void pairHashCodeTester() {
        Pair<String, String> pairObj = new Pair<>("obj1", "obj2");
        Pair<String, String> pairObj1 = new Pair<>("obj3", "obj4");
        Assert.assertNotEquals(pairObj.hashCode(), pairObj1.hashCode());
    }

    /**
     * Pair equals tester.
     */
    @Test
    public void pairEqualsTester() {
        Pair<String, String> pairObj = new Pair<>("obj1", "obj2");
        Pair<String, String> pairObj1 = new Pair<>("obj3", "obj4");
        Assert.assertEquals(pairObj, pairObj);
        Assert.assertNotEquals(null, pairObj);
        Assert.assertNotEquals(pairObj, pairObj1);
        Pair<String, String> pairObj3 = new Pair<>(null, "obj2");
        Pair<String, String> pairObj4 = new Pair<>("obj1", null);
        Assert.assertNotEquals(pairObj3, pairObj);
        Assert.assertNotEquals(pairObj4, pairObj);
        Pair<String, String> pairObj5 = new Pair<>("obj1", "obj4");
        Assert.assertNotEquals(pairObj5, pairObj);
    }

}
