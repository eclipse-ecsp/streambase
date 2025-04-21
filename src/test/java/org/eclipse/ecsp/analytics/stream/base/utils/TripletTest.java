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

import org.eclipse.ecsp.analytics.stream.base.utils.Triplet;
import org.junit.Assert;
import org.junit.Test;


/**
 * {@link TripletTest}.
 */
public class TripletTest {

    /**
     * Triplets getters test.
     */
    @Test
    public void tripletsGettersTest() {
        //to cover constructor
        Triplet<String, String, String> triplet = new Triplet(new String("obj1"),
                new String("obj2"), new String("obj3"));
        Assert.assertEquals("obj1", triplet.getA());
        Assert.assertEquals("obj2", triplet.getB());
        Assert.assertEquals("obj3", triplet.getC());
    }

    /**
     * Triple setters test test.
     */
    @Test
    public void tripleSettersTestTest() {
        Triplet triplet = new Triplet(new String("obj1"), new String("obj2"), new String("obj3"));
        triplet.setA(new String("A"));
        triplet.setB(new String("B"));
        triplet.setC(new String("C"));
        Assert.assertEquals("A", triplet.getA());
        Assert.assertEquals("B", triplet.getB());
        Assert.assertEquals("C", triplet.getC());
    }

}
