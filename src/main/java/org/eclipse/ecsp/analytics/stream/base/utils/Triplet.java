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


/**
 * {@link Triplet}.
 *
 * @param <A> a
 * @param <B> b
 * @param <C> c
 */
public class Triplet<A, B, C> {

    /** The a. */
    private A a;
    
    /** The b. */
    private B b;
    
    /** The c. */
    private C c;

    /**
     * Triplet() public constructor.
     *
     * @param a the a
     * @param b the b
     * @param c the c
     */
    public Triplet(A a, B b, C c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    /**
     * Gets the a.
     *
     * @return the a
     */
    public A getA() {
        return a;
    }

    /**
     * Sets the a.
     *
     * @param a the new a
     */
    public void setA(A a) {
        this.a = a;
    }

    /**
     * Gets the b.
     *
     * @return the b
     */
    public B getB() {
        return b;
    }

    /**
     * Sets the b.
     *
     * @param b the new b
     */
    public void setB(B b) {
        this.b = b;
    }

    /**
     * Gets the c.
     *
     * @return the c
     */
    public C getC() {
        return c;
    }

    /**
     * Sets the c.
     *
     * @param c the new c
     */
    public void setC(C c) {
        this.c = c;
    }

}
