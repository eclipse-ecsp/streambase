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
 * class Pair implements Serializable.
 *
 * @param <A> the generic type
 * @param <B> the generic type
 */
public class Pair<A, B> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -2994843765375347811L;
    
    /** The a. */
    private A a;
    
    /** The b. */
    private B b;

    /**
     * Instantiates a new pair.
     */
    public Pair() {
    }

    /**
     * Instantiates a new pair.
     *
     * @param a the a
     * @param b the b
     */
    public Pair(A a, B b) {
        this.a = a;
        this.b = b;
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
     * Hash code.
     *
     * @return the int
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((a == null) ? 0 : a.hashCode());
        result = prime * result + ((b == null) ? 0 : b.hashCode());
        return result;
    }

    /**
     * Equals.
     *
     * @param obj the obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Pair<?, ?> other = (Pair) obj;
        if (a == null) {
            if (other.a != null) {
                return false;
            }
        } else if (!a.equals(other.a)) {
            return false;
        }
        if (b == null) {
            if (other.b != null) {
                return false;
            }
        } else if (!b.equals(other.b)) {
            return false;
        }
        return true;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "Pair [a=" + a + ", b=" + b + "]";
    }

}
