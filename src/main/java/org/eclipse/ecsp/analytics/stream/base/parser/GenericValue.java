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

package org.eclipse.ecsp.analytics.stream.base.parser;

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * class {@link GenericValue}.
 */
public class GenericValue {
    
    /** The object. */
    private Object object;
    
    /** The type. */
    private Type type;

    /**
     * GenericValue().
     *
     * @param v v
     */
    public GenericValue(Object v) {
        this.object = v;
        if (object == null) {
            type = Type.EMPTY;
        } else if (v instanceof String) {
            type = Type.STRING;
        } else if (v instanceof Number) {
            type = Type.NUMBER;
        } else if (v instanceof Boolean) {
            type = Type.BOOL;
        } else if (v instanceof List) {
            type = Type.LIST;
        } else if (v instanceof Map) {
            type = Type.MAP;
        } else {
            type = Type.STRING;
            this.object = v.toString();
        }
    }

    /**
     * asDouble().
     *
     * @return double
     * @throws NumberFormatException NumberFormatException
     */
    public double asDouble() throws NumberFormatException {
        if (type == Type.EMPTY) {
            return 0.0D;
        }
        if (type == Type.NUMBER) {
            return ((Number) object).doubleValue();
        } else {
            return Double.parseDouble(object.toString());
        }
    }
    
    /**
     * Returns a value as double.

     * @param v value.
     * @param d A double value.
     * @return the value as double.
     */
    public static double asDouble(Object v, double d) {
        if (v == null) {
            return d;
        } else if (v instanceof String value) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException nfe) {
                if ("nan".equals(value)) {
                    return Double.NaN;
                }
            }
        } else if (v instanceof Number number) {
            return number.doubleValue();
        }
        return d;
    }
    
    /**
     * asLong().
     *
     * @return long
     * @throws NumberFormatException NumberFormatException
     */
    public long asLong() throws NumberFormatException {
        if (type == Type.EMPTY) {
            return Constants.LONG_MINUS_ONE;
        }
        if (type == Type.NUMBER) {
            return ((Number) object).longValue();
        } else {
            return Long.parseLong(object.toString());
        }
    }
    
    /**
     * asLong().
     *
     * @param v v
     * @param l l
     * @return long
     */
    public static long asLong(Object v, long l) {
        if (v == null) {
            return l;
        } else if (v instanceof String value) {
            return Long.parseLong(value);
        } else if (v instanceof Number number) {
            return number.longValue();
        }
        return l;
    }    

    /**
     * asString().
     *
     * @return String
     */

    public String asString() {
        if (type == Type.EMPTY) {
            return null;
        }
        if (type == Type.STRING) {
            return (String) object;
        } else {
            return object.toString();
        }
    }

    /**
     * asBoolean().
     *
     * @return boolean
     */
    public boolean asBoolean() {
        if (type == Type.EMPTY) {
            return false;
        }
        if (type == Type.BOOL) {
            return ((Boolean) object).booleanValue();
        } else {
            return Boolean.parseBoolean(object.toString());
        }
    }

    /**
     *  asOptionalDouble().
     *
     * @return Double
     */
    public Optional<Double> asOptionalDouble() {
        if (type == Type.EMPTY) {
            return Optional.empty();
        }
        if (type == Type.NUMBER) {
            return Optional.of(((Number) object).doubleValue());
        } else {
            try {
                return Optional.of(Double.valueOf(object.toString()));
            } catch (NumberFormatException nfe) {
                return Optional.empty();
            }
        }
    }

    /**
     * asOptionalLong().
     *
     * @return Long
     */
    public Optional<Long> asOptionalLong() {
        if (type == Type.EMPTY) {
            return Optional.empty();
        }
        if (type == Type.NUMBER) {
            return Optional.of(((Number) object).longValue());
        } else {
            try {
                return Optional.of(Long.valueOf(object.toString()));
            } catch (NumberFormatException nfe) {
                return Optional.empty();
            }
        }
    }

    /**
     * The Enum Type.
     */
    private enum Type {
        
        /** The string. */
        STRING, 
 /** The number. */
 NUMBER, 
 /** The bool. */
 BOOL, 
 /** The list. */
 LIST, 
 /** The map. */
 MAP, 
 /** The empty. */
 EMPTY;
    }
}
