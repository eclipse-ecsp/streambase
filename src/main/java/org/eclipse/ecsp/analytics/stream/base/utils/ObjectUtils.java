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
import java.util.Collection;
import java.util.Objects;


/**
 * {@link ObjectUtils}.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class ObjectUtils {

    /**
     * requireNonEmpty().
     */
    protected ObjectUtils() {
        
    }
    
    /**
     * Checks whether an object instance is null or not.

     * @param <T> The type of the object.
     * @param obj The object to apply the check on.
     * @param errorMsg The error to throw if the object is found null.
     * @return The object instance.
     */
    public static <T> T requireNonEmpty(T obj, String errorMsg) {
        Objects.requireNonNull(obj, errorMsg);
        if (obj instanceof String str && str.isEmpty()) {
            throw new ObjectUtilsException(errorMsg);
        }
        return obj;
    }

    /**
     * requireSizeOf().
     *
     * @param <T> the generic type
     * @param t t
     * @param expectedSize expectedSize
     * @param errorMsg errorMsg
     * @return boolean
     */
    public static <T> boolean requireSizeOf(Collection<T> t, int expectedSize, String errorMsg) {
        if (t.size() != expectedSize) {
            throw new ObjectUtilsException(errorMsg);
        }
        return true;
    }

    /**
     * Require non null.
     *
     * @param <T> the generic type
     * @param obj the obj
     * @param errorMsg the error msg
     * @return the t
     */
    public static <T> T requireNonNull(T obj, String errorMsg) {
        return Objects.requireNonNull(obj, errorMsg);
    }

    /**
     * requireMinSize().
     *
     * @param <T> the generic type
     * @param t Collection
     * @param expectedSize expectedSize
     * @param errorMsg errorMsg
     * @return boolean
     */
    public static <T> boolean requireMinSize(Collection<T> t, int expectedSize, String errorMsg) {
        if (t.size() < expectedSize) {
            throw new ObjectUtilsException(errorMsg);
        }
        return true;
    }

    /**
     * requiresNotNullAndNotEmpy().
     *
     * @param <T> the generic type
     * @param t Collection
     * @param errorMsg errorMsg
     * @return boolean
     */
    public static <T> boolean requiresNotNullAndNotEmpy(Collection<T> t, String errorMsg) {
        Objects.requireNonNull(t, errorMsg);
        if (t.isEmpty()) {
            throw new ObjectUtilsException(errorMsg);
        }
        return true;
    }

    /**
     * to check if a integer number is negative or not.
     *
     * @param <T> the generic type
     * @param obj obj
     * @param errorMessage errorMessage
     * @return the t
     */
    public static <T> T requireNonNegative(T obj, String errorMessage) {
        Objects.requireNonNull(obj, errorMessage);
        Integer num = null;
        if (obj instanceof Integer integer) {
            num = integer;
        } else if (obj instanceof String str) {
            num = Integer.parseInt(str);
        } else {
            throw new ObjectUtilsException("Method requireNonNegative suppports only integers or strings.");
        }

        if (num < 0) {
            throw new ObjectUtilsException(errorMessage);
        }
        return obj;

    }

}