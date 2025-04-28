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

import java.util.Map;

/**
 * interface {@link EventWrapperBase}.
 */
public interface EventWrapperBase {

    /**
     * To json.
     *
     * @return the string
     */
    String toJson();

    /**
     * Gets the property.
     *
     * @param wrapper the wrapper
     * @param name the name
     * @return the property
     */
    Object getProperty(Map<?, ?> wrapper, String name);

    /**
     * Gets the property.
     *
     * @param name the name
     * @return the property
     */
    Object getProperty(String name);

    /**
     * Gets the property by expr.
     *
     * @param name the name
     * @return the property by expr
     */
    Object getPropertyByExpr(String name);

    /**
     * Gets the property by expr.
     *
     * @param name the name
     * @param byFieldToBeSorted the by field to be sorted
     * @param sort the sort
     * @return the property by expr
     */
    Object getPropertyByExpr(String name, String byFieldToBeSorted, String sort);

    /**
     * Gets the raw event.
     *
     * @return the raw event
     */
    byte[] getRawEvent();

    /**
     * Gets the parses the exception.
     *
     * @return the parses the exception
     */
    Exception getParseException();

    /**
     * Checks if is valid.
     *
     * @return true, if is valid
     */
    boolean isValid();

}
