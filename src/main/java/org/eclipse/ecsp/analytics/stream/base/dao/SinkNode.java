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

package org.eclipse.ecsp.analytics.stream.base.dao;

import java.util.Optional;
import java.util.Properties;

/**
 * Contract for representing a sink node.
 */
public interface SinkNode<K, V> {

    /**
     * Initialize the sink node with the given properties.
     *
     * @param prop the properties to initialize the sink node
     */
    public void init(Properties prop);

    /**
     * If required implementors should override it according Returns
     * value for a given field for a given mongo collection / DDB tablename.
     *
     * @param primaryKeyValue primaryKeyValue
     * @param fieldName fieldName
     * @param tableName tableName
     * @return String
     */
    public default Optional<String> get(K primaryKeyValue, String fieldName, String tableName) {
        return Optional.empty();
    }

    /**
     * If required implementors should override it according Delete single
     * record with the primary key from the mongo collection / DDB table
     * name.
     *
     * @param id id
     * @param tableName tableName
      */
    public default void deleteSingleRecord(K id, String tableName) {

    }

    /**
     * Flush the records.
     */
    public default void flush() {

    }

    /**
     * Close the node.
     */
    public default void close() {

    }

    /**
     * Put the record into Mongo collection / DB Table.
     *
     * @param id the Key.
     * @param value the Value.
     * @param tableName Name of the table.
     * @param primaryKeyMapping Primary key.
     */
    public void put(K id, V value, String tableName, String primaryKeyMapping);

}
