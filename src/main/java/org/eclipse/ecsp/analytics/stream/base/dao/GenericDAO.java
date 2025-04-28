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

import dev.morphia.Datastore;

import java.util.List;

/**
 * Implemented by data sources. Data sources could be MongoDB/Cassandra/RDBMS/XML or any properties file.
 *
 */
public interface GenericDAO {

    /**
     * Retrieve only one record which matches the criteria based on keyFieldName/keyFieldValue.
     *
     * @param collectionName
     *         : Name of Table/collection
     * @param keyFieldName
     *         : Field name whose key is being use. Ex : _id
     * @param keyFieldValue
     *         : Value of the field (i.e. value of keyFieldName). Ex : _id :
     *         {@code <}P202{@code >}. P202 is value.
     * @param fields
     *         : Retrieved record will have the values of specified fields.
     *
     * @return String
     */
    String getRecord(String collectionName, String keyFieldName, String keyFieldValue, String... fields);

    /**
     * Retrieve all the records from the collection.
     *
     * @param collectionName
     *         : Name of Table/collection
     * @param fields
     *         : Retrieved records will have the values of specified fields.
     * @return List
     */
    List<String> getRecords(String collectionName, String... fields);

    /**
     * getAllRecords().
     *
     * @param collectionName collectionName
     * @return List
     */
    List<String> getAllRecords(String collectionName);

    /**
     * Retrieve the data store. From CFMS.
     *
     * @return Datastore
     */
    Datastore getDataStore();
}