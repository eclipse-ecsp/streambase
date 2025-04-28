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

package org.eclipse.ecsp.analytics.stream.base.kafka.internal;

import jakarta.annotation.PostConstruct;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.offset.OffsetManagementDaoMongoImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * BackdoorKafkaTopicOffsetDAOMongoImpl  extends {@link OffsetManagementDaoMongoImpl} which is a abstract class that
 * extends {@link org.eclipse.ecsp.nosqldao.mongodb.IgniteBaseDAOMongoImpl}.
 */
@Repository
public class BackdoorKafkaTopicOffsetDAOMongoImpl extends
        OffsetManagementDaoMongoImpl<String, BackdoorKafkaTopicOffset> {

    /** The service name identifier. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceNameIdentifier;

    /** The collection. */
    private String collection;

    /**
     * Gets the overriding collection name.
     *
     * @return the overriding collection name
     */
    @Override
    public String getOverridingCollectionName() {
        return collection;
    }

    /**
     * Sets the up.
     */
    @PostConstruct
    public void setUp() {
        collection = new StringBuilder().append("backdoorOffset").append(serviceNameIdentifier).toString();
    }

}
