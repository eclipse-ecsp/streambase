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

package org.eclipse.ecsp.analytics.stream.base.offset;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidServiceNameException;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * DAO class for maintaining the kafka topic offsets in MongoDB for 
 * stream-base's manual offset management feature.
 */
@Repository
public class KafkaStreamsOffsetManagementDAOMongoImpl extends
        OffsetManagementDaoMongoImpl<String, KafkaStreamsTopicOffset> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaStreamsOffsetManagementDAOMongoImpl.class);

    /** The service name identifier. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceNameIdentifier;

    /** The mongo collection name. */
    private String mongoCollectionName;

    /**
     * Gets the overriding collection name.
     *
     * @return the overriding collection name
     */
    @Override
    public String getOverridingCollectionName() {
        return mongoCollectionName;
    }

    /**
     * setUp().
     */
    @PostConstruct
    public void setUp() {
        // Underscore will not be removed
        serviceNameIdentifier = serviceNameIdentifier.replaceAll("[^\\w\\s]", "").toLowerCase();
        if (StringUtils.isEmpty(serviceNameIdentifier)) {
            throw new InvalidServiceNameException("Service name unavailable");
        }
        logger.debug("Servicename after removing special characters is {}", serviceNameIdentifier);
        mongoCollectionName = new StringBuilder().append("kafkastreamsoffset").append(serviceNameIdentifier).toString();
    }

}
