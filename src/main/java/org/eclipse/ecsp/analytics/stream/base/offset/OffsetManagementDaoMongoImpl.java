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

import org.eclipse.ecsp.entities.IgniteEntity;
import org.eclipse.ecsp.nosqldao.IgniteCriteria;
import org.eclipse.ecsp.nosqldao.IgniteCriteriaGroup;
import org.eclipse.ecsp.nosqldao.IgniteQuery;
import org.eclipse.ecsp.nosqldao.Operator;
import org.eclipse.ecsp.nosqldao.mongodb.IgniteBaseDAOMongoImpl;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.List;

/**
 * /**
 * DAO class for maintaining the kafka topic offsets in MongoDB for 
 * stream-base's manual offset management feature.
 *
 * @param <String> the generic type
 * @param <E> the element type
 */
public abstract class OffsetManagementDaoMongoImpl<String, 
                E extends IgniteEntity> extends IgniteBaseDAOMongoImpl<String, E> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(OffsetManagementDaoMongoImpl.class);

    /**
     * This method is used to get all documents form mongocollection when filtered on kafkatopic.
     *
     * @param kafkaTopic kafkaTopic
     * @return List
     */
    public List<E> getTopicOffsetList(String kafkaTopic) {
        logger.debug("Get Topic Offset for kafkaTopic {}", kafkaTopic);
        IgniteCriteria criteriaKafkaTopic = new IgniteCriteria("kafkaTopic", Operator.EQ, kafkaTopic);

        IgniteCriteriaGroup criteriaGroup = new IgniteCriteriaGroup(criteriaKafkaTopic);
        IgniteQuery query = new IgniteQuery(criteriaGroup);

        List<E> topicOffsetList = find(query);
        if (!topicOffsetList.isEmpty()) {
            logger.debug("Recieved {} offsets for topic:{} and entries are:{}", 
                    topicOffsetList.size(), kafkaTopic, topicOffsetList);
        } else {
            logger.debug("No entries found for kafkaTopic {}", kafkaTopic);
        }
        return topicOffsetList;
    }
}
