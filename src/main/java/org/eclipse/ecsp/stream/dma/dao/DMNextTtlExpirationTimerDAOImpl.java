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

package org.eclipse.ecsp.stream.dma.dao;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.nosqldao.mongodb.IgniteBaseDAOMongoImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;


/**
 * class DMNextTtlExpirationTimerDAOImpl extends IgniteBaseDAOMongoImpl.
 */
@Repository
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DMNextTtlExpirationTimerDAOImpl extends IgniteBaseDAOMongoImpl<String, DMNextTtlExpirationTimer>
        implements DMNextTtlExpirationTimerDAO {

    /** The collection. */
    private volatile String collection;

    /**
     * Gets the overriding collection name.
     *
     * @return the overriding collection name
     */
    @Override
    public String getOverridingCollectionName() {
        if (StringUtils.isEmpty(collection)) {
            collection = new StringBuilder().append("dmNextTtlExpirationTimer").append(serviceName).toString();
        }
        return collection;
    }

}
