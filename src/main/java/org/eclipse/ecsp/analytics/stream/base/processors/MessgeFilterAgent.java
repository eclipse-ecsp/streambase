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

package org.eclipse.ecsp.analytics.stream.base.processors;

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamBaseConstant;
import org.eclipse.ecsp.cache.GetStringRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.cache.PutStringRequest;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * class MessgeFilterAgent.
 */
@ConditionalOnProperty(name = PropertyNames.MSG_FILTER_ENABLED, havingValue = "true")
@Component
public class MessgeFilterAgent {
    
    /** The Constant LOGGER. */
    private static final IgniteLogger LOGGER = IgniteLoggerFactory.getLogger(MessgeFilterAgent.class);

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + "}")
    private String serviceName;

    /** The ttl. */
    @Value("${" + PropertyNames.MSG_FILTER_TTL_MS + ":60000}")
    private long ttl;

    /** The cache. */
    @Autowired
    private IgniteCache cache;

    /** The message filter. */
    @Autowired
    private MessageFilter messageFilter;

    /**
     * isDuplicate().
     *
     * @param igniteKey igniteKey
     * @param igniteEvent igniteEvent
     * @return boolean
     */
    public boolean isDuplicate(IgniteKey<?> igniteKey, IgniteEvent igniteEvent) {
        LOGGER.info(igniteEvent, "MessgeFilterAgent processing for serviceName: {} , igniteKey: {}", 
                serviceName, igniteKey);
        boolean isDuplicateFlag = false;
        String cacheKey = StreamBaseConstant.MSG_FILTER + serviceName + StreamBaseConstant.UNDERSCORE
                + messageFilter.filter(igniteKey, igniteEvent);
        String cacheValue = cache.getString(new GetStringRequest().withKey(cacheKey).withNamespaceEnabled(false));
        LOGGER.debug("Cache value retreived by DuplicateMessgeFilteringAgent: {} for cache key: {}", 
                cacheValue, cacheKey);

        int duplicateMsgCount = 1;

        PutStringRequest request = new PutStringRequest();
        request.withKey(cacheKey);

        if (cacheValue == null) {
            request.withValue(String.valueOf(duplicateMsgCount));
            isDuplicateFlag = false;
        } else {
            duplicateMsgCount = Integer.parseInt(cacheValue);
            request.withValue(String.valueOf(duplicateMsgCount + 1));
            isDuplicateFlag = true;
        }
        request.withTtlMs(ttl);
        request.withNamespaceEnabled(false);
        cache.putString(request);

        LOGGER.debug("Cache updated with key:{}, value: {} and ttl: {}", request.getKey(), 
                request.getValue(), request.getTtlMs());

        LOGGER.info(igniteEvent, "Returning :{} for MessgeFilterAgent duplicate check for serviceName: {}, "
                + "igniteKey: {}", isDuplicateFlag, serviceName, igniteKey);
        return isDuplicateFlag;
    }
}