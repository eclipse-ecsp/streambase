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

package org.eclipse.ecsp.analytics.stream.base.idgen.internal;

import org.eclipse.ecsp.analytics.stream.base.idgen.MessageIdPartGenerator;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * class ShortCounterIdPartGenerator implements MessageIdPartGenerator.
 */
@Component
public class ShortCounterIdPartGenerator implements MessageIdPartGenerator {
    
    /** The msg id suffix. */
    private static AtomicInteger msgIdSuffix = new AtomicInteger(0);
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(ShortCounterIdPartGenerator.class);

    /**
     * Gets the msg id suffix.
     *
     * @return the msg id suffix
     */
    public static AtomicInteger getMsgIdSuffix() {
        return msgIdSuffix;
    }

    /**
     * generateIdPart().
     *
     * @param serviceName serviceName
     * @return String
     */
    public String generateIdPart(String serviceName) {
        msgIdSuffix.compareAndSet(Short.MAX_VALUE, 0);
        int suffix = msgIdSuffix.incrementAndGet();
        logger.debug("Short Counter generated the ShortCounterIdPartGenerator {}", suffix);
        return String.valueOf(suffix);
    }

}
