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

import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.idgen.MessageIdPartGenerator;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.stereotype.Component;

/**
 * class ShortHashCodeIdPartGenerator implements MessageIdPartGenerator.
 */
@Component
public class ShortHashCodeIdPartGenerator implements MessageIdPartGenerator {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(ShortHashCodeIdPartGenerator.class);

    /**
     * generateIdPart().
     *
     * @param input input
     * @return String
     */
    public String generateIdPart(String input) {
        logger.debug("Input String to generate Short hashCode is {}", input);
        if (StringUtils.isEmpty(input)) {
            throw new IllegalArgumentException("Input String cannot be empty for generating hashcode");
        }

        String hc = null;
        try {
            hc = String.valueOf(generateShortHashCode(input));
        } catch (Exception e) {
            logger.error("Error while generating hashcode with input {}", input);
        }
        return hc;
    }

    /**
     * Generate short hash code.
     *
     * @param serviceName the service name
     * @return the short
     */
    private short generateShortHashCode(String serviceName) {
        int serviceHc = serviceName.hashCode();

        // to convert int to short
        // first XOR high 16 bits with the low 16 bits (helps in spreading
        // entropy), and
        // & with Short.MAX_VALUE for getting positive value
        short shortHashCode = (short) ((serviceHc ^ (serviceHc >>> Constants.INT_16)) & Short.MAX_VALUE);
        logger.debug("For ServiceName {}, hash code is {}, short hashcode is {} ", serviceName, serviceHc,
                shortHashCode);
        return shortHashCode;
    }

}
