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

package org.eclipse.ecsp.stream.dma.handler;

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.stream.dma.config.DMAConfigResolver;
import org.springframework.stereotype.Component;



/**
 * {@link DMAConfigResolverTestImpl} implements {@link DMAConfigResolver}.
 */
@Component
public class DMAConfigResolverTestImpl implements DMAConfigResolver {
    
    /**
     * Gets the retry interval.
     *
     * @param event the event
     * @return the retry interval
     */
    @Override
    public long getRetryInterval(IgniteEvent event) {
        if (event.getEcuType() == null) {
            return Constants.THREAD_SLEEP_TIME_5000;
        }
        if (event.getEcuType().equalsIgnoreCase("TELEMATICS")) {
            return Constants.THREAD_SLEEP_TIME_4000;
        } else if (event.getEcuType().equalsIgnoreCase("ecu1")) {
            return Constants.THREAD_SLEEP_TIME_3000;
        }
        return 0;
    }
}
