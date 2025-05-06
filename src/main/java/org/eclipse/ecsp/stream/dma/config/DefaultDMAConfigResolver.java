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

package org.eclipse.ecsp.stream.dma.config;

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


/**
 * Default implementation of DefaultDMAConfigResolver. By default dma.service.retry.interval.millis set in stream-base's
 *         properties file will be returned i.e. 60000.
 *
 * @author poorvi
 */
@Component
public class DefaultDMAConfigResolver implements DMAConfigResolver {

    /** The retry interval. */
    @Value("${" + PropertyNames.DMA_SERVICE_RETRY_INTERVAL_MILLIS + ":60000}")
    private long retryInterval;

    /**
     * Gets the retry interval.
     *
     * @param event the event
     * @return the retry interval
     */
    @Override
    public long getRetryInterval(IgniteEvent event) {
        return retryInterval;
    }
}
