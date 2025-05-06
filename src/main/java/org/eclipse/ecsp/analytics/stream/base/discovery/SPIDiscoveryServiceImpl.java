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

package org.eclipse.ecsp.analytics.stream.base.discovery;

import org.eclipse.ecsp.analytics.stream.base.StreamProcessor;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Discovers stream processors by inspecting Java SPI metadata.
 *
 * @author ssasidharan
 * @param <KIn> the generic type
 * @param <VIn> the generic type
 * @param <KOut> the generic type
 * @param <VOut> the generic type
 */
public class SPIDiscoveryServiceImpl<KIn, VIn, KOut, VOut> 
    implements StreamProcessorDiscoveryService<KIn, VIn, KOut, VOut> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(SPIDiscoveryServiceImpl.class);

    /**
     * Discover processors.
     *
     * @return the list
     */
    @Override
    @SuppressWarnings("rawtypes")
    public List<StreamProcessor<KIn, VIn, KOut, VOut>> discoverProcessors() {
        ServiceLoader<StreamProcessor> ldr = ServiceLoader.load(StreamProcessor.class);
        Iterator<StreamProcessor> procs = ldr.iterator();
        List<StreamProcessor<KIn, VIn, KOut, VOut>> procList = new ArrayList<>();
        while (procs.hasNext()) {
            StreamProcessor<KIn, VIn, KOut, VOut> proc = procs.next();
            logger.info("Discovered processor: " + proc.getClass().getName());
            procList.add(proc);
        }
        return procList;
    }

}
