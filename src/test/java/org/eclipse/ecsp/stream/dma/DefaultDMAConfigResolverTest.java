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

package org.eclipse.ecsp.stream.dma;

import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.stream.dma.config.DefaultDMAConfigResolver;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.springframework.test.context.TestPropertySource;



/**
 * DefaultDMAConfigResolverTest UT class for {@link DefaultDMAConfigResolver}.
 */
@TestPropertySource("/dma-connectionstatus-handler-test.properties")
public class DefaultDMAConfigResolverTest {
    
    /** The default DMA config resolver. */
    @InjectMocks
    DefaultDMAConfigResolver defaultDMAConfigResolver = new DefaultDMAConfigResolver();

    /**
     * Test get retry interval.
     */
    @Test
    public void testGetRetryInterval() {
        IgniteEventImpl event = new IgniteEventImpl();
        event.setEventId("testId");
        long retryInt = defaultDMAConfigResolver.getRetryInterval(event);
        Assert.assertEquals(0, retryInt);
    }
}
