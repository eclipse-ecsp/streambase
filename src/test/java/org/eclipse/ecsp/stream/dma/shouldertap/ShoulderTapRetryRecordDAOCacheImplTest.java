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

package org.eclipse.ecsp.stream.dma.shouldertap;

import org.eclipse.ecsp.analytics.stream.base.stores.CacheBypass;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.stream.dma.dao.ShoulderTapRetryRecordDAOCacheImpl;
import org.eclipse.ecsp.utils.metrics.InternalCacheGuage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;



/**
 * {@link ShoulderTapRetryRecordDAOCacheImplTest} test class for {@link ShoulderTapRetryRecordDAOCacheImpl}.
 */
public class ShoulderTapRetryRecordDAOCacheImplTest {

    /** The cache. */
    @InjectMocks
    private ShoulderTapRetryRecordDAOCacheImpl cache = new ShoulderTapRetryRecordDAOCacheImpl();

    /** The bypass. */
    @Mock
    private CacheBypass bypass;

    /** The ignite cache. */
    @Mock
    private IgniteCache igniteCache;

    /** The guage. */
    @Mock
    private InternalCacheGuage guage;

    /**
     * Test initialize.
     */
    @Test
    public void testInitialize() {
        MockitoAnnotations.openMocks(this);
        cache.setServiceName("ecall");
        cache.initialize("task_Id");
        Assert.assertEquals("ecall", (String) ReflectionTestUtils.getField(cache, "serviceName"));
    }
}
