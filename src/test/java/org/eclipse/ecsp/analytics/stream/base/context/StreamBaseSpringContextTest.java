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

package org.eclipse.ecsp.analytics.stream.base.context;

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.context.StreamBaseSpringContext;
import org.eclipse.ecsp.analytics.stream.base.metrics.reporter.HarmanRocksDBMetricsExporter;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;


/**
 * class {@link StreamBaseSpringContextTest} extends {@link KafkaStreamsApplicationTestBase}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@TestPropertySource("/stream-base-test.properties")
public class StreamBaseSpringContextTest extends KafkaStreamsApplicationTestBase {

    /** The spring ctx. */
    @Autowired
    private StreamBaseSpringContext springCtx;

    /**
     * Setup.
     *
     * @throws Exception the exception
     */
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    /**
     * Test get bean.
     */
    @Test
    public void testGetBean() {
        HarmanRocksDBMetricsExporter exporter = StreamBaseSpringContext.getBean(HarmanRocksDBMetricsExporter.class);
        Assert.assertNotNull(exporter);
    }

    /**
     * Test set application context.
     */
    @Test
    public void testSetApplicationContext() {
        ApplicationContext ctx = (ApplicationContext) ReflectionTestUtils.getField(springCtx, "context");
        Assert.assertNotNull(ctx);
    }
}
