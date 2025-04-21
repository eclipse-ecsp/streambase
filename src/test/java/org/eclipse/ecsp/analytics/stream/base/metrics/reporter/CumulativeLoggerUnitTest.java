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

package org.eclipse.ecsp.analytics.stream.base.metrics.reporter;

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.metrics.reporter.CumulativeLogger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;


/**
 * test class for {@link CumulativeLogger}.
 */
public class CumulativeLoggerUnitTest {

    /** The Constant COUNTER_1. */
    private static final String COUNTER_1 = "counter1";
    
    /** The prop. */
    Properties prop;
    
    /** The cumulative logger. */
    private CumulativeLogger cumulativeLogger;

    /**
     * setup method for initializing properties.
     */
    @Before
    public void setup() {
        prop = new Properties();
        cumulativeLogger = CumulativeLogger.getLogger();
        ReflectionTestUtils.setField(cumulativeLogger, "logEveryXMinute", 1);
        prop.put(PropertyNames.LOG_COUNTS_MINUTES, "1");
    }

    /**
     * Test increment.
     *
     * @throws InterruptedException the interrupted exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testIncrement() throws InterruptedException {
        CumulativeLogger.init(prop);
        int count = 0;
        int incrementBy = 1;
        cumulativeLogger.incrementBy(COUNTER_1, incrementBy);
        count += incrementBy;
    
        Map<String, AtomicLong> stateMap = (Map<String, AtomicLong>) ReflectionTestUtils.getField(cumulativeLogger, 
                "state");
        Assert.assertNotNull(stateMap);
        Assert.assertEquals(count, stateMap.get(COUNTER_1).get());

        cumulativeLogger.incrementByOne(COUNTER_1);
        count += 1;
        stateMap = (Map<String, AtomicLong>) ReflectionTestUtils.getField(cumulativeLogger, "state");
        Assert.assertNotNull(stateMap);
        Assert.assertEquals(count, stateMap.get(COUNTER_1).get());
    }

    /**
     * Test incorrect log duration value.
     *
     * @throws IllegalArgumentException the illegal argument exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testIncorrectLogDurationValue() throws IllegalArgumentException {

        prop.put(PropertyNames.LOG_COUNTS_MINUTES, "0");
        CumulativeLogger.init(prop);
    }
}
