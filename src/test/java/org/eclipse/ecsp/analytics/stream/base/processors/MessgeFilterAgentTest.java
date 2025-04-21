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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.cache.GetStringRequest;
import org.eclipse.ecsp.cache.IgniteCache;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;


/**
 * UT class {@link MessgeFilterAgentTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { MessageBaseFilterImpl.class })
public class MessgeFilterAgentTest {

    /** The messge filter agent test. */
    @Spy
    private MessgeFilterAgent messgeFilterAgentTest;

    /** The message filter. */
    @Autowired
    private MessageFilter messageFilter;

    /** The cache. */
    @Mock
    private IgniteCache cache;

    /** The ignite event. */
    private IgniteEvent igniteEvent;

    /** The ignite string key. */
    private IgniteKey<String> igniteStringKey;

    /**
     * setup().
     *
     * @throws NoSuchFieldException NoSuchFieldException
     * @throws SecurityException SecurityException
     */
    @Before
    public void setup() throws NoSuchFieldException, SecurityException {
        MockitoAnnotations.initMocks(this);
        igniteStringKey = new IgniteStringKey("test123");
        igniteEvent = new IgniteEventImpl();
        ReflectionTestUtils.setField(messgeFilterAgentTest, "messageFilter", messageFilter);
        ReflectionTestUtils.setField(messgeFilterAgentTest, "cache", cache);
        ReflectionTestUtils.setField(messgeFilterAgentTest, "serviceName", "serviceName");
        Mockito.when(cache.getString(any(GetStringRequest.class)))
                .thenReturn(String.valueOf(Constants.LONG_1603946935));
    }

    /**
     * Test is duplicate.
     */
    @Test
    public void testIsDuplicate() {
        Assert.assertTrue(messgeFilterAgentTest.isDuplicate(igniteStringKey, igniteEvent));
        igniteStringKey = new IgniteStringKey("test1234");
        Mockito.when(cache.getString(any(GetStringRequest.class))).thenReturn(null);
        Assert.assertFalse(messgeFilterAgentTest.isDuplicate(igniteStringKey, igniteEvent));

    }

}
