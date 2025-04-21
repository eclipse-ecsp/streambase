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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;



/**
 * class {@link GlobalMessageGeneratorTest}.
 */
public class GlobalMessageGeneratorTest {

    /** The global message id generator. */
    @InjectMocks
    private GlobalMessageIdGenerator globalMessageIdGenerator;

    /** The message id config service. */
    @Mock
    private SequenceBlockService messageIdConfigService;

    /**
     * setUp().
     *
     * @throws NoSuchFieldException the no such field exception
     * @throws SecurityException the security exception
     * @throws IllegalArgumentException the illegal argument exception
     * @throws IllegalAccessException IllegalAccessException
     * @throwIllegalAccessException NoSuchFieldException
     * @thIllegalAccessException SecurityException
     */
    @Before
    public void setup() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        MockitoAnnotations.initMocks(this);
        globalMessageIdGenerator.init();
        Field retryCounterFiled = globalMessageIdGenerator.getClass().getDeclaredField("retryCounter");
        retryCounterFiled.setAccessible(true);
        retryCounterFiled.set(globalMessageIdGenerator, (byte) Constants.THREE);

        retryCounterFiled = globalMessageIdGenerator.getClass().getDeclaredField("retryInterval");
        retryCounterFiled.setAccessible(true);
        retryCounterFiled.set(globalMessageIdGenerator, Constants.THREAD_SLEEP_TIME_1000);
    }

    /**
     * Retry unit test while DAO layer does not return any value.
     *
     * @throws Exception Exception
     */
    @Test(expected = RuntimeException.class)
    public void testGenerateUniqueMsgId() throws Exception {
        Mockito.when(messageIdConfigService.getMessageIdConfig(Mockito.anyString()))
                .thenReturn(null);
        globalMessageIdGenerator.generateUniqueMsgId("testid");
    }

    /**
     * Test generate unique msg id when vehicle id null.
     *
     * @throws Exception the exception
     */
    @Test(expected = RuntimeException.class)
    public void testGenerateUniqueMsgIdWhenVehicleIdNull() throws Exception {
        Mockito.when(messageIdConfigService.getMessageIdConfig(Mockito.anyString()))
                .thenReturn(null);
        globalMessageIdGenerator.generateUniqueMsgId(null);
    }

}
