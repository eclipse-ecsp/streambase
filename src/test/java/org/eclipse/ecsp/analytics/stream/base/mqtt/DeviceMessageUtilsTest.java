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

package org.eclipse.ecsp.analytics.stream.base.mqtt;

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.GlobalMessageIdGenerator;
import org.eclipse.ecsp.domain.DeviceMessageFailureEventDataV1_0;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageUtils;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;


/**
 * {@link DeviceMessageUtils} test class for {@link DeviceMessageUtilsTest}.
 */
public class DeviceMessageUtilsTest {
    
    /** The device message utils. */
    @InjectMocks
    DeviceMessageUtils deviceMessageUtils = new DeviceMessageUtils();

    /** The global message id generator. */
    @Mock
    GlobalMessageIdGenerator globalMessageIdGenerator = Mockito.mock(GlobalMessageIdGenerator.class);

    /** The spc. */
    @Mock
    private StreamProcessingContext spc = Mockito.mock(StreamProcessingContext.class);

    /** The key. */
    @Mock
    private IgniteKey key = Mockito.mock(IgniteKey.class);

    /**
     * Testpost failure event.
     */
    @Test
    public void testpostFailureEvent() {
        ReflectionTestUtils.setField(deviceMessageUtils, "msgIdGenerator", globalMessageIdGenerator);
        IgniteEventImpl igniteEventImpl = new IgniteEventImpl();
        igniteEventImpl.setEventId("123");
        igniteEventImpl.setBizTransactionId("vjhsv");
        igniteEventImpl.setVehicleId("vehicle12");
        igniteEventImpl.setTimezone((short) TestConstants.INT_1343678902);
        DeviceMessageFailureEventDataV1_0 messageFailureEventDataV10 = new DeviceMessageFailureEventDataV1_0();
        messageFailureEventDataV10.setDeviceDeliveryCutoffExceeded(true);
        messageFailureEventDataV10.setFailedIgniteEvent(igniteEventImpl);

        Mockito.when(globalMessageIdGenerator.generateUniqueMsgId(
                Mockito.any())).thenReturn("123");
        String feedbackTopic = "feedbackTopic";
        deviceMessageUtils.postFailureEvent(messageFailureEventDataV10,
                key, spc, feedbackTopic);
        Mockito.verify(spc, Mockito.times(1))
                .forwardDirectly((IgniteKey) any(), (IgniteEventImpl) any(), Mockito.anyString());
    }
}
