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


import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;



/**
 * class GlobalMessageGeneratorIntegrationTest extends {@link KafkaStreamsApplicationTestBase}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@TestPropertySource("/messageid-generator-test.properties")
public class GlobalMessageGeneratorIntegrationTest extends KafkaStreamsApplicationTestBase {

    /** The global message id generator. */
    @Autowired
    private GlobalMessageIdGenerator globalMessageIdGenerator;

    /** The sequence block DAO. */
    @Autowired
    private SequenceBlockDAO sequenceBlockDAO;

    /**
     * Test generate unique msg id.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGenerateUniqueMsgId() throws Exception {
        String vechileId = "vechileid1";
        String messageId = globalMessageIdGenerator.generateUniqueMsgId(vechileId);
        List<SequenceBlock> sequenceBlockList = sequenceBlockDAO.findByIds(vechileId);
        assertEquals(Integer.valueOf(messageId),
                Integer.valueOf(sequenceBlockList.get(0).getMaxValue() - globalMessageIdGenerator.getBlockValue() + 1));
    }

    /**
     * Map entry must be deleted which is least recently used, after some specified threshold value reached.
     */
    @Test
    public void testGenerateUniqueMsgIdForEviction() {
        List<String> list1 = new ArrayList<>();
        int i = 0;
        while (i++ < Constants.THREAD_SLEEP_TIME_100) {
            list1.add(globalMessageIdGenerator.generateUniqueMsgId(Integer.toString(i)));
        }
        @SuppressWarnings("unchecked")
        Map<String, SequenceBlock> vehicleIdCounterMap = (Map<String, SequenceBlock>)
                ReflectionTestUtils.getField(globalMessageIdGenerator,
                "vehicleIdCounterMap");
        assertNull(vehicleIdCounterMap.get("0"));
        assertNotNull(vehicleIdCounterMap.get("97"));
        assertNotNull(vehicleIdCounterMap.get("98"));
        assertNotNull(vehicleIdCounterMap.get("99"));
        assertNotNull(vehicleIdCounterMap.get("100"));
    }

    /**
     * Test generate unique msg id when vehicle id null.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGenerateUniqueMsgIdWhenVehicleIdNull() throws Exception {
        assertThrows(RuntimeException.class, () -> globalMessageIdGenerator.generateUniqueMsgId(null));
    }

}