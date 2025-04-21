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
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;


/**
 * {@link SequenceBlockServiceImplIntegrationTest} test class for {@link SequenceBlockServiceImpl}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@TestPropertySource("/stream-base-test.properties")
public class SequenceBlockServiceImplIntegrationTest {

    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The Constant REDIS. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS = new EmbeddedRedisServer();
    
    /** The Constant VEHICLE_ID. */
    private static final String VEHICLE_ID = "Vehicle123";
    
    /** The sequence block service. */
    @Autowired
    private SequenceBlockService sequenceBlockService;
    
    /** The sequence block dao. */
    @Autowired
    private SequenceBlockDAO sequenceBlockDao;
    
    /** The block value. */
    @Value("${" + PropertyNames.SEQUENCE_BLOCK_MAXVALUE + ":1000}")
    private int blockValue;

    /**
     * Test seq block service save and update.
     */
    @Test
    public void testSeqBlockServiceSaveAndUpdate() {

        sequenceBlockDao.deleteAll();
        String vehicleId = VEHICLE_ID;
        SequenceBlock seqBlock = sequenceBlockService.getMessageIdConfig(vehicleId);
        assertEquals("The same vehicle id is not retrived ", true, VEHICLE_ID.equals(seqBlock.getVehicleId()));
        assertEquals("sequence block's max value not updated correctly",
                true, (blockValue == seqBlock.getMaxValue()));

        seqBlock = sequenceBlockService.getMessageIdConfig(vehicleId);
        assertEquals("sequence block's max value not updated correctly",
                true, (blockValue * Constants.TWO == seqBlock.getMaxValue()));

        seqBlock = sequenceBlockService.getMessageIdConfig(vehicleId);
        assertEquals("Entity not updated", true, seqBlock.getVehicleId().equals(vehicleId));
        assertEquals("Expected Current value not equal to current "
                        + "value of seq block", seqBlock.getMaxValue() - blockValue,
                seqBlock.getCurrentValue());
    }

    /**
     * Test seq block service max value reached.
     */
    @Test
    public void testSeqBlockServiceMaxValueReached() {

        String vehicleId = VEHICLE_ID;
        SequenceBlock seqBlock = new SequenceBlock();
        seqBlock.setMaxValue(Integer.MAX_VALUE);
        seqBlock.setTimeStamp(System.currentTimeMillis());
        seqBlock.setVehicleId(vehicleId);
        sequenceBlockDao.save(seqBlock);

        seqBlock = sequenceBlockService.getMessageIdConfig(vehicleId);
        assertEquals("Collection not reset when max value for Integer reached", blockValue, seqBlock.getMaxValue());
    }

}