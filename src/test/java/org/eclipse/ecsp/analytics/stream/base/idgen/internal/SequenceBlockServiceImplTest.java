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
import org.eclipse.ecsp.nosqldao.IgniteQuery;
import org.eclipse.ecsp.nosqldao.Updates;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * UT class {@link SequenceBlockServiceImplTest}.
 */
public class SequenceBlockServiceImplTest {

    /** The Constant VEHICLE_ID. */
    private static final String VEHICLE_ID = "Vehicle999";
    
    /** The seq block service. */
    @InjectMocks
    private SequenceBlockServiceImpl seqBlockService;
    
    /** The sequence block DAO. */
    @Mock
    private SequenceBlockDAO sequenceBlockDAO;
    
    /** The block value. */
    private int blockValue = Constants.THREAD_SLEEP_TIME_1000;

    /**
     * setup().
     *
     * @throws NoSuchFieldException NoSuchFieldException
     * @throws SecurityException SecurityException
     * @throws IllegalArgumentException IllegalArgumentException
     * @throws IllegalAccessException IllegalAccessException
     */
    @Before
    public void setup() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        MockitoAnnotations.initMocks(this);

        Field blockValueField = seqBlockService.getClass().getDeclaredField("blockValue");
        blockValueField.setAccessible(true);
        blockValueField.set(seqBlockService, blockValue);
    }

    /**
     * Test generate unique msg id with insert.
     */
    @Test
    public void testGenerateUniqueMsgIdWithInsert() {

        Mockito.when(sequenceBlockDAO.find(ArgumentMatchers.any(IgniteQuery.class))).thenReturn(null);
        SequenceBlock seqBlock = new SequenceBlock();
        seqBlock.setVehicleId(VEHICLE_ID);
        seqBlock.setTimeStamp(System.currentTimeMillis());
        seqBlock.setMaxValue(blockValue);
        seqBlock.setCurrentValue(0);
        Mockito.when(sequenceBlockDAO.save(ArgumentMatchers.any(SequenceBlock.class))).thenReturn(seqBlock);

        SequenceBlock updatedSeqBlock = seqBlockService.getMessageIdConfig(VEHICLE_ID);

        assertEquals("Max value not saved as expected",
                seqBlock.getMaxValue(), updatedSeqBlock.getMaxValue());
        assertEquals("Current value not saved as expected",
                seqBlock.getCurrentValue(), updatedSeqBlock.getCurrentValue());
    }

    /**
     * Test generate unique msg id with update pass.
     */
    @Test
    public void testGenerateUniqueMsgIdWithUpdatePass() {

        int oldMaxValue = Constants.THREAD_SLEEP_TIME_4000;
        SequenceBlock seqBlock = new SequenceBlock();
        seqBlock.setCurrentValue(oldMaxValue - blockValue);
        seqBlock.setMaxValue(oldMaxValue);
        seqBlock.setTimeStamp(System.currentTimeMillis() - (Constants.SIXTY * Constants.THREAD_SLEEP_TIME_1000));
        seqBlock.setVehicleId(VEHICLE_ID);
        List<SequenceBlock> seqBlockList = new ArrayList<>();
        seqBlockList.add(seqBlock);

        Mockito.when(sequenceBlockDAO.find(ArgumentMatchers
                .any(IgniteQuery.class))).thenReturn(seqBlockList);
        Mockito.when(sequenceBlockDAO.update(ArgumentMatchers
                        .any(IgniteQuery.class), ArgumentMatchers.any(Updates.class)))
                .thenReturn(true);

        SequenceBlock updatedSeqBlock = seqBlockService.getMessageIdConfig(VEHICLE_ID);

        Mockito.verify(sequenceBlockDAO, Mockito.times(1))
                .update(ArgumentMatchers.any(IgniteQuery.class), ArgumentMatchers.any(Updates.class));

        assertEquals("Max value not updated as expected", oldMaxValue + blockValue, updatedSeqBlock.getMaxValue());
        assertEquals("Current value not updated as expected", oldMaxValue, updatedSeqBlock.getCurrentValue());
    }

    /**
     * Test generate unique msg id with update fail.
     */
    @Test
    public void testGenerateUniqueMsgIdWithUpdateFail() {

        int oldMaxValue = Constants.THREAD_SLEEP_TIME_4000;
        SequenceBlock seqBlock = new SequenceBlock();
        seqBlock.setCurrentValue(oldMaxValue - blockValue);
        seqBlock.setMaxValue(oldMaxValue);
        seqBlock.setTimeStamp(System.currentTimeMillis() - (Constants.SIXTY * Constants.THREAD_SLEEP_TIME_1000));
        seqBlock.setVehicleId(VEHICLE_ID);
        List<SequenceBlock> seqBlockList = new ArrayList<>();
        seqBlockList.add(seqBlock);

        Mockito.when(sequenceBlockDAO.find(ArgumentMatchers.any(IgniteQuery.class)))
                .thenReturn(seqBlockList);
        Mockito.when(sequenceBlockDAO.update(ArgumentMatchers.any(IgniteQuery.class),
                        ArgumentMatchers.any(Updates.class)))
                .thenReturn(false);

        SequenceBlock updatedSeqBlock = seqBlockService.getMessageIdConfig(VEHICLE_ID);

        Mockito.verify(sequenceBlockDAO, Mockito.times(1))
                .update(ArgumentMatchers.any(IgniteQuery.class), ArgumentMatchers.any(Updates.class));
        assertEquals("Sequence Block got updated", null, updatedSeqBlock);
    }

}
