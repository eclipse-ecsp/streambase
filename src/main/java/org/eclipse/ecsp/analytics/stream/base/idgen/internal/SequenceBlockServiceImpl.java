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

import com.mongodb.ReadPreference;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.nosqldao.IgniteCriteria;
import org.eclipse.ecsp.nosqldao.IgniteCriteriaGroup;
import org.eclipse.ecsp.nosqldao.IgniteQuery;
import org.eclipse.ecsp.nosqldao.Operator;
import org.eclipse.ecsp.nosqldao.Updates;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * SequenceBlockService used to generate messageId block Implementation.
 */
@Service
public class SequenceBlockServiceImpl implements SequenceBlockService {

    /** The block value. */
    @Value("${" + PropertyNames.SEQUENCE_BLOCK_MAXVALUE + ":10000}")
    private int blockValue;

    /** The logger. */
    private IgniteLogger logger = IgniteLoggerFactory.getLogger(SequenceBlockServiceImpl.class);

    /** The sequence block dao. */
    @Autowired
    private SequenceBlockDAO sequenceBlockDao;

    /**
     * getMessageIdConfig().
     *
     * @param vehicleId vehicleId
     * @return SequenceBlock
     */
    public SequenceBlock getMessageIdConfig(String vehicleId) {
        SequenceBlock sb = findSequenceBlockById(vehicleId);
        logger.info("SequenceBlock fetch from MongoDB for vehicleId {} is {}", vehicleId, sb);
        if (null != sb) {
            if ((Integer.MAX_VALUE - (sb.getMaxValue() + blockValue)) <= 0) {
                resetCollection(vehicleId);
                return insertSequenceBlock(vehicleId);
            }
            long currSeqBlockTimeStamp = sb.getTimeStamp();
            int updatedMaxValue = sb.getMaxValue() + blockValue;
            sb.setCurrentValue(sb.getMaxValue());
            sb.setMaxValue(updatedMaxValue);
            sb.setTimeStamp(System.currentTimeMillis());
            boolean isSequenceBlockUpdated = updateSequenceBlock(sb, currSeqBlockTimeStamp);
            if (isSequenceBlockUpdated) {
                logger.info("Updated and returning sequenceBlock for vehicleId {},  is {}", vehicleId, sb);
            } else {
                // This is to handle the case where document was updated by 
                // another thread, and the current thread needs to retry.
                logger.info("No sequence block found for vehicleID {}, with "
                                + "maxValue < {}. Attempting again with increased max value.",
                        vehicleId, updatedMaxValue);
                return null;
            }
        } else {
            sb = insertSequenceBlock(vehicleId);
            logger.info("Saved and returning sequenceBlock for vehicleId {},  is {}", vehicleId, sb);
        }
        return sb;
    }

    /**
     * Insert sequence block.
     *
     * @param vehicleId the vehicle id
     * @return the sequence block
     */
    private SequenceBlock insertSequenceBlock(String vehicleId) {
        SequenceBlock sequenceBlock = createSequenceBlock(vehicleId, blockValue);
        return sequenceBlockDao.save(sequenceBlock);
    }

    /**
     * Find sequence block by id.
     *
     * @param vehicleId the vehicle id
     * @return the sequence block
     */
    private SequenceBlock findSequenceBlockById(String vehicleId) {
        IgniteCriteria filterByIdCriteria = new IgniteCriteria("_id", Operator.EQ, vehicleId);
        IgniteCriteriaGroup igniteCriteriaGrp = new IgniteCriteriaGroup(filterByIdCriteria);
        IgniteQuery igniteQuery = new IgniteQuery(igniteCriteriaGrp);
        igniteQuery.setReadPreference(ReadPreference.primaryPreferred());
        List<SequenceBlock> seqBlockList = sequenceBlockDao.find(igniteQuery);
        if (!CollectionUtils.isEmpty(seqBlockList)) {
            return seqBlockList.get(0);
        }
        return null;
    }

    /**
     * Update sequence block.
     *
     * @param sequenceBlock the sequence block
     * @param currSeqBlockTimeStamp the curr seq block time stamp
     * @return true, if successful
     */
    private boolean updateSequenceBlock(SequenceBlock sequenceBlock, long currSeqBlockTimeStamp) {
        String vehicleId = sequenceBlock.getVehicleId();
        IgniteCriteria filterByIdCriteria = new IgniteCriteria("_id", Operator.EQ, vehicleId);
        IgniteCriteria equalTSCriteria = new IgniteCriteria(IdGenConstants
                .SEQUENCE_BLOCK_TIMESTAMP_FIELD_NAME, Operator.EQ,
                currSeqBlockTimeStamp);
        IgniteCriteria lessThanMaxValueCriteria = new IgniteCriteria(IdGenConstants
                .SEQUENCE_BLOCK_MAXVALUE_FIELD_NAME, Operator.LT,
                sequenceBlock.getMaxValue());
        IgniteCriteriaGroup igniteCriteriaGrp = new IgniteCriteriaGroup(filterByIdCriteria)
                .and(equalTSCriteria)
                .and(lessThanMaxValueCriteria);
        logger.debug("Attempting to update sequenceBlock for vehicleId {}"
                + " with IgniteQueryGroup {}", vehicleId, igniteCriteriaGrp);

        Updates update = new Updates();
        update.addFieldSet(IdGenConstants.SEQUENCE_BLOCK_MAXVALUE_FIELD_NAME, sequenceBlock.getMaxValue());
        update.addFieldSet(IdGenConstants.SEQUENCE_BLOCK_CURRVALUE_FIELD_NAME, sequenceBlock.getCurrentValue());
        update.addFieldSet(IdGenConstants.SEQUENCE_BLOCK_TIMESTAMP_FIELD_NAME, sequenceBlock.getTimeStamp());
        IgniteQuery igniteQuery = new IgniteQuery(igniteCriteriaGrp);
        return sequenceBlockDao.update(igniteQuery, update);
    }

    /**
     * Creates the sequence block.
     *
     * @param vehicleId the vehicle id
     * @param maxValue the max value
     * @return the sequence block
     */
    private SequenceBlock createSequenceBlock(String vehicleId, int maxValue) {
        SequenceBlock seqBlock = new SequenceBlock();
        seqBlock.setVehicleId(vehicleId);
        seqBlock.setTimeStamp(System.currentTimeMillis());
        seqBlock.setMaxValue(maxValue);
        seqBlock.setCurrentValue(0);
        return seqBlock;
    }

    /**
     * Reset collection.
     *
     * @param vehicleId the vehicle id
     */
    private void resetCollection(String vehicleId) {
        logger.info("Resetting sequenceBlock as max value for vehicleId {} "
                + "has crossed {}", vehicleId, Integer.MAX_VALUE);
        sequenceBlockDao.deleteById(vehicleId);
    }

}
