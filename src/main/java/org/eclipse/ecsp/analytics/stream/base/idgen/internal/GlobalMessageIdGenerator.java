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

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidSequenceBlockException;
import org.eclipse.ecsp.analytics.stream.base.idgen.MessageIdGenerator;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * GlobalMessageGenerator is responsible for generating message unique ID
 * and store in memory to serve caching using LRU algorithm and this
 * is not thread safe.
 * This class has a dependency on Mongo so need to give
 * permission to access the mongo collection "sequenceBlock" (for all CRUD
 * operations).
 *
 * @author Binoy Mandal
 */
@Component
@Scope("prototype")
public class GlobalMessageIdGenerator implements MessageIdGenerator {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(GlobalMessageIdGenerator.class);

    /** The threshold. */
    @Value("${lru.map.threshold:100000}")
    private int threshold;

    /** The block value. */
    @Value("${" + PropertyNames.SEQUENCE_BLOCK_MAXVALUE + ":10000}")
    private int blockValue;

    /** The retry counter. */
    @Value("${message.generation.retry.counter:5}")
    private byte retryCounter;

    /** The retry interval. */
    @Value("${message.generation.retry.interval:500}")
    private long retryInterval;

    /** The vehicle id counter map. */
    private Map<String, SequenceBlock> vehicleIdCounterMap;

    /** The sequence block service. */
    @Autowired
    private SequenceBlockService sequenceBlockService;

    /**
     * init().
     */
    @PostConstruct
    public void init() {
        logger.info("Initializing vehicleID counter map with "
                + "capacity threshold of : {}", threshold);
        vehicleIdCounterMap = Collections.synchronizedMap(
                new LinkedHashMap<String, SequenceBlock>(threshold, Constants.FLOAT_DECIMAL75, true) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean removeEldestEntry(Map.Entry<String, SequenceBlock> eldest) {
                        return size() > threshold;
                    }
                });
    }

    /**
     * Get messageId for provided vechileId. If vehicleId is present in cache
     * then generate message id using cached information else get
     * data from MongoDB for provided vehicleId and generate.
     *
     * @param vehicleId
     *         for which message id need to be generated.
     * @return String
     */
    @Override
    public String generateUniqueMsgId(String vehicleId) {
        if (StringUtils.isEmpty(vehicleId)) {
            logger.error("SequenceBlock can't be fetched as input value passed is empty or null");
            throw new InvalidSequenceBlockException("SequenceBlock can't be fetched as input value for"
                    + " vehicleID is passed as empty or null");
        }
        SequenceBlock sequenceBlock = vehicleIdCounterMap.get(vehicleId);
        if (Objects.isNull(sequenceBlock) || (sequenceBlock.getCurrentValue() >= sequenceBlock.getMaxValue())) {
            sequenceBlock = getLastUpdatedValue(vehicleId);
        }
        int nextId = sequenceBlock.getCurrentValue() + 1;
        sequenceBlock.setCurrentValue(nextId);
        vehicleIdCounterMap.put(vehicleId, sequenceBlock);
        logger.info("MessageId generated for vehicleId {} is {}", vehicleId, nextId);
        return Integer.toString(nextId);
    }

    /**
     * Gets the last updated value.
     *
     * @param vehicleId the vehicle id
     * @return the last updated value
     */
    private SequenceBlock getLastUpdatedValue(String vehicleId) {
        SequenceBlock sequenceBlock;
        byte currentRetry = 0;
        while ((sequenceBlock = sequenceBlockService.getMessageIdConfig(vehicleId)) == null
                && currentRetry++ < retryCounter) {
            logger.debug("Sleeping for {}, unable to fetch sequenceblock from dao " + "for vehicleId {}, "
                    + "in attempt number : {}", retryInterval, vehicleId, currentRetry);
            sleep(retryInterval);
        }
        if (Objects.nonNull(sequenceBlock)) {
            logger.debug("Fetched updated max value from mongo for vehicleId : {}, as : {}, and current value as {}",
                    vehicleId, sequenceBlock.getMaxValue(), sequenceBlock.getCurrentValue());
            return sequenceBlock;
        } else {
            logger.error("Unable to fetch updated max value from mongo for "
                    + "vehicleId: {}. Max retries exhausted.", vehicleId);
            throw new InvalidSequenceBlockException("Unable to fetch updated max value from mongo "
                    + "for vehicleId: " + vehicleId);
        }
    }
    
    /**
     * Sleep.
     *
     * @param val the val
     */
    private void sleep(long val) {
        try {
            TimeUnit.MILLISECONDS.sleep(val);
        } catch (InterruptedException e) {
            logger.error("Thread: {} interrupted while retrying for getting data from mongodb",
                    Thread.currentThread().getName());
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Gets the block value.
     *
     * @return the block value
     */
    // Getter used for unit test cases
    int getBlockValue() {
        return blockValue;
    }

}
