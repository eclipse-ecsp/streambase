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

package org.eclipse.ecsp.stream.dma.dao;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.nosqldao.IgniteCriteria;
import org.eclipse.ecsp.nosqldao.IgniteCriteriaGroup;
import org.eclipse.ecsp.nosqldao.IgniteOrderBy;
import org.eclipse.ecsp.nosqldao.IgniteQuery;
import org.eclipse.ecsp.nosqldao.Operator;
import org.eclipse.ecsp.nosqldao.mongodb.IgniteBaseDAOMongoImpl;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.eclipse.ecsp.stream.dma.dao.DMAConstants.DMA_SHOULDER_TAP_ENABLED_MESSAGE_PRIORITY;
import static org.eclipse.ecsp.stream.dma.dao.DMAConstants.IS_TTL_NOTIF_PROCESSED_FIELD;
import static org.eclipse.ecsp.stream.dma.dao.DMAConstants.TTL_EXPRIATION_TIME_FIELD;


/**
 * {@link DMOfflineBufferEntryDAOMongoImpl} extends {@link IgniteBaseDAOMongoImpl}
 *         implements {@link DMOfflineBufferEntryDAO} .
 */
@Repository
@ConditionalOnProperty(name = PropertyNames.DMA_ENABLED, havingValue = "true")
public class DMOfflineBufferEntryDAOMongoImpl extends IgniteBaseDAOMongoImpl<String, DMOfflineBufferEntry>
        implements DMOfflineBufferEntryDAO {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DMOfflineBufferEntryDAOMongoImpl.class);

    /** The service name identifier. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceNameIdentifier;

    /** The collection. */
    private volatile String collection;

    /**
     * Gets the overriding collection name.
     *
     * @return the overriding collection name
     */
    @Override
    public String getOverridingCollectionName() {
        if (StringUtils.isEmpty(collection)) {
            collection = new StringBuilder().append("dmOfflineBufferEntries").append(serviceNameIdentifier).toString();
        }
        return collection;
    }

    /**
     * Adds the offline buffer entry.
     *
     * @param vehicleId the vehicle id
     * @param igniteKey the ignite key
     * @param entity the entity
     * @param subService the sub service
     */
    @Override
    public void addOfflineBufferEntry(String vehicleId, @SuppressWarnings("rawtypes") IgniteKey igniteKey,
            DeviceMessage entity, String subService) {
        logger.debug("Add buffer entry for vehicle id {} and service {}", vehicleId, serviceNameIdentifier);
        DeviceMessageHeader header = entity.getDeviceMessageHeader();
        LocalDateTime eventDate = LocalDateTime.ofEpochSecond(header.getTimestamp(), 0, ZoneOffset.UTC);
        DMOfflineBufferEntry offlineEntry = new DMOfflineBufferEntry();
        offlineEntry.setIgniteKey(igniteKey);
        offlineEntry.setVehicleId(vehicleId);
        offlineEntry.setEventTs(eventDate);
        offlineEntry.setEvent(entity);
        offlineEntry.setPendingRetries(entity.getDeviceMessageHeader().getPendingRetries());
        offlineEntry.setTtlExpirationTime(header.getDeviceDeliveryCutoff());
        offlineEntry.setTtlNotifProcessed(false);
        if (StringUtils.isNotEmpty(subService)) {
            offlineEntry.setSubService(subService);
        }
        String targetDeviceId = header.getTargetDeviceId();
        if (StringUtils.isNotEmpty(targetDeviceId)) {
            offlineEntry.setDeviceId(targetDeviceId);
        }
        if (header.isShoulderTapEnabled()) {
            offlineEntry.setPriority(DMA_SHOULDER_TAP_ENABLED_MESSAGE_PRIORITY);
        }

        /*
         * RTC-404497, if a service has sharded the dma offline buffer collection
         * the shard key information has to be sent to ignite dao for further use mandated from mongodb 4.4
         * */
        offlineEntry = save(offlineEntry);
        logger.debug("Saved ignite event {}", offlineEntry);
    }

    /**
     * Removes the offline buffer entry.
     *
     * @param offlineEntryId the offline entry id
     */
    @Override
    public void removeOfflineBufferEntry(String offlineEntryId) {
        deleteByIds(offlineEntryId);
        logger.debug("Removed offline entry with id {}", offlineEntryId);
    }

    /**
     * Gets the offline buffer entries sorted by priority.
     *
     * @param vehicleId the vehicle id
     * @param descSortOrder the desc sort order
     * @param deviceId the device id
     * @param subService the sub service
     * @return the offline buffer entries sorted by priority
     */
    @Override
    public List<DMOfflineBufferEntry> getOfflineBufferEntriesSortedByPriority(String vehicleId, boolean descSortOrder,
            Optional<String> deviceId, Optional<String> subService) {
        logger.debug("Get offline entries for vehicle id {} and service {}", vehicleId, serviceNameIdentifier);
        IgniteCriteria criteriaVehicleId = new IgniteCriteria("vehicleId", Operator.EQ, vehicleId);

        IgniteCriteriaGroup criteriaGroup = new IgniteCriteriaGroup(criteriaVehicleId);

        if (deviceId.isPresent()) {
            IgniteCriteria criteriaDeviceId = new IgniteCriteria("deviceId", Operator.EQ,
                    deviceId.get());
            criteriaGroup.and(criteriaDeviceId);
        }

        if (subService.isPresent()) {
            IgniteCriteria criteriaSubService = new IgniteCriteria("subService", Operator.EQ, subService.get());
            criteriaGroup.and(criteriaSubService);
        }

        IgniteQuery query = new IgniteQuery(criteriaGroup);

        IgniteOrderBy igniteOrderBy = new IgniteOrderBy().byfield("priority");

        if (descSortOrder) {
            igniteOrderBy.desc();
        }

        query.orderBy(igniteOrderBy);

        List<DMOfflineBufferEntry> offlineEntries = find(query);
        if (!offlineEntries.isEmpty()) {
            logger.debug("Got {} offline entries {}", offlineEntries.size(), offlineEntries);
            return offlineEntries;
        } else {
            logger.debug("No entries found for vehicle {} and service {}", vehicleId, serviceNameIdentifier);
            return Collections.emptyList();
        }
    }

    /**
     * Fetch the entry from offline buffer collection with TTL which is latest to expire.
     *
     * @return the offline buffer entry with earliest ttl
     */
    @Override
    public DMOfflineBufferEntry getOfflineBufferEntryWithEarliestTtl() {

        logger.debug("Get offline buffer entry with earliest TTL");
        IgniteCriteria ttlExpirationTimeExists = new IgniteCriteria(TTL_EXPRIATION_TIME_FIELD, Operator.GTE, 0);
        IgniteCriteria ttlNotifNotProcessed = new IgniteCriteria(IS_TTL_NOTIF_PROCESSED_FIELD, Operator.EQ, false);
        IgniteCriteriaGroup criteriaGroup = new IgniteCriteriaGroup(ttlExpirationTimeExists).and(ttlNotifNotProcessed);

        IgniteQuery query = new IgniteQuery(criteriaGroup);
        IgniteOrderBy igniteOrderBy = new IgniteOrderBy().byfield(TTL_EXPRIATION_TIME_FIELD);
        query.orderBy(igniteOrderBy);
        query.setPageNumber(DMAConstants.TOP_RECORD_NUMBER);
        query.setPageSize(DMAConstants.TOP_RECORD_NUMBER);

        List<DMOfflineBufferEntry> offlineEntries = find(query);

        if (!offlineEntries.isEmpty()) {
            logger.debug("Got offline entry with earliest TTL : {}", offlineEntries);
            return offlineEntries.get(0);
        } else {
            logger.debug("No entries found in dmOfflineBufferEntries collection");
            return null;
        }
    }

    /**
     * Fetch all records from offline buffer for which TTL has expired, 
     * and TTL value is greater than or equal to 0,
     * and TTL notification has not already been processed. This check to avoid
     * picking entries from the collection for which TTL value is set to default value i.e "-1"
     *
     * @return the offline buffer entries with expired ttl
     */
    @Override
    public List<DMOfflineBufferEntry> getOfflineBufferEntriesWithExpiredTtl() {

        long currTime = System.currentTimeMillis();
        IgniteCriteria criteriaExpiredTtl = new IgniteCriteria(TTL_EXPRIATION_TIME_FIELD, Operator.LTE, currTime);
        IgniteCriteria criteriaExpiredTtlNonDefault = new IgniteCriteria(TTL_EXPRIATION_TIME_FIELD, Operator.GTE, 0);
        IgniteCriteria ttlNotifNotProcessed = new IgniteCriteria(IS_TTL_NOTIF_PROCESSED_FIELD, Operator.EQ, false);
        IgniteCriteriaGroup criteriaGroup = new IgniteCriteriaGroup(criteriaExpiredTtl)
            .and(criteriaExpiredTtlNonDefault).and(ttlNotifNotProcessed);
        IgniteQuery query = new IgniteQuery(criteriaGroup);

        List<DMOfflineBufferEntry> offlineEntries = find(query);
        if (!offlineEntries.isEmpty()) {
            logger.debug("Got {} offline entries {}", offlineEntries.size(), offlineEntries);
            return offlineEntries;
        } else {
            logger.debug("No entries found in offline buffer with expiredTTL. Queried with time : {}", 
                    LocalDateTime.ofEpochSecond(currTime, 0, ZoneOffset.UTC).toString());
            return Collections.emptyList();
        }
    }

}
