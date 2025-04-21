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

import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Field;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Index;
import dev.morphia.annotations.IndexOptions;
import dev.morphia.annotations.Indexes;
import dev.morphia.utils.IndexType;
import org.eclipse.ecsp.entities.AbstractIgniteEntity;
import org.eclipse.ecsp.entities.AuditableIgniteEntity;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;

import java.time.LocalDateTime;
import java.util.UUID;


/**
 * class {@link DMOfflineBufferEntry} extends {@link AbstractIgniteEntity}
 * implements {@link AuditableIgniteEntity}.
 */
@Entity()
@Indexes(value = { @Index(fields = @Field(value = "eventTs"),
        options = @IndexOptions(expireAfterSeconds = 31536000, background = true)),
                   @Index(fields = { @Field(value = "vehicleId"), 
                     @Field(value = "priority", type = IndexType.DESC) }, options = @IndexOptions(background = true)),
                   @Index(fields = { @Field(value = "ttlExpirationTime"), 
                     @Field(value = "isTtlNotifProcessed") }, options = @IndexOptions(background = true)) })
public class DMOfflineBufferEntry extends AbstractIgniteEntity implements AuditableIgniteEntity {
    
    /** The id. */
    @Id
    private String id;

    /** The vehicle id. */
    private String vehicleId;

    /** The device id. */
    private String deviceId;

    /** The event ts. */
    private LocalDateTime eventTs;

    /** The ignite key. */
    private IgniteKey<String> igniteKey;

    /** The event. */
    private DeviceMessage event;

    /** The priority. */
    /*
     * Priority of the message with range from 0 to 10. Default is 0. For
     * DeviceMessage with isShoulderTapEnabled=true, set priority as 10.
     */
    private int priority;

    /** The pending retries. */
    private int pendingRetries;

    /** The sub service. */
    /*
     * RTC 355420. If sub-services exists, then ignite query to fetch documents
     * from mongo should have sub-service condition as well along with VIN.
     */
    private String subService;

    /** The ttl expiration time. */
    private long ttlExpirationTime;
    
    /** The is ttl notif processed. */
    private boolean isTtlNotifProcessed;

    /**
     * Instantiates a new DM offline buffer entry.
     */
    public DMOfflineBufferEntry() {
        StringBuilder keyBuilder = new StringBuilder();
        id = keyBuilder.append(UUID.randomUUID().toString()).append(System.currentTimeMillis()).toString();
    }

    /**
     * Gets the event.
     *
     * @return the event
     */
    public DeviceMessage getEvent() {
        return event;
    }

    /**
     * Sets the event.
     *
     * @param event the new event
     */
    public void setEvent(DeviceMessage event) {
        this.event = event;
    }

    /**
     * Gets the event ts.
     *
     * @return the event ts
     */
    public LocalDateTime getEventTs() {
        return eventTs;
    }

    /**
     * Sets the event ts.
     *
     * @param eventTs the new event ts
     */
    public void setEventTs(LocalDateTime eventTs) {
        this.eventTs = eventTs;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the ignite key.
     *
     * @return the ignite key
     */
    public IgniteKey<String> getIgniteKey() {
        return igniteKey;
    }

    /**
     * Sets the ignite key.
     *
     * @param igniteKey the new ignite key
     */
    public void setIgniteKey(IgniteKey<String> igniteKey) {
        this.igniteKey = igniteKey;
    }

    /**
     * Gets the priority.
     *
     * @return the priority
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Sets the priority.
     *
     * @param priority the new priority
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * Gets the vehicle id.
     *
     * @return the vehicle id
     */
    public String getVehicleId() {
        return vehicleId;
    }

    /**
     * Sets the vehicle id.
     *
     * @param vehicleId the new vehicle id
     */
    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    /**
     * Gets the device id.
     *
     * @return the device id
     */
    public String getDeviceId() {
        return deviceId;
    }

    /**
     * Sets the device id.
     *
     * @param deviceId the new device id
     */
    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    /**
     * Gets the pending retries.
     *
     * @return the pending retries
     */
    public int getPendingRetries() {
        return this.pendingRetries;
    }

    /**
     * Sets the pending retries.
     *
     * @param pendingRetries the new pending retries
     */
    public void setPendingRetries(int pendingRetries) {
        this.pendingRetries = pendingRetries;
    }

    /**
     * Gets the sub service.
     *
     * @return the sub service
     */
    public String getSubService() {
        return this.subService;
    }

    /**
     * Sets the sub service.
     *
     * @param subService the new sub service
     */
    public void setSubService(String subService) {
        this.subService = subService;
    }

    /**
     * Gets the ttl expiration time.
     *
     * @return the ttl expiration time
     */
    public long getTtlExpirationTime() {
        return ttlExpirationTime;
    }

    /**
     * Sets the ttl expiration time.
     *
     * @param ttlExpirationTime the new ttl expiration time
     */
    public void setTtlExpirationTime(long ttlExpirationTime) {
        this.ttlExpirationTime = ttlExpirationTime;
    }
    
    /**
     * Checks if is ttl notif processed.
     *
     * @return true, if is ttl notif processed
     */
    public boolean isTtlNotifProcessed() {
        return isTtlNotifProcessed;
    }

    /**
     * Sets the ttl notif processed.
     *
     * @param isTtlNotifProcessed the new ttl notif processed
     */
    public void setTtlNotifProcessed(boolean isTtlNotifProcessed) {
        this.isTtlNotifProcessed = isTtlNotifProcessed;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "DMOfflineBufferEntry [id=" + id + ", vehicleId=" + vehicleId
                + ", deviceId=" + deviceId + ", eventTs=" + eventTs
                + ", igniteKey=" + igniteKey + ", event=" + event + ", priority="
                + priority + ",pendingRetries=" + pendingRetries + ",subService=" + subService
                + ", ttlExpirationTime=" + ttlExpirationTime + ", "
                + "isTtlNotifProcessed=" + isTtlNotifProcessed + "]";
    }

}
