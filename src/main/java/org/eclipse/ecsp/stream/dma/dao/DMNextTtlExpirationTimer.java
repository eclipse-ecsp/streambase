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
import dev.morphia.annotations.Id;
import org.eclipse.ecsp.entities.AbstractIgniteEntity;


/**
 *  class DMNextTtlExpirationTimer extends AbstractIgniteEntity.
 */
@Entity()
public class DMNextTtlExpirationTimer extends AbstractIgniteEntity {

    /** The id. */
    @Id
    private String id;

    /** The ttl expiration timer. */
    private long ttlExpirationTimer;

    /**
     * Instantiates a new DM next ttl expiration timer.
     */
    public DMNextTtlExpirationTimer() {
        this.id = DMAConstants.DM_NEXT_TTL_EXPIRATION_TIMER_KEY;
    }

    /**
     * Instantiates a new DM next ttl expiration timer.
     *
     * @param ttlExpirationTimer the ttl expiration timer
     */
    public DMNextTtlExpirationTimer(long ttlExpirationTimer) {
        this.id = DMAConstants.DM_NEXT_TTL_EXPIRATION_TIMER_KEY;
        this.ttlExpirationTimer = ttlExpirationTimer;
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
     * Sets the id.
     *
     * @param id the new id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the ttl expiration timer.
     *
     * @return the ttl expiration timer
     */
    public long getTtlExpirationTimer() {
        return ttlExpirationTimer;
    }

    /**
     * Sets the ttl expiration timer.
     *
     * @param ttlExpirationTimer the new ttl expiration timer
     */
    public void setTtlExpirationTimer(long ttlExpirationTimer) {
        this.ttlExpirationTimer = ttlExpirationTimer;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return "DMNextTtlExpirationTimer [id=" + id + ", ttlExpirationTimer=" + ttlExpirationTimer + "]";
    }

}
