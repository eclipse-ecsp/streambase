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

package org.eclipse.ecsp.analytics.stream.vehicleprofile.utils;


/**
 * VehicleProfileEntity.
 */
public class VehicleProfileEntity {

    /** The vin. */
    private String vin;

    /**
     * Sets the vin.
     *
     * @param vin the new vin
     */
    public void setVin(String vin) {
        this.vin = vin;
    }

    /**
     * Gets the vin.
     *
     * @return the vin
     */
    public String getVin() {
        return vin;
    }
}
