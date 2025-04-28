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

import jakarta.annotation.PostConstruct;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.dao.CacheBackedInMemoryBatchCompleteCallBack;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.analytics.stream.base.stores.CachedMapStateStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdMapping;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;


/**
 * DeviceStatusCacheBackedInMemoryDAO is an implementation of
 * DeviceStatusDAO where the DAO layer is CacheBackedInMemoryDAOImpl (fusion of
 * In-Memory Map store and Redis).
 * Whenever querying for device status the input deviceId
 * should be of the format DEVICE_STATUS_{@code <}SERVICE{@code >}_{@code <}deviceID{@code >}.
 *
 * @author avadakkootko
 */
@Repository
public class DeviceStatusDaoCacheBackedInMemoryImpl extends
        CachedMapStateStore<DeviceStatusKey, VehicleIdDeviceIdMapping> implements DeviceConnStatusDAO {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DeviceStatusDaoCacheBackedInMemoryImpl.class);

    /** The latest offset metadata. */
    private OffsetMetadata latestOffsetMetadata;

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /** The sub services. */
    @Value("${" + PropertyNames.SUB_SERVICES + ":}")
    private String subServices;

    /**
     * Sets the service name.
     *
     * @param serviceName the new service name
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Get the latest TopicPartition and offset value of Kafka Consumer from Redis.
     *
     * @param serviceName the service name
     * @return the offset metadata
     */
    @Override
    public Optional<OffsetMetadata> getOffsetMetadata(String serviceName) {
        return Optional.of(latestOffsetMetadata);
    }

    /**
     * initialize().
     */
    @PostConstruct
    public void initialize() {
        long currentTime = System.currentTimeMillis();
        setCallBack(new DmaBatchCompleteCallBack());
        setPersistInIgniteCache(false);
        long endTime = System.currentTimeMillis();
        logger.info("Time taken to Initialize DeviceStatusDAO is {} seconds",
                (endTime - currentTime) / Constants.THOUSAND);
    }

    /**
     * Sets the sub services.
     *
     * @param subServices the new sub services
     */
    public void setSubServices(String subServices) {
        this.subServices = subServices;
    }

    /**
     * The Class DmaBatchCompleteCallBack.
     */
    class DmaBatchCompleteCallBack implements CacheBackedInMemoryBatchCompleteCallBack {

        /**
         * Batch completed.
         *
         * @param processedRecords the processed records
         */
        @Override
        public void batchCompleted(List<Object> processedRecords) {
        //  Auto-generated method stub
        }

    }

}