package org.eclipse.ecsp.stream.dma.dao;

import jakarta.annotation.PostConstruct;
import org.eclipse.ecsp.analytics.stream.base.kafka.internal.OffsetMetadata;
import org.eclipse.ecsp.analytics.stream.base.stores.CachedMapStateStore;
import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;
import org.eclipse.ecsp.stream.dma.dao.key.DeviceStatusKey;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Implementation of the DeviceConnStatusDao interface for managing device connection status.
 */
@Repository("deviceConnStatusApiDaoImpl")
public class DeviceConnStatusApiDaoImpl
        extends CachedMapStateStore<DeviceStatusKey, VehicleIdDeviceIdStatus>
        implements DeviceConnStatusDao<VehicleIdDeviceIdStatus> {

    /**
     * Logger instance for logging messages.
     */
    private static IgniteLogger logger =
            IgniteLoggerFactory.getLogger(DeviceConnStatusApiDaoImpl.class);

    /**
     * Metadata for the latest Kafka topic partition and offset.
     */
    private OffsetMetadata latestOffsetMetadata;

    /**
     * Retrieves the latest TopicPartition and offset value of Kafka Consumer from Redis.
     *
     * @param serviceName Name of the service for which offset metadata is retrieved.
     * @return An {@link Optional} containing the latest offset metadata, if available.
     */
    @Override
    public Optional<OffsetMetadata> getOffsetMetadata(String serviceName) {
        logger.trace("getting offsetMetadata for service{}", serviceName);
        return Optional.of(latestOffsetMetadata);
    }

    /**
     * Initializes the DAO by setting persistence in Ignite cache to false.
     */
    @PostConstruct
    @Override
    public void initialize() {
        setPersistInIgniteCache(false);
    }
}
