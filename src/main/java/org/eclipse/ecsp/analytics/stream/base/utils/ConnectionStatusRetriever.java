package org.eclipse.ecsp.analytics.stream.base.utils;

import org.eclipse.ecsp.entities.dma.VehicleIdDeviceIdStatus;

/**
 * CR-4570 DMA should expose an interface for services to retrieve connection 
 * status from an API.
 * Services can implement this interface and plug-in its implementation configuration to 
 * provide their own logic to call the API and fetch device connection status.
 *
 * @author HBadshah
 */
public interface ConnectionStatusRetriever {

    /** An interface to fetch the connection status of devices from an API.
     *
     * @param requestId The ID of the request.
     * @param vehicleId The ID of the vehicle.
     * @param deviceId the ID of the device.
     *
     * @return {@link VehicleIdDeviceIdStatus}
     */
    public VehicleIdDeviceIdStatus getConnectionStatusData(String requestId, String vehicleId, String deviceId);
}