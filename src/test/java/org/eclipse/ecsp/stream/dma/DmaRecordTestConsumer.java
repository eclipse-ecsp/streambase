package org.eclipse.ecsp.stream.dma;

import org.eclipse.ecsp.stream.dma.entities.IgniteDeviceStatusRecord;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.ArrayList;
import java.util.List;

/**
 * A consumer class for testing DMA records. This class listens for `IgniteDeviceStatusRecord`
 * events asynchronously and stores the received records in a list for further testing.
 */
@Configuration
@EnableAsync
public class DmaRecordTestConsumer {

    private static final IgniteLogger LOGGER =
            IgniteLoggerFactory.getLogger(DmaRecordTestConsumer.class);

    private List<IgniteDeviceStatusRecord> deviceStatusRecordList = new ArrayList<>();

    @Async
    @EventListener
    public void onApplicationEvent(IgniteDeviceStatusRecord deviceStatusRecord) {
        LOGGER.debug("Message received from DMA {}", deviceStatusRecord);
        this.deviceStatusRecordList.add(deviceStatusRecord);
    }

    public List<IgniteDeviceStatusRecord> getMessages() {
        return this.deviceStatusRecordList;
    }
}
