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

package org.eclipse.ecsp.stream.dma.shouldertap;

import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;



/**
 * {@link DeviceShoulderTapServiceTest} UT class for {@link DeviceShoulderTapService}.
 */
public class DeviceShoulderTapServiceTest {

    /** The device shoulder tap service. */
    @InjectMocks
    private DeviceShoulderTapService deviceShoulderTapService;

    /** The shoulder tap retry handler. */
    @Mock
    private DeviceShoulderTapRetryHandler shoulderTapRetryHandler;

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        deviceShoulderTapService.setShoulderTapEnabled(true);
    }

    /**
     * Test wake up device wake up status true.
     */
    @Test
    public void testWakeUpDeviceWakeUpStatusTrue() {

        TestKey igniteKey = new TestKey();
        DeviceMessage igniteEvent = new DeviceMessage();
        TestEvent event = new TestEvent();
        igniteEvent.setEvent(event);

        Map<String, Object> extraParameters = new HashMap<>();
        extraParameters.put("bizTransactionId", "bizTransactionId123");

        Mockito.when(shoulderTapRetryHandler.registerDevice(igniteKey, igniteEvent, extraParameters))
                .thenReturn(true);
        String requestId = "Request123";
        String vehicleId = "Vehicle123";
        String serviceName = "ECall";
        boolean wakeUpStatus = deviceShoulderTapService
                .wakeUpDevice(requestId, vehicleId, serviceName, igniteKey, igniteEvent,
                extraParameters);

        assertEquals("Expected wakeUpStatus as true", true, wakeUpStatus);

        Mockito.verify(shoulderTapRetryHandler, Mockito.times(1)).registerDevice(Mockito.any(IgniteKey.class),
                Mockito.any(DeviceMessage.class), Mockito.any(Map.class));
    }

    /**
     * Test wake up device wake up status false.
     */
    @Test
    public void testWakeUpDeviceWakeUpStatusFalse() {
        TestKey igniteKey = new TestKey();
        TestEvent event = new TestEvent();
        DeviceMessage igniteEvent = new DeviceMessage();
        igniteEvent.setEvent(event);

        Map<String, Object> extraParameters = new HashMap<>();
        extraParameters.put("bizTransactionId", "bizTransactionId123");

        Mockito.when(shoulderTapRetryHandler.registerDevice(igniteKey, igniteEvent, extraParameters))
                .thenReturn(false);

        String requestId = "Req123";
        String vehicleId = "Vehicle123";
        String serviceName = "ECall";
        boolean wakeUpStatus = deviceShoulderTapService
                .wakeUpDevice(requestId, vehicleId, serviceName, igniteKey, igniteEvent,
                extraParameters);

        assertEquals("Expected wakeUpStatus as false", false, wakeUpStatus);
        Mockito.verify(shoulderTapRetryHandler, Mockito.times(1)).registerDevice(Mockito.any(IgniteKey.class),
                Mockito.any(DeviceMessage.class), Mockito.any(Map.class));
    }

    /**
     * Test wake up device wake up status with shoulder tap disabled.
     */
    @Test
    public void testWakeUpDeviceWakeUpStatusWithShoulderTapDisabled() {
        DeviceMessage igniteEvent = new DeviceMessage();
        TestEvent event = new TestEvent();
        igniteEvent.setEvent(event);

        Map<String, Object> extraParameters = new HashMap<>();
        extraParameters.put("bizTransactionId", "bizTransactionId123");

        deviceShoulderTapService.setShoulderTapEnabled(false);
        TestKey igniteKey = new TestKey();
        String serviceName = "ECall";
        String requestId = "Req123";
        String vehicleId = "Vehicle123";
        boolean wakeUpStatus = deviceShoulderTapService.wakeUpDevice(requestId,
                vehicleId, serviceName, igniteKey, igniteEvent,
                extraParameters);
        assertEquals("Expected wakeUpStatus as false", false, wakeUpStatus);

        Mockito.verify(shoulderTapRetryHandler, Mockito.times(0))
                .registerDevice(igniteKey, igniteEvent, extraParameters);
    }

    /**
     * Testexecute on device active status.
     */
    @Test
    public void testexecuteOnDeviceActiveStatus() {
        String requestId = "Req123";
        String vehicleId = "Vehicle123";
        String serviceName = "ECall";

        deviceShoulderTapService.executeOnDeviceActiveStatus(requestId, vehicleId, serviceName);
        Mockito.verify(shoulderTapRetryHandler, Mockito.times(1))
                .deregisterDevice(Mockito.any(String.class));
    }

    /**
     * Test execute on device active status with shoulder tap disabled.
     */
    @Test
    public void testExecuteOnDeviceActiveStatusWithShoulderTapDisabled() {
        deviceShoulderTapService.setShoulderTapEnabled(false);
        String requestId = "Req123";
        String vehicleId = "Vehicle123";
        String serviceName = "ECall";
        deviceShoulderTapService.executeOnDeviceActiveStatus(requestId, vehicleId, serviceName);
        Mockito.verify(shoulderTapRetryHandler, Mockito.times(0))
                .deregisterDevice(Mockito.any(String.class));
    }

    /**
     * Test set stream processing context.
     */
    @Test
    public void testSetStreamProcessingContext() {
        StreamProcessingContext streamProcessingContext = Mockito.mock(StreamProcessingContext.class);
        deviceShoulderTapService.setStreamProcessingContext(streamProcessingContext);

        Mockito.verify(shoulderTapRetryHandler,
                Mockito.times(1)).setStreamProcessingContext(Mockito.any(StreamProcessingContext.class));
    }

    /**
     * Test setup.
     */
    @Test
    public void testSetup() {
        String taskId = "0_0";
        deviceShoulderTapService.setup(taskId);

        Mockito.verify(shoulderTapRetryHandler,
                Mockito.times(1)).setup(Mockito.any(String.class));
    }

    /**
     * Test setup with shoulder tap disabled.
     */
    @Test
    public void testSetupWithShoulderTapDisabled() {
        String taskId = "0_0";
        deviceShoulderTapService.setShoulderTapEnabled(false);
        deviceShoulderTapService.setup(taskId);
        Mockito.verify(shoulderTapRetryHandler,
                Mockito.times(0)).setup(Mockito.any(String.class));
    }

    /**
     * The Class TestKey.
     */
    class TestKey implements IgniteKey<String> {

        /** The vehicle id. */
        private String vehicleId;

        /**
         * Gets the key.
         *
         * @return the key
         */
        @Override
        public String getKey() {
            return vehicleId;
        }

        /**
         * Sets the key.
         *
         * @param vehicleId the new key
         */
        public void setKey(String vehicleId) {
            this.vehicleId = vehicleId;
        }
    }

    /**
     * The Class TestEvent.
     */
    class TestEvent extends IgniteEventImpl {
        
        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /**
         * Instantiates a new test event.
         */
        public TestEvent() {
            // Nothing to do.
        }

        /**
         * Gets the event id.
         *
         * @return the event id
         */
        @Override
        public String getEventId() {
            return "Speed";
        }

        /**
         * Gets the target device id.
         *
         * @return the target device id
         */
        @Override
        public Optional<String> getTargetDeviceId() {
            return Optional.of("Device12345");
        }

    }
}
