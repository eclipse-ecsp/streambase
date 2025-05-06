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

package org.eclipse.ecsp.stream.dma;

import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.ShortHashCodeIdPartGenerator;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.key.IgniteStringKey;
import org.eclipse.ecsp.stream.dma.handler.DeviceHeaderUpdater;
import org.eclipse.ecsp.stream.dma.handler.DeviceMessageHandler;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;



/**
 * UT class {@link MsgIdUpdaterTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/stream-base-test.properties")
public class MsgIdUpdaterTest {
    
    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The Constant REDIS. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS = new EmbeddedRedisServer();
    
    /** The Constant SERVICE_SP_NAME. */
    private static final String SERVICE_SP_NAME = "Ecall";
    
    /** The received event. */
    private static DeviceMessage receivedEvent;
    
    /** The device id updater. */
    @Autowired
    private DeviceHeaderUpdater deviceIdUpdater;
    
    /** The transformer. */
    @Autowired
    private DeviceMessageIgniteEventTransformer transformer;
    
    /** The source topic. */
    private String sourceTopic = "testTopic";

    /**
     * Message id updation test.
     */
    @Test
    public void messageIdUpdationTest() {
        TestEvent value = new TestEvent();
        value.setVehicleId("vehicleId1");
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(value), Version.V1_0,
                value, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        DeviceMessage valueWithHeaders = deviceIdUpdater.addMessageIdIfNotPresent(entity);
        ShortHashCodeIdPartGenerator shcApp = new ShortHashCodeIdPartGenerator();
        String incVal = shcApp.generateIdPart(SERVICE_SP_NAME);

        boolean containsHashCode = valueWithHeaders.getDeviceMessageHeader().getMessageId()
                .startsWith(incVal);
        // Assert.assertTrue(containsHashCode);
        Assert.assertNull(valueWithHeaders.getDeviceMessageHeader().getCorrelationId());
    }

    /**
     * Message id updation negative test.
     */
    // MessageId is not overridden if already set.
    @Test
    public void messageIdUpdationNegativeTest() {
        TestEvent value = new TestEvent();
        value.setMessageId("notOverRidden");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(value),
                Version.V1_0, value, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        DeviceMessage valueWithHeaders = deviceIdUpdater.addMessageIdIfNotPresent(entity);
        Assert.assertEquals("notOverRidden", valueWithHeaders.getDeviceMessageHeader().getMessageId());
    }

    /**
     * <P>
     * event.header.updation.type=messageId
     *
     * Hence it sets a messageId if it is not set.(header updated)
     *
     * If a messageId is set the IgniteEvent is not updated.
     *
     * CorrelationId is not updated in either case.
     * </P>
     */
    @Test
    public void deviceHeaderUpdaterTest() {
        deviceIdUpdater.setNextHandler(new TestHandler());
        IgniteStringKey key = new IgniteStringKey();
        key.setKey("vehicleABC");
        TestEvent value = new TestEvent();
        value.setVehicleId("vehicleABC");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(value),
                Version.V1_0, value, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        Assert.assertNull(value.getMessageId());
        deviceIdUpdater.handle(key, entity);
        Assert.assertNotNull(receivedEvent.getDeviceMessageHeader().getMessageId());
        Assert.assertNull(value.getCorrelationId());

        value.setMessageId("notOverRidden");
        entity = new DeviceMessage(transformer.toBlob(value), Version.V1_0,
                value, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        deviceIdUpdater.handle(key, entity);
        Assert.assertEquals("notOverRidden", receivedEvent.getDeviceMessageHeader().getMessageId());
        Assert.assertNull(value.getCorrelationId());
    }

    /**
     * inner class TestHandler implements DeviceMessageHandler.
     */
    public static final class TestHandler implements DeviceMessageHandler {

        /**
         * Handle.
         *
         * @param key the key
         * @param value the value
         */
        @Override
        public void handle(IgniteKey<?> key, DeviceMessage value) {
            receivedEvent = value;

        }

        /**
         * Sets the next handler.
         *
         * @param handler the new next handler
         */
        @Override
        public void setNextHandler(DeviceMessageHandler handler) {

        }

        /**
         * Close.
         */
        @Override
        public void close() {

        }

    }

    /**
     * innerc class TestEvent extends IgniteEventImpl.
     */
    public class TestEvent extends IgniteEventImpl {

        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /**
         * Instantiates a new test event.
         */
        public TestEvent() {

        }

        /**
         * Gets the event id.
         *
         * @return the event id
         */
        @Override
        public String getEventId() {
            return "Sample";
        }

    }

}
