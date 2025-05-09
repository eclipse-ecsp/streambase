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
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.ShortCounterIdPartGenerator;
import org.eclipse.ecsp.analytics.stream.base.idgen.internal.ShortHashCodeIdPartGenerator;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.cache.redis.EmbeddedRedisServer;
import org.eclipse.ecsp.dao.utils.EmbeddedMongoDB;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.stream.dma.handler.DeviceHeaderUpdater;
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
 * UT class  {@link MsgIdAndCorrIdUpdaterTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/stream-base-test.properties")
public class MsgIdAndCorrIdUpdaterTest {
    
    /** The Constant MONGO_SERVER. */
    @ClassRule
    public static final EmbeddedMongoDB MONGO_SERVER = new EmbeddedMongoDB();
    
    /** The Constant REDIS. */
    @ClassRule
    public static final EmbeddedRedisServer REDIS = new EmbeddedRedisServer();
    
    /** The Constant SERVICE_SP_NAME. */
    private static final String SERVICE_SP_NAME = "Ecall";
    
    /** The device id updater. */
    @Autowired
    private DeviceHeaderUpdater deviceIdUpdater;
    
    /** The transformer. */
    @Autowired
    private DeviceMessageIgniteEventTransformer transformer;

    /** The source topic. */
    private String sourceTopic = "testTopic";

    /**
     * Test index appender.
     */
    @Test
    public void testIndexAppender() {
        ShortCounterIdPartGenerator indexApp = new ShortCounterIdPartGenerator();
        int val = indexApp.getMsgIdSuffix().get();
        String incVal = indexApp.generateIdPart(SERVICE_SP_NAME);
        Assert.assertEquals(incVal, ((val + 1) + ""));
    }

    /**
     * Test short hash code appender.
     */
    @Test
    public void testShortHashCodeAppender() {
        ShortHashCodeIdPartGenerator shcApp = new ShortHashCodeIdPartGenerator();
        String incVal = shcApp.generateIdPart(SERVICE_SP_NAME);
        Assert.assertEquals("21465", incVal);
    }

    /**
     * Message Id should be set if not present and correlationId should be null.
     */

    @Test
    public void testAddingMessageId() {

        TestEvent value = new TestEvent();
        value.setVehicleId("vehicleId");
        DeviceMessage entity = new DeviceMessage(transformer.toBlob(value),
                Version.V1_0, value, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        DeviceMessageHeader valueWithHeaders = deviceIdUpdater.addMessageIdAndCorrelationIdIfNotPresent(entity)
                .getDeviceMessageHeader();
        Assert.assertNull(valueWithHeaders.getCorrelationId());

    }

    /**
     * correlationId Id should be set if MessageId is present and MessageId should be updated with a new value.
     */
    @Test
    public void testAddingCorrelationId() {
        TestEvent value = new TestEvent();
        value.setMessageId("12345");
        value.setVehicleId("vehicleId");

        DeviceMessage entity = new DeviceMessage(transformer.toBlob(value),
                Version.V1_0, value, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);

        DeviceMessageHeader valueWithHeaders = deviceIdUpdater.addMessageIdAndCorrelationIdIfNotPresent(entity)
                .getDeviceMessageHeader();
        Assert.assertEquals("12345", valueWithHeaders.getCorrelationId());
    }

    /**
     * Test generating unique message ids.
     */
    @Test
    public void testGeneratingUniqueMessageIds() {

        TestEvent value1 = new TestEvent();
        value1.setVehicleId("vehicleId1");
        DeviceMessage entity1 = new DeviceMessage(transformer.toBlob(value1),
                Version.V1_0, value1, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        DeviceMessageHeader valueWithHeaders1 = deviceIdUpdater.addMessageIdAndCorrelationIdIfNotPresent(entity1)
                .getDeviceMessageHeader();

        TestEvent value2 = new TestEvent();
        value2.setVehicleId("vehicleId1");
        DeviceMessage entity2 = new DeviceMessage(transformer.toBlob(value2),
                Version.V1_0, value2, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        DeviceMessageHeader valueWithHeaders2 = deviceIdUpdater.addMessageIdAndCorrelationIdIfNotPresent(entity2)
                .getDeviceMessageHeader();

        TestEvent value3 = new TestEvent();
        value3.setVehicleId("vehicleId1");
        DeviceMessage entity3 = new DeviceMessage(transformer.toBlob(value3),
                Version.V1_0, value3, sourceTopic, Constants.THREAD_SLEEP_TIME_60000);
        DeviceMessageHeader valueWithHeaders3 = deviceIdUpdater.addMessageIdAndCorrelationIdIfNotPresent(entity3)
                .getDeviceMessageHeader();

        String incVal1 = valueWithHeaders1.getMessageId();
        String incVal2 = valueWithHeaders2.getMessageId();
        String incVal3 = valueWithHeaders3.getMessageId();

        Assert.assertNotEquals(incVal1, incVal2);
        Assert.assertNotEquals(incVal2, incVal3);
        Assert.assertNotEquals(incVal3, incVal1);

    }

    /**
     * inner class TestEvent extends IgniteEventImpl.
     */
    public class TestEvent extends IgniteEventImpl {

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
            return "Sample";
        }

    }

}
