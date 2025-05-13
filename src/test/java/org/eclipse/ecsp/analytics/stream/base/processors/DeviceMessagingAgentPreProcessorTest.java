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

package org.eclipse.ecsp.analytics.stream.base.processors;

import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.entities.dma.RetryRecord;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMARetryRecordDAOCacheBackedInMemoryImpl;
import org.eclipse.ecsp.stream.dma.dao.key.RetryRecordKey;
import org.eclipse.ecsp.stream.dma.handler.RetryTestEvent;
import org.eclipse.ecsp.stream.dma.handler.RetryTestKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;

import java.util.Optional;
import java.util.Properties;



/**
 * UT class for {@link DeviceMessagingAgentPreProcessor}.
 */
public class DeviceMessagingAgentPreProcessorTest {

    /** The dma pre processor. */
    @InjectMocks
    private DeviceMessagingAgentPreProcessor dmaPreProcessor;

    /** The retry event DAO. */
    @Mock
    private DMARetryRecordDAOCacheBackedInMemoryImpl retryEventDAO;

    /** The spc. */
    @Mock
    private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

    /** The ignite key. */
    private RetryTestKey igniteKey;

    /** The msg id. */
    private String msgId = "msgId123";
    
    /** The task id. */
    private String taskId = "00_00";

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":Ecall}")
    private String serviceName;

    /**
     * setup().
     *
     * @throws Exception Exception
     */
    @Before
    public void setUp() throws Exception {
        igniteKey = new RetryTestKey();
        igniteKey.setKey("VIN123");
        MockitoAnnotations.initMocks(this);
        DeviceMessage entity = new DeviceMessage();
        DeviceMessageHeader header = new DeviceMessageHeader();
        header.withMessageId(msgId);
        entity.setDeviceMessageHeader(header);
        RetryRecord retryRecord = new RetryRecord(igniteKey, entity, System.currentTimeMillis());
        Mockito.when(retryEventDAO.get(new RetryRecordKey(msgId, taskId))).thenReturn(retryRecord);
    }

    /**
     * Test remove event from cache.
     */
    @Test
    public void testRemoveEventFromCache() {
        RetryTestEvent event = new RetryTestEvent();
        event.setMessageId("msgId223");
        event.setCorrelationId(msgId);
        String mapKey = RetryRecordKey.getMapKey(serviceName, taskId);
        dmaPreProcessor.setMapKey(mapKey);
        dmaPreProcessor.process(new Record<>(igniteKey, event, System.currentTimeMillis()));
        Mockito.verify(retryEventDAO, Mockito.times(1))
                .deleteFromMap(Mockito.anyString(), Mockito.any(RetryRecordKey.class),
                Mockito.any(Optional.class), Mockito.anyString());

        ArgumentCaptor<String> parentKeyArgument = ArgumentCaptor.forClass(String.class);
        Mockito.verify(retryEventDAO).deleteFromMap(parentKeyArgument.capture(), Mockito.any(RetryRecordKey.class),
                Mockito.any(Optional.class), Mockito.anyString());
        Assert.assertEquals(mapKey, parentKeyArgument.getValue());
    }

    /**
     * Testnull record.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testnullRecord() {
        RetryTestEvent event = new RetryTestEvent();
        event.setMessageId("msgId223");
        event.setCorrelationId(msgId);
        String mapKey = RetryRecordKey.getMapKey(serviceName, taskId);
        dmaPreProcessor.setMapKey(mapKey);
        dmaPreProcessor.process(null);

    }

    /**
     * Testnull key.
     */
    @Test(expected = RuntimeException.class)
    public void testnullKey() {
        RetryTestEvent event = new RetryTestEvent();
        event.setMessageId("msgId223");
        event.setCorrelationId(msgId);
        dmaPreProcessor.configChanged(new Properties());
        dmaPreProcessor.punctuate(TestConstants.TWELVE);
        dmaPreProcessor.setMapKey(null);
        dmaPreProcessor.process(new Record<>(null, event, System.currentTimeMillis()));
    }

}