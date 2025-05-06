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

package org.eclipse.ecsp.stream.dma.handler;

import org.eclipse.ecsp.analytics.stream.base.KafkaStreamsProcessorContext;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessage;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.config.DMAConfigResolver;
import org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer;
import org.eclipse.ecsp.transform.Transformer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;



/**
 * {@link DeviceMessagingHandlerChainTest}  UT class for {@link DeviceMessagingHandlerChain}.
 */
public class DeviceMessagingHandlerChainTest {

    /** The Constant CONFIG_RESOLVER. */
    private static final String CONFIG_RESOLVER = "configResolver";
    
    /** The Constant DMA_POST_DISPATCH_HANDLER. */
    private static final String DMA_POST_DISPATCH_HANDLER = "dmaPostDispatchHandler";
    
    /** The Constant DEVICE_MESSAGING_EVENT_TRANSFORMER. */
    private static final String DEVICE_MESSAGING_EVENT_TRANSFORMER = "deviceMessagingEventTransformer";
    
    /** The Constant DEFAULT_POST_DISPATCH_HANDLER_CLASS_NAME. */
    private static final String DEFAULT_POST_DISPATCH_HANDLER_CLASS_NAME =
            "org.eclipse.ecsp.stream.dma.handler.DefaultPostDispatchHandler";

    /** The handler chain. */
    @InjectMocks
    public DeviceMessagingHandlerChain handlerChain = new DeviceMessagingHandlerChain();
    
    /** The config resolver. */
    @Mock
    public DMAConfigResolver configResolver = Mockito.mock(DMAConfigResolverTestImpl.class);
    
    /** The message handler. */
    @Mock
    public DeviceMessageHandler messageHandler = Mockito.mock(DefaultPostDispatchHandler.class);
    
    /** The transformer. */
    @InjectMocks
    public Transformer transformer = new DeviceMessageIgniteEventTransformer();
    
    /** The spc. */
    @Mock
    public StreamProcessingContext spc = Mockito.mock(KafkaStreamsProcessorContext.class);
    
    /** The msg validator. */
    @Mock
    public DeviceMessageValidator msgValidator = Mockito.mock(DeviceMessageValidator.class);
    
    /** The header updater. */
    @Mock
    public DeviceHeaderUpdater headerUpdater = Mockito.mock(DeviceHeaderUpdater.class);
    
    /** The retry handler. */
    @Mock
    public RetryHandler retryHandler = Mockito.mock(RetryHandler.class);
    
    /** The thrown. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    /** The dispatch handler. */
    @Mock
    private DispatchHandler dispatchHandler = Mockito.mock(DispatchHandler.class);
    
    /** The key. */
    private RetryTestKey key;
    
    /** The ignite event. */
    private IgniteEventImpl igniteEvent;
    
    /** The conn status handler. */
    @Mock
    private DeviceConnectionStatusHandler connStatusHandler = Mockito.mock(DeviceConnectionStatusHandler.class);
    
    /** The dma post dispatch handler. */
    @Mock
    private DeviceMessageHandler dmaPostDispatchHandler = Mockito.mock(DeviceMessageHandler.class);

    /**
     * setup(): to initialize ignite event.
     */
    @Before
    public void setup() {
        igniteEvent = new IgniteEventImpl();
        igniteEvent.setEventId("test");
        igniteEvent.setDeviceRoutable(true);
        key = new RetryTestKey();
        key.setKey("vehicle123");
        ReflectionTestUtils.setField(handlerChain, "dmEventTransformer", transformer);
        ReflectionTestUtils.setField(handlerChain, "spc", spc);
        ReflectionTestUtils.setField(handlerChain, CONFIG_RESOLVER, configResolver);
        ReflectionTestUtils.setField(handlerChain,
                "deviceMessageValidator", msgValidator);
        ReflectionTestUtils.setField(handlerChain,
                "dmaPostDispatchHandlerClass", DEFAULT_POST_DISPATCH_HANDLER_CLASS_NAME);
        ReflectionTestUtils.setField(handlerChain, DEVICE_MESSAGING_EVENT_TRANSFORMER,
                "org.eclipse.ecsp.transform.DeviceMessageIgniteEventTransformer");
    }

    /**
     * Test populate map.
     */
    @Test
    public void testPopulateMap() {
        String broker1 = "broker1";
        String ecuType1 = "ecuType1";
        String ecuType2 = "ecuType2";
        String testTopic1 = "testTopic1";
        String testTopic2 = "testTopic2";

        String item1 = broker1 + ":" + ecuType1 + "#" + testTopic1 + "," + ecuType2 + "#" + testTopic2;

        List<String> ecuTypes = new ArrayList<>(Arrays.asList(item1));
        ReflectionTestUtils.setField(handlerChain, "ecuTypes", ecuTypes);
        handlerChain.setUp();

        Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) ReflectionTestUtils
                .getField(handlerChain, "brokerToEcuTypesMapping");
        String actualBrokerName = (String) map.keySet().toArray()[0];
        Assert.assertEquals("broker1", actualBrokerName);

        Map<String, String> innerMap = map.get(actualBrokerName);
        Assert.assertEquals(testTopic1, innerMap.get(ecuType1.toLowerCase()));
        Assert.assertEquals(testTopic2, innerMap.get(ecuType2.toLowerCase()));
    }

    /**
     * Test setup with default dma post dispatch handler.
     */
    @Test
    public void testSetupWithDefaultDmaPostDispatchHandler() {
        handlerChain.setUp();
        Assert.assertEquals("Incorrect post dispatch handler implementation class",
                DEFAULT_POST_DISPATCH_HANDLER_CLASS_NAME,
                ReflectionTestUtils.getField(handlerChain, DMA_POST_DISPATCH_HANDLER).getClass().getCanonicalName());
    }

    /**
     * Test setup with null dma config resolver class.
     */
    @Test
    public void testSetupWithNullDmaConfigResolverClass() {
        ReflectionTestUtils.setField(handlerChain, CONFIG_RESOLVER, null);
        handlerChain.setUp();
        DMAConfigResolver configResolverInstance = (DMAConfigResolver)
                ReflectionTestUtils.getField(handlerChain, CONFIG_RESOLVER);
        Assert.assertNotNull(configResolverInstance);
    }

    /**
     * Test get device routable entity with empty dm feedback topic.
     */
    @Test
    public void testGetDeviceRoutableEntityWithEmptyDmFeedbackTopic() {
        Mockito.when(spc.streamName()).thenReturn("ecall");
        Mockito.when(configResolver.getRetryInterval(Mockito.any(IgniteEventImpl.class)))
                .thenReturn(TestConstants.THREAD_SLEEP_TIME_1000);
        ReflectionTestUtils.setField(handlerChain, "deviceMessageFeedbackTopic", "");
        handlerChain.handle(key, igniteEvent);

        verify(msgValidator).handle(Mockito.any(IgniteKey.class), Mockito.any(DeviceMessage.class));
    }

    /**
     * Test get device routable entity for invalid retry interval.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetDeviceRoutableEntityForInvalidRetryInterval() {
        Mockito.when(spc.streamName()).thenReturn("ecall");
        Mockito.when(configResolver.getRetryInterval(Mockito.any(IgniteEventImpl.class))).thenReturn(0L);
        handlerChain.handle(key, igniteEvent);
    }

    /**
     * Test get device routable entity with emtpy stream name.
     */
    @Test(expected = RuntimeException.class)
    public void testGetDeviceRoutableEntityWithEmtpyStreamName() {
        Mockito.when(spc.streamName()).thenReturn("");
        handlerChain.handle(key, igniteEvent);
    }

    /**
     * Test setup with empty device message transformer.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetupWithEmptyDeviceMessageTransformer() {
        ReflectionTestUtils.setField(handlerChain, DEVICE_MESSAGING_EVENT_TRANSFORMER, "");
        handlerChain.setUp();
    }

    /**
     * Test setup with invalid device message transformer.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetupWithInvalidDeviceMessageTransformer() {
        ReflectionTestUtils.setField(handlerChain, DEVICE_MESSAGING_EVENT_TRANSFORMER,
                "org.eclipse.ecsp.analytics.InvalidTransformer");
        handlerChain.setUp();
    }

    /**
     * Test setup with invalid dma config resolver class.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetupWithInvalidDmaConfigResolverClass() {
        ReflectionTestUtils.setField(handlerChain, "dmaConfigResolverImplClass",
                "org.eclipse.ecsp.dma.InvalidConfigResolve");
        handlerChain.setUp();
    }

    /**
     * Test construct chain with empty stream processing context.
     */
    @Test(expected = RuntimeException.class)
    public void testConstructChainWithEmptyStreamProcessingContext() {
        handlerChain.constructChain("taskId", null);
    }

    /**
     * Test close method.
     */
    @Test
    public void testCloseMethod() {
        ReflectionTestUtils.setField(handlerChain, "connStatusHandler", connStatusHandler);
        ReflectionTestUtils.setField(handlerChain, "headerUpdater", headerUpdater);
        ReflectionTestUtils.setField(handlerChain, "retryHandler", retryHandler);
        ReflectionTestUtils.setField(handlerChain, "dispatchHandler", dispatchHandler);
        ReflectionTestUtils.setField(handlerChain, "dmaPostDispatchHandler", dmaPostDispatchHandler);

        handlerChain.close();
        verify(msgValidator).close();
        verify(dispatchHandler).close();
    }

    /**
     * Test construct chain method.
     */
    @Test
    public void testConstructChainMethod() {
        ReflectionTestUtils.setField(handlerChain, "connStatusHandler", connStatusHandler);
        ReflectionTestUtils.setField(handlerChain, "headerUpdater", headerUpdater);
        ReflectionTestUtils.setField(handlerChain, "retryHandler", retryHandler);
        ReflectionTestUtils.setField(handlerChain, "dispatchHandler", dispatchHandler);
        ReflectionTestUtils.setField(handlerChain, "dmaPostDispatchHandler", dmaPostDispatchHandler);

        handlerChain.constructChain("one", spc);
        verify(connStatusHandler).setNextHandler(any());
        verify(retryHandler).setNextHandler(any());
        verify(headerUpdater).setNextHandler(any());
        verify(dispatchHandler).setNextHandler(any());
        verify(msgValidator).setNextHandler(any());

    }
}
