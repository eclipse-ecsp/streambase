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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.discovery.PropBasedDiscoveryServiceImpl;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;



/**
 * class {@link TestStreamProcessorIntegrationTesting} extends {@link KafkaStreamsApplicationTestBase}. 
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/integration-test-application.properties")
public class TestStreamProcessorIntegrationTesting extends KafkaStreamsApplicationTestBase {

    /** The source topic name. */
    private static String sourceTopicName;
    
    /** The sink topic name. */
    private static String sinkTopicName;
    
    /** The i. */
    private static int i = 0;

    /** The key. */
    String key = "Device123";

    /**
     * Setup.
     *
     * @throws Exception the exception
     */
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        i++;
        sourceTopicName = "sourceTopic" + i;
        sinkTopicName = "sinkTopic" + i;
        sinkTopicName = "sinkTopic" + i;
        createTopics(sourceTopicName, sinkTopicName);

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                Serdes.String().deserializer().getClass().getName());

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());

        ksProps.put(PropertyNames.DISCOVERY_SERVICE_IMPL, PropBasedDiscoveryServiceImpl.class.getName());
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopicName);
        ksProps.put("sink.topic.name", sinkTopicName);
        ksProps.put(PropertyNames.APPLICATION_ID, "pt");
    }

    /**
     *  testTestBasicStreamProcessor().
     *
     * @throws Exception Exception
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     * @throws TimeoutException TimeoutException
     * @throws JSONException JSONException
     */
    @org.junit.Test
    public void testTestBasicStreamProcessor()
            throws Exception, ExecutionException, InterruptedException, TimeoutException, JSONException {


        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, TestStreamProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());

        launchApplication();

        // Using Message Generator send the data to the Kafka.
        String[] args = new String[Constants.FIVE];
        args[0] = sourceTopicName;
        args[1] = key;
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0}}";
        args[Constants.TWO] = speedEvent;
        args[Constants.THREE] = KAFKA_CLUSTER.bootstrapServers();
        args[Constants.FOUR] = "false";
        MessageGenerator.produce(args);
        runAsync(() -> {}, delayedExecutor(TestConstants.THREAD_SLEEP_TIME_10000, MILLISECONDS)).join();
        List<String[]> messages = KafkaTestUtils.getMessages(sinkTopicName, consumerProps, 1, 
                Constants.THREAD_SLEEP_TIME_10000);
        Assert.assertEquals(key, messages.get(0)[0]);

        // [Device123,
        // {"EventId":"Speed","Version":"1.0","Timestamp":0,"Data":{"value":20.0},
        // "RequestId":"Device123-id","SourceDeviceId":"Device123"}]
        String expectedValue = "{\"EventID\":\"Speed\",\"Version\":\"1.0\",\"Timestamp\":0,\"Data\":"
                + "{\"value\":20.0},\"RequestId\":\"Device123-id\",\"SourceDeviceId\":\"Device123\"}";
        JSONAssert.assertEquals(expectedValue, messages.get(0)[1], false);
        shutDownApplication();

    }

    /**
     * This stream processor forwards messages to same topic multiple times as well as to multiple topics.
     *
     * @throws Exception the exception
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     * @throws TimeoutException TimeoutException
     * @throws JSONException JSONException
     */
    @Test
    public void testStreamProcessorWithMultipleForwardsToMultipleTopics()
            throws Exception, ExecutionException, InterruptedException, TimeoutException, JSONException {

        String newSinkTopic = "sink2";
        ksProps.put("sink.topic.name", sinkTopicName + "," + newSinkTopic);
        ksProps.put(PropertyNames.PRE_PROCESSORS,
                "org.eclipse.ecsp.analytics.stream.base.processors.TaskContextInitializer,"
                        + "org.eclipse.ecsp.analytics.stream.base.processors.ProtocolTranslatorPreProcessor");
        ksProps.put(PropertyNames.POST_PROCESSORS,
                "org.eclipse.ecsp.analytics.stream.base.processors.DeviceMessagingAgentPostProcessor,"
                        + "org.eclipse.ecsp.analytics.stream.base.processors.ProtocolTranslatorPostProcessor");
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, TestStreamProcessorMultiForwards.class.getName());

        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());

        launchApplication();

        // Using Message Generator send the data to the Kafka.
        String[] args = new String[Constants.FIVE];
        args[0] = sourceTopicName;
        args[1] = key;
        String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0}}";
        args[Constants.TWO] = speedEvent;
        args[Constants.THREE] = KAFKA_CLUSTER.bootstrapServers();
        args[Constants.FOUR] = "false";
        MessageGenerator.produce(args);
        runAsync(() -> {}, delayedExecutor(TestConstants.THREAD_SLEEP_TIME_10000, MILLISECONDS)).join();
        List<String[]> messages = KafkaTestUtils.getMessages(sinkTopicName, consumerProps, 1, 
                Constants.THREAD_SLEEP_TIME_10000);

        // we should get 3 msgs because we did 3 times forward
        Assert.assertEquals(Constants.THREE, messages.size());

        // [Device123,
        // {"EventId":"Speed","Version":"1.0","Timestamp":0,"Data":{"value":20.0},
        // "RequestId":"Device123-id","SourceDeviceId":"Device123"}]
        String expectedValue = "{\"EventID\":\"Speed\",\"Version\":\"1.0\",\"Timestamp\":0,\"Data\":"
                + "{\"value\":20.0},\"RequestId\":\"Device123-id\",\"SourceDeviceId\":\"Device123\"}";

        JSONAssert.assertEquals(expectedValue, messages.get(0)[1], false);
        JSONAssert.assertEquals(expectedValue, messages.get(1)[1], false);
        JSONAssert.assertEquals(expectedValue, messages.get(Constants.TWO)[1], false);

        messages = KafkaTestUtils.getMessages(newSinkTopic, consumerProps, 1, Constants.THREAD_SLEEP_TIME_10000);

        // we should get 3 msgs because we did 3 times forward
        Assert.assertEquals(1, messages.size());

        // [Device123,
        // {"EventId":"Speed","Version":"1.0","Timestamp":0,"Data":{"value":20.0},
        // "RequestId":"Device123-id","SourceDeviceId":"Device123"}]
        expectedValue = "{\"EventID\":\"Speed\",\"Version\":\"1.0\",\"Timestamp\":0,\"Data\""
                + ":{\"value\":20.0},\"RequestId\":\"Device123-id\",\"SourceDeviceId\":\"Device123\"}";

        JSONAssert.assertEquals(expectedValue, messages.get(0)[1], false);
        shutDownApplication();

    }

}
