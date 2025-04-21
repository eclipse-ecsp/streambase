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

package org.eclipse.ecsp.analytics.stream.base.healthcheck;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.eclipse.ecsp.analytics.stream.base.KafkaSslConfig;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * UT class {@link KafkaTopicsMonitorTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {KafkaTopicsHealthMonitor.class, KafkaSslConfig.class})
@TestPropertySource("/topics-health-monitor-test.properties")
public class KafkaTopicsMonitorTest {
       
    /** The topic. */
    private final String topic = "health";
    
    /** The boot strap server. */
    @Value("${" + PropertyNames.BOOTSTRAP_SERVERS + ":localhost:9092}")
    private String bootStrapServer;
    
    
    
    /** The ctx. */
    @Autowired
    ApplicationContext ctx;

    /** The spc. */
    @Mock
    StreamProcessingContext spc;
    
    /** The kafa topics monitor. */
    @InjectMocks
    private KafkaTopicsHealthMonitor kafaTopicsMonitor;
    
    /** The props. */
    private Properties props;
    
    /** The describe topics result. */
    @Mock
    private DescribeTopicsResult describeTopicsResult;

    /** The describe topics result failure. */
    @Mock
    private DescribeTopicsResult describeTopicsResultFailure;

    /** The admin. */
    @Mock
    private AdminClient admin;

    /**
     * setup().
     *
     * @throws Exception Exception
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        props = new Properties();
        props.put(PropertyNames.BOOTSTRAP_SERVERS, bootStrapServer);
        Set<String> topics = new HashSet<>();
        topics.add(topic);
        ReflectionTestUtils.setField(kafaTopicsMonitor, "topics", topics);
        
        Map<String, String[]> topicConfig = new HashMap<>();
        KafkaTopicsHealthMonitor monitor = ctx.getBean(KafkaTopicsHealthMonitor.class);
        topicConfig = ReflectionTestUtils.invokeMethod(monitor, "getTopicsConfig", new Object[0]);
        ReflectionTestUtils.setField(kafaTopicsMonitor, "topicConfig", topicConfig);
    }

    /**
     * Test to create a single feed in DB and verify there is no exception while saving the data to MongoDB.
     *
     * @throws Exception the exception
     */
    @Test
    public void testKafkaTopicsMonitor() throws Exception {

        Mockito.when(describeTopicsResult.topicNameValues()).thenReturn(getDescribeTopicsResult());
        Mockito.when(admin.describeTopics(Mockito.anyCollection())).thenReturn(describeTopicsResult);
        assertEquals(true, kafaTopicsMonitor.isHealthy(true));

        Mockito.when(describeTopicsResult.topicNameValues()).thenReturn(getDescribeTopicsResultFailure());
        Mockito.when(admin.describeTopics(Mockito.anyCollection())).thenReturn(describeTopicsResult);
        assertEquals(false, kafaTopicsMonitor.isHealthy(true));

    }

    /**
     * getDescribeTopicsResult().
     *
     * @return Map
     * @throws InterruptedException InterruptedException
     * @throws ExecutionException ExecutionException
     */
    private Map<String, KafkaFuture<TopicDescription>> getDescribeTopicsResult()
            throws InterruptedException, ExecutionException {
        Node node = new Node(Constants.INT_18, "localhost", Constants.INT_9092);
        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(1, node, Collections
                .singletonList(node), Collections.singletonList(node));

        TopicDescription topicDescription = new TopicDescription(topic, false,
                Collections.singletonList(topicPartitionInfo));

        KafkaFuture<TopicDescription> kf = KafkaFuture.completedFuture(topicDescription);

        Map<String, KafkaFuture<TopicDescription>> map = new HashMap<String, KafkaFuture<TopicDescription>>();
        map.put(topic, kf);

        return map;
    }

    /**
     * getDescribeTopicsResultFailure().
     *
     * @return map
     * @throws InterruptedException InterruptedException
     * @throws ExecutionException ExecutionException
     */
    private Map<String, KafkaFuture<TopicDescription>> getDescribeTopicsResultFailure()
            throws InterruptedException, ExecutionException {

        TopicDescription topicDescription = new TopicDescription(topic, false,
                Collections.emptyList());

        KafkaFuture<TopicDescription> kf = KafkaFuture.completedFuture(topicDescription);

        Map<String, KafkaFuture<TopicDescription>> map = new HashMap<String, KafkaFuture<TopicDescription>>();
        map.put(topic, kf);

        return map;
    }

    /**
     * Test scenario where partition leader is null.
     *
     * @throws Exception the exception
     */
    @Test
    public void testKafkaTopicsMontiorWithNullPartitionLeader() throws Exception {

        Mockito.when(describeTopicsResult.topicNameValues())
            .thenReturn(getDescribeTopicsResultWithNullPartitionLeader());
        Mockito.when(admin.describeTopics(Mockito.anyCollection())).thenReturn(describeTopicsResult);
        assertEquals(false, kafaTopicsMonitor.isHealthy(true));
    }

    /**
     *  getDescribeTopicsResultWithNullPartitionLeader().
     *
     * @return Map
     * @throws InterruptedException InterruptedException
     * @throws ExecutionException ExecutionException
     */
    private Map<String, KafkaFuture<TopicDescription>> getDescribeTopicsResultWithNullPartitionLeader()
            throws InterruptedException, ExecutionException {
        Node node = new Node(Constants.INT_18, "localhost", Constants.INT_9092);
        TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(1, null, Collections
                .singletonList(node), Collections.singletonList(node));

        TopicDescription topicDescription = new TopicDescription(topic, false,
                Collections.singletonList(topicPartitionInfo));

        KafkaFuture<TopicDescription> kf = KafkaFuture.completedFuture(topicDescription);

        Map<String, KafkaFuture<TopicDescription>> map = new HashMap<String, KafkaFuture<TopicDescription>>();
        map.put(topic, kf);

        return map;
    }
}
