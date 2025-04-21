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

package org.eclipse.ecsp.analytics.stream.base.kafka.internal;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.State;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;


/**
 * Test class to test the BackDoorKafkaConsumer.
 */
public class BackDoorKafkaConsumerMockTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    
    /** The topic. */
    String topic = "testtopic";
    
    /** The backdoor kafka consumer. */
    @InjectMocks
    private TestBackDoorKafkaConsumer backdoorKafkaConsumer = new TestBackDoorKafkaConsumer();
    
    /** The topic offset dao. */
    @Mock
    private BackdoorKafkaTopicOffsetDAOMongoImpl topicOffsetDao;
    
    /** The consumer. */
    @Mock
    private KafkaConsumer<byte[], byte[]> consumer;
    
    /** The admin client. */
    @Mock
    private AdminClient adminClient;
    
    /** The kafka consumer run executor. */
    @Mock
    private ExecutorService kafkaConsumerRunExecutor;
    
    /** The offsets mgmt executor. */
    @Mock
    private ScheduledExecutorService offsetsMgmtExecutor;

    /**
     * Subclasses should invoke this method in their @Before.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        backdoorKafkaConsumer.setConsumer(consumer);
        backdoorKafkaConsumer.setKafkaConsumerTopic(topic);
        backdoorKafkaConsumer.addCallback(new TestCallBack(), 0);
        backdoorKafkaConsumer.addCallback(new TestCallBack(), 1);
    }

    /**
     * Test remove consumer group.
     */
    @Test
    public void testRemoveConsumerGroup() {
        backdoorKafkaConsumer.getClosed().set(false);
        backdoorKafkaConsumer.getStartedConsumer().set(true);
        backdoorKafkaConsumer.setKafkaAdminClient(adminClient);
        backdoorKafkaConsumer.setOffsetsMgmtExecutor(offsetsMgmtExecutor);
        backdoorKafkaConsumer.setKafkaConsumerRunExecutor(kafkaConsumerRunExecutor);
        backdoorKafkaConsumer.shutdown();
        Mockito.verify(adminClient, Mockito.times(1)).deleteConsumerGroups(Mockito.anyList());
    }

    /**
     * Test back door health monitor.
     */
    @Test
    public void testBackDoorHealthMonitor() {
        Assert.assertFalse(backdoorKafkaConsumer.needsRestartOnFailure());
        Assert.assertFalse(backdoorKafkaConsumer.isEnabled());
        Assert.assertEquals("TestBackDoorKafkaConsumerMonitor", backdoorKafkaConsumer.monitorName());
        Assert.assertEquals("TestBackDoorKafkaConsumerGuage", backdoorKafkaConsumer.metricName());
    }

    /**
     * Test reset kafka consumer offset.
     */
    @Test
    public void testResetKafkaConsumerOffset() {
        List<BackdoorKafkaTopicOffset> topicOffsetList = new ArrayList<BackdoorKafkaTopicOffset>();
        BackdoorKafkaTopicOffset bkto1 = new BackdoorKafkaTopicOffset(topic, 0, TestConstants.THREAD_SLEEP_TIME_1000);
        BackdoorKafkaTopicOffset bkto2 = new BackdoorKafkaTopicOffset(topic, 1, TestConstants.THREAD_SLEEP_TIME_2000);
        topicOffsetList.add(bkto1);
        topicOffsetList.add(bkto2);
        Mockito.when(topicOffsetDao.getTopicOffsetList(topic))
                .thenReturn(topicOffsetList);

        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        Node leader = new Node(1, "host", TestConstants.HOST);
        Node[] replicas = new Node[1];
        replicas[0] = leader;
        PartitionInfo p1 = new PartitionInfo(topic, 0, leader,
                replicas, replicas, replicas);
        TopicPartition tp1 = new TopicPartition(topic, 0);

        PartitionInfo p2 = new PartitionInfo(topic, 1, leader,
                replicas, replicas, replicas);

        partitions.add(p1);
        partitions.add(p2);

        TopicPartition tp2 = new TopicPartition(topic, 1);
        List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
        topicPartitions.add(tp1);
        topicPartitions.add(tp2);

        Mockito.when(consumer.partitionsFor(topic))
                .thenReturn(partitions);

        Map<TopicPartition, Long> endOffsetMap = new HashMap<TopicPartition, Long>();
        endOffsetMap.put(tp1, TestConstants.THREAD_SLEEP_TIME_1500);
        endOffsetMap.put(tp2, TestConstants.THREAD_SLEEP_TIME_2500);

        Map<TopicPartition, Long> beginningOffsetMap = new HashMap<TopicPartition, Long>();
        beginningOffsetMap.put(tp1, TestConstants.THREAD_SLEEP_TIME_500);
        beginningOffsetMap.put(tp2, TestConstants.THREAD_SLEEP_TIME_1500);

        Mockito.when(consumer.endOffsets(topicPartitions))
                .thenReturn(endOffsetMap);
        Mockito.when(consumer.beginningOffsets(topicPartitions))
                .thenReturn(beginningOffsetMap);

        backdoorKafkaConsumer.resetKafkaConsumerOffset();

        Mockito.verify(consumer, Mockito.times(1)).seek(tp1, TestConstants.THREAD_SLEEP_TIME_1000);
        Mockito.verify(consumer, Mockito.times(1)).seek(tp2, TestConstants.THREAD_SLEEP_TIME_2000);

    }

    /**
     * Test reset kafka consumer offset to beginning.
     */
    @Test
    public void testResetKafkaConsumerOffsetToBeginning() {
        List<BackdoorKafkaTopicOffset> topicOffsetList = new ArrayList<BackdoorKafkaTopicOffset>();
        BackdoorKafkaTopicOffset bkto1 = new BackdoorKafkaTopicOffset(topic, 0, TestConstants.THREAD_SLEEP_TIME_100);
        BackdoorKafkaTopicOffset bkto2 = new BackdoorKafkaTopicOffset(topic, 1, TestConstants.THREAD_SLEEP_TIME_200);
        topicOffsetList.add(bkto1);
        topicOffsetList.add(bkto2);
        Mockito.when(topicOffsetDao.getTopicOffsetList(topic))
                .thenReturn(topicOffsetList);

        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        Node leader = new Node(1, "host", TestConstants.HOST);
        Node[] replicas = new Node[1];
        replicas[0] = leader;
        PartitionInfo p1 = new PartitionInfo(topic, 0, leader,
                replicas, replicas, replicas);
        TopicPartition tp1 = new TopicPartition(topic, 0);

        PartitionInfo p2 = new PartitionInfo(topic, 1, leader,
                replicas, replicas, replicas);
        partitions.add(p1);
        partitions.add(p2);
        List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
        topicPartitions.add(tp1);
        TopicPartition tp2 = new TopicPartition(topic, 1);
        topicPartitions.add(tp2);

        Mockito.when(consumer.partitionsFor(topic))
                .thenReturn(partitions);

        Map<TopicPartition, Long> endOffsetMap = new HashMap<TopicPartition, Long>();
        endOffsetMap.put(tp1, TestConstants.THREAD_SLEEP_TIME_1500);
        endOffsetMap.put(tp2, TestConstants.THREAD_SLEEP_TIME_2500);

        Map<TopicPartition, Long> beginningOffsetMap = new HashMap<TopicPartition, Long>();
        beginningOffsetMap.put(tp1, TestConstants.THREAD_SLEEP_TIME_500);
        beginningOffsetMap.put(tp2, TestConstants.THREAD_SLEEP_TIME_1500);

        Mockito.when(consumer.endOffsets(topicPartitions))
                .thenReturn(endOffsetMap);
        Mockito.when(consumer.beginningOffsets(topicPartitions))
                .thenReturn(beginningOffsetMap);

        backdoorKafkaConsumer.resetKafkaConsumerOffset();

        Mockito.verify(consumer, Mockito.times(1)).seekToBeginning(Collections.singletonList(tp1));
        Mockito.verify(consumer, Mockito.times(1)).seekToBeginning(Collections.singletonList(tp2));

    }

    /**
     * Test reset kafka consumer offset to end.
     */
    @Test
    public void testResetKafkaConsumerOffsetToEnd() {
        List<BackdoorKafkaTopicOffset> topicOffsetList = new ArrayList<BackdoorKafkaTopicOffset>();

        Mockito.when(topicOffsetDao.getTopicOffsetList(topic))
                .thenReturn(topicOffsetList);

        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        Node leader = new Node(1, "host", TestConstants.HOST);
        Node[] replicas = new Node[1];
        replicas[0] = leader;
        PartitionInfo p1 = new PartitionInfo(topic, 0, leader,
                replicas, replicas, replicas);
        TopicPartition tp1 = new TopicPartition(topic, 0);
        PartitionInfo p2 = new PartitionInfo(topic, 1, leader,
                replicas, replicas, replicas);
        partitions.add(p1);
        partitions.add(p2);
        List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
        TopicPartition tp2 = new TopicPartition(topic, 1);
        topicPartitions.add(tp1);
        topicPartitions.add(tp2);

        Mockito.when(consumer.partitionsFor(topic))
                .thenReturn(partitions);

        Map<TopicPartition, Long> endOffsetMap = new HashMap<TopicPartition, Long>();
        endOffsetMap.put(tp1, TestConstants.THREAD_SLEEP_TIME_1500);
        endOffsetMap.put(tp2, TestConstants.THREAD_SLEEP_TIME_2500);

        Map<TopicPartition, Long> beginningOffsetMap = new HashMap<TopicPartition, Long>();
        beginningOffsetMap.put(tp1, TestConstants.THREAD_SLEEP_TIME_500);
        beginningOffsetMap.put(tp2, TestConstants.THREAD_SLEEP_TIME_1500);

        Mockito.when(consumer.endOffsets(topicPartitions))
                .thenReturn(endOffsetMap);
        Mockito.when(consumer.beginningOffsets(topicPartitions))
                .thenReturn(beginningOffsetMap);

        backdoorKafkaConsumer.resetKafkaConsumerOffset();

        Mockito.verify(consumer, Mockito.times(1)).seekToEnd(Collections.singletonList(tp1));
        Mockito.verify(consumer, Mockito.times(1)).seekToEnd(Collections.singletonList(tp2));

    }

    /**
     * The Class TestCallBack.
     */
    class TestCallBack implements BackdoorKafkaConsumerCallback {

        /** The key. */
        private String key;
        
        /** The value. */
        private IgniteEvent value;

        /**
         * Gets the key.
         *
         * @return the key
         */
        public String getKey() {
            return key;
        }

        /**
         * Gets the value.
         *
         * @return the value
         */
        public IgniteEvent getValue() {
            return value;
        }

        /**
         * Process.
         *
         * @param key the key
         * @param value the value
         * @param meta the meta
         */
        @Override
        public void process(IgniteKey key, IgniteEvent value, OffsetMetadata meta) {
            this.key = key.getKey().toString();
            this.value = value;
        }

        /**
         * Gets the committable offset.
         *
         * @return the committable offset
         */
        @Override
        public Optional<OffsetMetadata> getCommittableOffset() {
            return Optional.empty();
        }

    }

    /**
     * The Class TestBackDoorKafkaConsumer.
     */
    private class TestBackDoorKafkaConsumer extends BackdoorKafkaConsumer {

        /** The resetcomplete. */
        private boolean resetcomplete = false;
        
        /** The streamstate. */
        private State streamstate = State.RUNNING;

        /**
         * Gets the name.
         *
         * @return the name
         */
        @Override
        public String getName() {
            return "test";
        }

        /**
         * Gets the kafka consumer group id.
         *
         * @return the kafka consumer group id
         */
        @Override
        public String getKafkaConsumerGroupId() {
            return "testGrpId";
        }

        /**
         * Gets the kafka consumer topic.
         *
         * @return the kafka consumer topic
         */
        @Override
        public String getKafkaConsumerTopic() {
            return topic;
        }

        /**
         * Gets the poll.
         *
         * @return the poll
         */
        @Override
        public long getPoll() {
            return TestConstants.THREAD_SLEEP_TIME_10;
        }

        /**
         * Checks if is offsets reset complete.
         *
         * @return true, if is offsets reset complete
         */
        @Override
        public boolean isOffsetsResetComplete() {
            return resetcomplete;
        }

        /**
         * Sets the reset offsets.
         *
         * @param reset the new reset offsets
         */
        @Override
        public void setResetOffsets(boolean reset) {
            resetcomplete = reset;

        }

        /**
         * Gets the stream state.
         *
         * @return the stream state
         */
        @Override
        public State getStreamState() {
            return streamstate;
        }

        /**
         * Sets the stream state.
         *
         * @param newState the new stream state
         */
        @Override
        public void setStreamState(State newState) {
            streamstate = newState;

        }

        /**
         * Monitor name.
         *
         * @return the string
         */
        @Override
        public String monitorName() {
            return "TestBackDoorKafkaConsumerMonitor";
        }

        /**
         * Needs restart on failure.
         *
         * @return true, if successful
         */
        @Override
        public boolean needsRestartOnFailure() {
            return false;
        }

        /**
         * Metric name.
         *
         * @return the string
         */
        @Override
        public String metricName() {
            return "TestBackDoorKafkaConsumerGuage";
        }

        /**
         * Checks if is enabled.
         *
         * @return true, if is enabled
         */
        @Override
        public boolean isEnabled() {
            return false;
        }

    }
}
