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

package org.eclipse.ecsp.analytics.stream.base.kafka.support;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TaskMetadataImpl;
import org.apache.kafka.streams.processor.internals.ThreadMetadataImpl;
import org.eclipse.ecsp.analytics.stream.base.kafka.support.KafkaStreamsThreadStatusPrinter;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;


/**
 * test class {@link KafkaStreamsThreadStatusPrinterTest}.
 */
public class KafkaStreamsThreadStatusPrinterTest {

    /** The mockito rule. */
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    /** The ks. */
    @Mock
    private KafkaStreams ks;

    /**
     * Test no exceptions.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testNoExceptions() throws InterruptedException {
        KafkaStreamsThreadStatusPrinter tsp = new KafkaStreamsThreadStatusPrinter();
        Mockito.when(ks.metadataForLocalThreads()).thenReturn(createThreadMetadata());
        tsp.init(ks);
        runAsync(() -> {}, delayedExecutor(Constants.THREAD_SLEEP_TIME_500, MILLISECONDS)).join();
        Mockito.verify(ks, Mockito.atLeastOnce()).metadataForLocalThreads();
        tsp.close();
    }

    /**
     * Creates the thread metadata.
     *
     * @return the sets the
     */
    private Set<ThreadMetadata> createThreadMetadata() {
        Set<TopicPartition> tps = new HashSet<>();
        tps.add(new TopicPartition("topic1", 1));
        Set<TaskMetadata> activeTasks = new HashSet<>();
        activeTasks.add(new TaskMetadataImpl(new TaskId(1, 1, "taskid1"), tps, Collections.emptyMap(), 
                Collections.emptyMap(), Optional.of(Long.valueOf(System.currentTimeMillis()))));
        Set<String> producerClientIds = new HashSet<>();
        producerClientIds.add("producerClientId");
        ThreadMetadata tm = new ThreadMetadataImpl("thread1", "active", "mainConsumerClientId", 
                "restoreConsumerClientId", producerClientIds, "adminClientId", activeTasks, Collections.emptySet());
        Set<ThreadMetadata> tms = new HashSet<>();
        tms.add(tm);
        return tms;
    }
}
