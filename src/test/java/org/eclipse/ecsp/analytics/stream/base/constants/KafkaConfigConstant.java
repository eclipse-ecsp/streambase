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

package org.eclipse.ecsp.analytics.stream.base.constants;

/**
 * The KafkaConfigConstant class contains constants for Kafka configuration properties.
 */
public final class KafkaConfigConstant {

    private KafkaConfigConstant() {
        // Private constructor to prevent instantiation
    }

    /** The Constant BROKER_ID. */
    public static final String BROKER_ID = "broker.id";

    /** The Constant LISTENERS. */
    public static final String LISTENERS = "listeners";

    /** The Constant NUM_PARTITIONS. */
    public static final String NUM_PARTITIONS = "num.partitions";

    /** The Constant AUTO_CREATE_TOPICS_ENABLE. */
    public static final String AUTO_CREATE_TOPICS_ENABLE = "auto.create.topics.enable";

    /** The Constant MESSAGE_MAX_BYTES. */
    public static final String MESSAGE_MAX_BYTES = "message.max.bytes";

    /** The Constant CONTROLLED_SHUTDOWN_ENABLE. */
    public static final String CONTROLLED_SHUTDOWN_ENABLE = "controlled.shutdown.enable";

    /** The Constant LOG_DIR. */
    public static final String LOG_DIR = "log.dir";

    /** The Constant ZOOKEEPER_CONNECT. */
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";

    /** The Constant ZOOKEEPER_SESSION_TIMEOUT_MS. */
    public static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";

    /** The Constant ZOOKEEPER_CONNECTION_TIMEOUT_MS. */
    public static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";

    /** The Constant LOG_RETENTION_HOURS. */
    public static final String LOG_RETENTION_HOURS = "log.retention.hours";

    /** The Constant DELETE_TOPIC_ENABLE. */
    public static final String DELETE_TOPIC_ENABLE = "delete.topic.enable";

    /** The Constant LOG_CLEANER_DEDUPE_BUFFER_SIZE. */
    public static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE = "log.cleaner.dedupe.buffer.size";

    /** The Constant GROUP_MIN_SESSION_TIMEOUT_MS. */
    public static final String GROUP_MIN_SESSION_TIMEOUT_MS = "group.min.session.timeout.ms";

    /** The Constant OFFSETS_TOPIC_REPLICATION_FACTOR. */
    public static final String OFFSETS_TOPIC_REPLICATION_FACTOR = "offsets.topic.replication.factor";

    /** The Constant OFFSETS_TOPIC_NUM_PARTITIONS. */
    public static final String OFFSETS_TOPIC_NUM_PARTITIONS = "offsets.topic.num.partitions";
}
