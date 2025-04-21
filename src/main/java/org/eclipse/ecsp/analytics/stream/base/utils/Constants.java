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

package org.eclipse.ecsp.analytics.stream.base.utils;

/**
 * Constants file.
 */
public class Constants {

    /**
     * Instantiates a new constants.
     */
    private Constants() {
        
    }

    /** The Constant ONE. */
    public static final int ONE = 1;
    
    /** The Constant NEGATIVE_ONE. */
    public static final int NEGATIVE_ONE = -1;
    
    /** The Constant COMMA. */
    public static final String COMMA = ",";
    
    /** The Constant FORWARD_SLASH. */
    public static final String FORWARD_SLASH = "/";
    
    /** The Constant HYPHEN. */
    public static final String HYPHEN = "-";
    
    /** The Constant UNDERSCORE. */
    public static final char UNDERSCORE = '_';
    
    /** The Constant DOT. */
    public static final char DOT = '.';
    
    /** The Constant CONSUMER_GROUP. */
    public static final String CONSUMER_GROUP = "consumer-group";
    
    /** The Constant DLQ_NULL_KEY. */
    public static final String DLQ_NULL_KEY = "DLQ-NULL-KEY-";
    
    /** The Constant TO_DEVICE. */
    public static final String TO_DEVICE = FORWARD_SLASH + "2d";
    
    /** The Constant KAFKA_TOPICS_HEALTH_MONITOR. */
    public static final String KAFKA_TOPICS_HEALTH_MONITOR = "KAFKA_TOPICS_HEALTH_MONITOR";
    
    /** The Constant KAFKA_TOPICS_METRIC_NAME. */
    public static final String KAFKA_TOPICS_METRIC_NAME = "KAFKA_TOPICS_HEALTH_GAUGE";
    
    /** The Constant DELIMITER. */
    public static final String DELIMITER = "\\|";
    
    /** The Constant FORCED_HEALTH_CHECK_DEVICE_ID. */
    public static final String FORCED_HEALTH_CHECK_DEVICE_ID = "testDevice123";
    
    /** The Constant FORCED_HEALTH_DEFAULT_TEST_TOPIC_NAME. */
    public static final String FORCED_HEALTH_DEFAULT_TEST_TOPIC_NAME = "test";
    
    /** The Constant ECU_TYPE_BROKER_TOPIC_DELIMETER. */
    public static final String ECU_TYPE_BROKER_TOPIC_DELIMETER = "#";
    
    /** The Constant ROCKSDB_PREFIX. */
    public static final String ROCKSDB_PREFIX = "rocksdb.";
    
    /** The Constant CACHE_BYPASS_REDIS_RETRY_THREAD. */
    public static final String CACHE_BYPASS_REDIS_RETRY_THREAD = "CacheBypassRedisRetryThread";
    
    /** The Constant PLATFORM_ID. */
    public static final String PLATFORM_ID = "platformId";
    
    /** The Constant PLAIN. */
    // 3rd party DFF Kafka endpoint constants
    public static final String PLAIN = "PLAIN";
    
    /** The Constant TRUE. */
    public static final String TRUE = "true";
    
    /** The Constant FALSE. */
    public static final String FALSE = "false";
    
    /** The Constant PAHO. */
    public static final String PAHO = "paho";
    
    /** The Constant FIFTY. */
    public static final int FIFTY = 50;
    
    /** The Constant THREAD_SLEEP_TIME_5000. */
    public static final int THREAD_SLEEP_TIME_5000 = 5000;
    
    /** The Constant THREAD_SLEEP_TIME_15000. */
    public static final int THREAD_SLEEP_TIME_15000 = 15000;
    
    /** The Constant THREAD_SLEEP_TIME_500. */
    public static final int THREAD_SLEEP_TIME_500 = 500;
    
    /** The Constant THREAD_SLEEP_TIME_4000. */
    public static final int THREAD_SLEEP_TIME_4000 = 4000;
    
    /** The Constant THREAD_SLEEP_TIME_400. */
    public static final int THREAD_SLEEP_TIME_400 = 400;
    
    /** The Constant THREAD_SLEEP_TIME_5900. */
    public static final int THREAD_SLEEP_TIME_5900 = 5900;
    
    /** The Constant THREAD_SLEEP_TIME_30000. */
    public static final int THREAD_SLEEP_TIME_30000 = 30000;
    
    /** The Constant THREAD_SLEEP_TIME_9000. */
    public static final int THREAD_SLEEP_TIME_9000 = 9000;
    
    /** The Constant THREAD_SLEEP_TIME_120000. */
    public static final int THREAD_SLEEP_TIME_120000 = 120000;
    
    /** The Constant THREAD_SLEEP_TIME_3000. */
    public static final int THREAD_SLEEP_TIME_3000 = 3000;
    
    /** The Constant THREAD_SLEEP_TIME_300. */
    public static final int THREAD_SLEEP_TIME_300 = 300;
    
    /** The Constant INT_30. */
    public static final int INT_30 = 30;
    
    /** The Constant THREAD_SLEEP_TIME_2500. */
    public static final int THREAD_SLEEP_TIME_2500 = 2500;
    
    /** The Constant THREAD_SLEEP_TIME_250. */
    public static final int THREAD_SLEEP_TIME_250 = 250;
    
    /** The Constant THREAD_SLEEP_TIME_2000. */
    public static final int THREAD_SLEEP_TIME_2000 = 2000;
    
    /** The Constant THREAD_SLEEP_TIME_1500. */
    public static final int THREAD_SLEEP_TIME_1500 = 1500;
    
    /** The Constant THREAD_SLEEP_TIME_1000. */
    public static final int THREAD_SLEEP_TIME_1000 = 1000;
    
    /** The Constant INT_1883. */
    public static final int INT_1883 = 1883;
    
    /** The Constant INT_18. */
    public static final int INT_18 = 18;
    
    /** The Constant INT_1000000. */
    public static final int INT_1000000 = 1000000;
    
    /** The Constant THREAD_SLEEP_TIME_1000000. */
    public static final long THREAD_SLEEP_TIME_1000000 = 1000000;
    
    /** The Constant THREAD_SLEEP_TIME_200. */
    public static final int THREAD_SLEEP_TIME_200 = 200;
    
    /** The Constant THREAD_SLEEP_TIME_100. */
    public static final int THREAD_SLEEP_TIME_100 = 100;
    
    /** The Constant THREAD_SLEEP_TIME_10000. */
    public static final int THREAD_SLEEP_TIME_10000 = 10000;
    
    /** The Constant INT_16. */
    public static final int INT_16 = 16;
    
    /** The Constant TWENTY_THOUSAND. */
    public static final long TWENTY_THOUSAND = 20000L;
    
    /** The Constant THIRTY_THOUSAND. */
    public static final long THIRTY_THOUSAND = 30000L;

    /** The Constant SIXTEEN. */
    public static final long SIXTEEN = 16;
    
    /** The Constant SIXTY. */
    public static final long SIXTY = 60;

    /** The Constant INT_45. */
    public static final long INT_45 = 45;
    
    /** The Constant THREAD_SLEEP_TIME_10. */
    public static final int THREAD_SLEEP_TIME_10 = 10;
    
    /** The Constant TWENTY. */
    public static final int TWENTY = 20;
    
    /** The Constant TEN. */
    public static final int TEN = 10;
    
    /** The Constant SEVEN. */
    public static final short SEVEN = 7;
    
    /** The Constant SIX. */
    public static final int SIX = 6;
    
    /** The Constant FIVE. */
    public static final int FIVE = 5;
    
    /** The Constant FOUR. */
    public static final int FOUR = 4;
    
    /** The Constant THREE. */
    public static final int THREE = 3;
    
    /** The Constant TWO. */
    public static final int TWO = 2;
    
    /** The Constant THREAD_SLEEP_TIME_6000. */
    public static final int THREAD_SLEEP_TIME_6000 = 6000;
    
    /** The Constant THREAD_SLEEP_TIME_60000. */
    public static final int THREAD_SLEEP_TIME_60000 = 60000;
    
    /** The Constant LONG_60000. */
    public static final long LONG_60000 = 60000L;
    
    /** The Constant HOST. */
    public static final int HOST = 1234;

    /** The Constant OFFSET_VALUE. */
    public static final short OFFSET_VALUE = 32;
    
    /** The Constant BYTE_1024. */
    public static final int BYTE_1024 = 1024;
    
    /** The Constant INT_4736565. */
    public static final long INT_4736565 = 4736565;
    
    /** The Constant INT_4792987. */
    public static final long INT_4792987 = 4792987;
    
    /** The Constant LONG_MINUS_ONE. */
    public static final long LONG_MINUS_ONE = -1;
    
    /** The Constant INT_MINUS_ONE. */
    public static final int INT_MINUS_ONE = -1;
    
    /** The Constant INT_MINUS_TWO. */
    public static final long INT_MINUS_TWO = -2;
    
    /** The Constant INT_202. */
    public static final int INT_202 = 202;
    
    /** The Constant INT_45000. */
    public static final int INT_45000 = 45000;
    
    /** The Constant INT_80000. */
    public static final int INT_80000 = 80000;
    
    /** The Constant INT_120000. */
    public static final int INT_120000 = 120000;
    
    /** The Constant INT_20000. */
    public static final int INT_20000 = 20000;
    
    /** The Constant INT_6144. */
    public static final int INT_6144 = 6144;
    
    /** The Constant INT_26379. */
    public static final int INT_26379 = 26379;
    
    /** The Constant INT_6379. */
    public static final int INT_6379 = 6379;
    
    /** The Constant FLOAT_DECIMAL75. */
    public static final float FLOAT_DECIMAL75 = 0.75f;
    
    /** The Constant LONG_1443717903851. */
    public static final long LONG_1443717903851 = 1443717903851L;
    
    /** The Constant LONG_1475151880125. */
    public static final long LONG_1475151880125 = 1475151880125L;
    
    /** The Constant LONG_111. */
    public static final long LONG_111 = 111L;
    
    /** The Constant INT_9092. */
    public static final int INT_9092 = 9092;
    
    /** The Constant SHORT_0_X_1_F_8_B. */
    public static final short SHORT_0_X_1_F_8_B = 0x1f8b;
    
    /** The Constant SHORT_0_X_1_F_9_D. */
    public static final short SHORT_0_X_1_F_9_D = 0x1f9d;
    
    /** The Constant SHORT_0_X_425_A. */
    public static final short SHORT_0_X_425_A = 0x425a;
    
    /** The Constant SHORT_0_X_7801. */
    public static final short SHORT_0_X_7801 = 0x7801;
    
    /** The Constant SHORT_0_X_789_C. */
    public static final short SHORT_0_X_789_C = 0x789c;
    
    /** The Constant SHORT_0_X_78_DA. */
    public static final short SHORT_0_X_78_DA = 0x78da;
    
    /** The Constant SHORT_0_X_504_B. */
    public static final short SHORT_0_X_504_B = 0x504b;
    
    /** The Constant LONG_1603946935. */
    public static final long LONG_1603946935 = 1603946935L;
    
    /** The Constant THOUSAND. */
    public static final int THOUSAND = 1000;
    
    /** The Constant TWO_THOUSAND. */
    public static final int TWO_THOUSAND = 2000;
    
    /** The Constant HIVEMQ. */
    public static final String HIVEMQ = "hivemq";
    
    /** The Constant CANNOT_BE_NULL_ERROR_MSG. */
    public static final String CANNOT_BE_NULL_ERROR_MSG = " shouldn't be null or empty.";
    
    /** The Constant MQTT_CLIENT_AS_ONE_WAY_TLS. */
    public static final String MQTT_CLIENT_AS_ONE_WAY_TLS = " For mqtt client auth mechanism as one way tls";

    /** The Constant CANNOT_BE_EMPTY. */
    public static final String CANNOT_BE_EMPTY = " cannot be empty";
    
    /** The Constant FOR_PARTITION. */
    public static final String FOR_PARTITION = "For partition ";
    
    /** The Constant VALUE_END. */
    public static final String VALUE_END = "}";
    
    /** The Constant VALUE_START. */
    public static final String VALUE_START = "${";
    
    /** The Constant COLON_9100. */
    public static final String COLON_9100 = ":9100";
    
    /** The Constant RECEIVED_NULL_KEY. */
    public static final String RECEIVED_NULL_KEY = "Received null key.";
    
    /** The Constant DEFAULT_PLATFORMID. */
    public static final String DEFAULT_PLATFORMID = "default";
}