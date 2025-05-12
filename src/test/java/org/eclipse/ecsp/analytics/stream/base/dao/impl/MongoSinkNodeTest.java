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

package org.eclipse.ecsp.analytics.stream.base.dao.impl;


import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.Properties;

/**
 * Test class to verify the functionalities of KafkaSinkNodeTest class.
 */
//@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Launcher.class)
@EnableRuleMigrationSupport
@TestPropertySource("/integration-test-application.properties")
public class MongoSinkNodeTest extends KafkaStreamsApplicationTestBase {

    /** The mongo sink node. */
    private final MongoSinkNode mongoSinkNode = new MongoSinkNode();

    /** The properties. */
    private Properties properties = new Properties();

    /**
     * setup().
     *
     * @throws Exception Exception
     */
    @Before
    public void setup() throws Exception {
        properties.setProperty(PropertyNames.MONGODB_URL, "localhost");
        properties.setProperty(PropertyNames.MONGODB_PORT, "12345");
        properties.setProperty(PropertyNames.MONGODB_AUTH_USERNAME, "ADMIN");
        properties.setProperty(PropertyNames.MONGODB_AUTH_PSWD, "password");
        properties.setProperty(PropertyNames.MONGODB_AUTH_DB, "ADMIN");
        properties.setProperty(PropertyNames.MONGODB_DBNAME, "ADMIN");
        properties.setProperty(PropertyNames.MONGODB_POOL_MAX_SIZE, "50");
        properties.setProperty(PropertyNames.MONGO_CLIENT_MAX_WAIT_TIME_MS, "30000");
        properties.setProperty(PropertyNames.MONGO_CLIENT_CONNECTION_TIMEOUT_MS, "20000");
        properties.setProperty(PropertyNames.MONGO_CLIENT_SOCKET_TIMEOUT_MS, "60000");
    }

    /**
     * Publising message to kafka and consume the same.
     */
    @Test
    public void testInit() {
        mongoSinkNode.init(properties);
        mongoSinkNode.get("a", "b", "c");
        mongoSinkNode.put("a", "b", "c", "d");
        Assertions.assertDoesNotThrow(() -> mongoSinkNode.deleteSingleRecord("a", "b"));
    }
}

