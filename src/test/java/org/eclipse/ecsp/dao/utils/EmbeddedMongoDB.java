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

package org.eclipse.ecsp.dao.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.eclipse.ecsp.nosqldao.spring.config.AbstractIgniteDAOMongoConfig;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;

import java.util.Map;

/**
 * class EmbeddedMongoDB extends ExternalResource.
 */
public class EmbeddedMongoDB extends ExternalResource {

    /** The Constant LOGGER. */
    private static final IgniteLogger LOGGER = IgniteLoggerFactory.getLogger(EmbeddedMongoDB.class);
    
    @Container
    MongoDBContainer mongoDbContainer = new MongoDBContainer("mongo:6.0.13");
    
    /**
     * Before.
     *
     * @throws Throwable the throwable
     */
    @Override
    protected void before() throws Throwable {
        mongoDbContainer.start();
        LOGGER.info("Embedded mongo DB started on address {} ", mongoDbContainer.getHost());
        AbstractIgniteDAOMongoConfig.overridingPort = mongoDbContainer.getFirstMappedPort();

        LOGGER.info("MongoClient connecting for pre-work DB configuration...");
        try (MongoClient mongoClient = MongoClients.create(mongoDbContainer.getConnectionString())) {
            Map<String, Object> commandArguments = new BasicDBObject();
            commandArguments.put("createUser", "admin");
            commandArguments.put("pwd", "dummyPass");
            String[] roles = { "readWrite" };
            commandArguments.put("roles", roles);
            MongoDatabase adminDatabase = mongoClient.getDatabase("admin");
            BasicDBObject command = new BasicDBObject(commandArguments);
            adminDatabase.runCommand(command);
        }
    }

    /**
     * After.
     */
    @Override
    protected void after() {
        if (mongoDbContainer.isCreated()) {
            mongoDbContainer.stop();
        }
    }
}