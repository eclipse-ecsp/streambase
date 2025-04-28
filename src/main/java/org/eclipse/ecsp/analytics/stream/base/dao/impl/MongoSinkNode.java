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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.dao.SinkNode;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Implementation for Mongo DB as sink node.
 *
 * @see SinkNode
 */
public class MongoSinkNode implements SinkNode<String, String> {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(MongoSinkNode.class);

    /** The mongoDB port. */
    private int port = 0;
    
    /** The connection url. */
    private String url = "";
    
    /** MongoDB username. */
    private String username = "";
    
    /** MongoDB password. */
    private char[] password = new char[50];
    
    /** The db access. */
    private String dbAccess = "";
    
    /** The db name. */
    private String dbName = "";
    
    /** The {@link Datastore}. */
    private Datastore datastore = null;
    
    /** The properties instance. */
    Properties props;
    
    /** The max connection. */
    private int maxConnection = 50;

    /**
     * Connects to Mongo DB.
     */
    private void connect() {
        MongoClient mongoClient = null;
        try {
            logger.info("Connecting to MongoDB");
            MongoCredential credential = MongoCredential.createCredential(username, dbAccess, password);
            MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder();
            mongoClientSettingsBuilder
                    .applyToConnectionPoolSettings(builder -> {
                        builder.maxConnecting(maxConnection);
                        builder.maxWaitTime(Integer.parseInt(props.getProperty(
                                        PropertyNames.MONGO_CLIENT_MAX_WAIT_TIME_MS)),
                                TimeUnit.MILLISECONDS);
                    }).applyToSocketSettings(builder -> {
                        builder.connectTimeout(Integer.parseInt(props.getProperty(
                                PropertyNames.MONGO_CLIENT_CONNECTION_TIMEOUT_MS)),
                                TimeUnit.MILLISECONDS);
                        builder.readTimeout(Integer.parseInt(props.getProperty(
                                PropertyNames.MONGO_CLIENT_SOCKET_TIMEOUT_MS)),
                                TimeUnit.MILLISECONDS);
                    }).readPreference(ReadPreference.secondaryPreferred())
                    .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress(url, port)))
                    ).codecRegistry(MongoClientSettings.getDefaultCodecRegistry()).credential(credential);
            logger.info("MongoDB connection strings. URL {} and port {}", url, port);
            mongoClient = MongoClients.create(mongoClientSettingsBuilder.build());
            Calendar endTime = Calendar.getInstance();
            Calendar startTime = Calendar.getInstance();
            logger.info("Inititalizing the mongodb and time taken is: {}",
                    endTime.getTimeInMillis() - startTime.getTimeInMillis());

            // create the Datastore connecting to the default port on the local
            // host
            this.datastore = Morphia.createDatastore(mongoClient, dbName);
            //
            // tell Morphia where to find your classes
            this.datastore.getMapper().mapPackage("org.eclipse.ecsp.haa.pulse.entity");
            this.datastore.getMapper().mapPackage("org.eclipse.ecsp.haa.pulse.domain");
            Calendar endTimeMorpia = Calendar.getInstance();
            logger.debug("Connection time taken from Morphia : "
                    + (endTimeMorpia.getTimeInMillis() - endTime.getTimeInMillis()));
        } catch (Exception e) {
            if (mongoClient != null) {
                mongoClient.close();
                logger.debug("DB Connection closed");
            } else {
                logger.debug("DB Connection already closed");
            }
            logger.info(" MongoDB exception " + e);
            this.datastore = null;
        }
    }

    /**
     * Gets the data store.
     *
     * @return the data store
     */
    public Datastore getDataStore() {
        return this.datastore;
    }

    /**
     * Initializes the configuration for connection to MongoDB.
     *
     * @param prop the properties instance to get the supplied config values from.
     */
    @Override
    public void init(Properties prop) {
        this.props = prop;
        url = props.getProperty(PropertyNames.MONGODB_URL);
        port = Integer.parseInt(props.getProperty(PropertyNames.MONGODB_PORT));
        username = props.getProperty(PropertyNames.MONGODB_AUTH_USERNAME);
        password = props.getProperty(PropertyNames.MONGODB_AUTH_PSWD).toCharArray();
        dbAccess = props.getProperty(PropertyNames.MONGODB_AUTH_DB);
        dbName = props.getProperty(PropertyNames.MONGODB_DBNAME);
        maxConnection = Integer.parseInt(props.getProperty(PropertyNames.MONGODB_POOL_MAX_SIZE));

        connect();
    }

    /**
     * Gets the record from a specified collection.
     *
     * @param primaryKeyValue the primary key value
     * @param fieldName the field name
     * @param tableName the table / collection name
     * @return the optional
     */
    @Override
    public Optional<String> get(String primaryKeyValue, String fieldName, String tableName) {
        return Optional.empty();
    }

    /**
     * Puts the record into the specified collection.
     *
     * @param id the id
     * @param value the value
     * @param tableName the table name
     * @param primaryKeyMapping the primary key mapping
     */
    @Override
    public void put(String id, String value, String tableName, String primaryKeyMapping) {
    //overridden method of SinkNode class
    }

}
