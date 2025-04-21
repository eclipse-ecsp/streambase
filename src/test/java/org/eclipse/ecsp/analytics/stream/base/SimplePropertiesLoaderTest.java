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

package org.eclipse.ecsp.analytics.stream.base;

import org.eclipse.ecsp.analytics.stream.base.SimplePropertiesLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * {@link SimplePropertiesLoaderTest} UT class for {@link SimplePropertiesLoader}.
 */
public class SimplePropertiesLoaderTest {

    /** The Constant CLASSPATH_RESOUCE_NAME. */
    private static final String CLASSPATH_RESOUCE_NAME = "application-base-test.properties";

    /** The Constant INVALID_RESOUCE_NAME. */
    private static final String INVALID_RESOUCE_NAME = "unknown";

    /** The Constant LOCALHOST_MQTT. */
    private static final String LOCALHOST_MQTT = "tcp://127.0.0.1:1883";

    /** The Constant MQTT_BROKER_URL. */
    private static final String MQTT_BROKER_URL = "mqtt.broker.url";

    /** The file path under filesystem. */
    private final String FILE_PATH_UNDER_FILESYSTEM =
            getClass().getClassLoader().getResource(CLASSPATH_RESOUCE_NAME).getPath();

    /** The simple properties loader. */
    private SimplePropertiesLoader simplePropertiesLoader;

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        simplePropertiesLoader = new SimplePropertiesLoader();
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception {
        simplePropertiesLoader = null;
    }

    /**
     * if source is not a valid file name then it will throw IllegalArgumentException.
     *
     * @throws IOException IOException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testLoadPropertiesForInvalidSource() throws IOException {
        simplePropertiesLoader.loadProperties(INVALID_RESOUCE_NAME);
    }

    /**
     * Only loading filesystem file.
     *
     * @throws IOException IOException
     */
    @Test
    public void testLoadPropertiesForVvalidSourceInFileSystem() throws IOException {
        Properties props = simplePropertiesLoader.loadProperties(FILE_PATH_UNDER_FILESYSTEM);
        assertNotNull(props);
        assertEquals(LOCALHOST_MQTT, props.getProperty(MQTT_BROKER_URL));
    }

    /**
     * Null check It should load only system property and environment variable.
     *
     * @throws IOException IOException
     */
    @Test
    public void testLoadPropertiesForSorceIsNull() throws IOException {
        Properties props = simplePropertiesLoader.loadProperties(null);
        assertNotNull(props);
        assertTrue(props.size() > 0);
    }

}
