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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Loads properties from the given source.
 * If the given source exists in both classpath and filesystem then first
 * classpath is loaded followed by what is in the filesystem.
 * This is followed by all system properties followed by all system env.
 * Subsequent sources override the earlier sources. So System Env
 * overrides the earlier sources.
 *
 */
public class SimplePropertiesLoader {
    /**
     * Loads properties from the given source.
     * If the given source exists in both classpath and filesystem then first
     * classpath is loaded followed by what is in the filesystem.
     * This is followed by all system properties followed by all system env.
     * Subsequent sources override the earlier sources. So System Env
     * overrides the earlier sources.
     *
     * @param source - an entry in the classpath and/or filesystem
     * @return Properties
     * @throws IOException IOException
     */
    public Properties loadProperties(String source) throws IOException {
        Properties props = new Properties();
        if (null != source) {
            InputStream is = getClass().getResourceAsStream(source);
            if (is != null) {
                props.load(is);
                is.close();
            }
            File f = new File(source);
            if (f.exists() && f.isFile()) {
                try (InputStream fis = new BufferedInputStream(new FileInputStream(f))) {
                    Properties nprops = new Properties();
                    nprops.load(fis);
                    nprops.forEach((k, v) -> props.setProperty((String) k, (String) v));
                }
            }
            if (props.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid properties source specified. "
                                + "The file could not be found neither in the classpath nor the filesystem.");
            }
        }
        System.getProperties().forEach((k, v) -> props.setProperty((String) k, (String) v));
        System.getenv().forEach(props::setProperty);
        return props;
    }

}
