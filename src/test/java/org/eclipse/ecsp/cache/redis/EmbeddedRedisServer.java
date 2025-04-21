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

package org.eclipse.ecsp.cache.redis;

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.rules.ExternalResource;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer408;
import redis.embedded.util.Architecture;
import redis.embedded.util.OS;


/**
 * class {@link EmbeddedRedisServer} extends {@link ExternalResource}.
 */
public class EmbeddedRedisServer extends ExternalResource {
    
    /** The redis. */
    private RedisServer408 redis = null;
    
    /** The port. */
    private int port = 0;

    /**
     * Before.
     *
     * @throws Throwable the throwable
     */
    @Override
    protected void before() throws Throwable {
        RedisExecProvider igniteProvider = RedisExecProvider.defaultProvider();
        igniteProvider.override(OS.MAC_OS_X, Architecture.x86,
                "redis-server-4.0.8.app");
        igniteProvider.override(OS.MAC_OS_X, Architecture.x86_64,
                "redis-server-4.0.8.app");
        igniteProvider.override(OS.UNIX, Architecture.x86,
                "redis-server-4.0.8");
        igniteProvider.override(OS.UNIX, Architecture.x86_64,
                "redis-server-4.0.8");
        igniteProvider.override(OS.WINDOWS, Architecture.x86, "redis-server.exe");
        igniteProvider.override(OS.WINDOWS, Architecture.x86_64, "redis-server.exe");
        port = new PortScanner().getAvailablePort(Constants.INT_6379);
        redis = new RedisServer408(igniteProvider, port);
        redis.start();
        RedisConfig.overridingPort = port;
    }

    /**
     * After.
     */
    @Override
    protected void after() {
        redis.stop();
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

}
