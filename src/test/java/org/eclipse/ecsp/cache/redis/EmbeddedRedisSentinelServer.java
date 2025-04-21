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
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.junit.rules.ExternalResource;
import redis.embedded.RedisCluster;
import redis.embedded.RedisCluster408;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisSentinel;
import redis.embedded.RedisServer;
import redis.embedded.util.Architecture;
import redis.embedded.util.OS;

import java.util.Arrays;


/**
 * class EmbeddedRedisSentinelServer extends ExternalResource.
 */
public class EmbeddedRedisSentinelServer extends ExternalResource {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(EmbeddedRedisSentinelServer.class);
    
    /** The redis. */
    private RedisCluster408 redis = null;

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
        int[] sentinelPorts = new int[Constants.THREE];
        PortScanner portScanner = new PortScanner();
        sentinelPorts[0] = portScanner.getAvailablePort(Constants.INT_26379);
        sentinelPorts[1] = portScanner.getAvailablePort(sentinelPorts[0] + 1);
        sentinelPorts[Constants.TWO] = portScanner.getAvailablePort(sentinelPorts[1] + 1);
        int[] serverPorts = new int[Constants.TWO];
        serverPorts[0] = portScanner.getAvailablePort(Constants.INT_6379);
        serverPorts[1] = portScanner.getAvailablePort(serverPorts[0] + 1);
        logger.info("Sentinel ports {}, {}, {}", sentinelPorts[0], sentinelPorts[1], sentinelPorts[Constants.TWO]);
        RedisCluster rc = RedisCluster.builder().withSentinelBuilder(RedisSentinel.builder()
                .redisExecProvider(igniteProvider))
                .withServerBuilder(RedisServer.builder().redisExecProvider(igniteProvider))
                .sentinelPorts(Arrays.asList(sentinelPorts[0],
                        sentinelPorts[1], sentinelPorts[Constants.TWO]))
                // .serverPorts(Arrays.asList(serverPorts[0], serverPorts[1]))
                .replicationGroup("mogambo", 1).build();
        redis = new RedisCluster408(rc);
        RedisConfig.overridingSentinelPorts = new Integer[] {
                sentinelPorts[0], sentinelPorts[1], sentinelPorts[Constants.TWO] };
        redis.start();
    }

    /**
     * After.
     */
    @Override
    protected void after() {
        redis.stop();
    }

}
