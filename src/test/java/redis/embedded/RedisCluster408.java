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

package redis.embedded;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;



/**
 * class RedisCluster408 extends RedisCluster.
 */
public class RedisCluster408 extends RedisCluster {

    /**
     * Instantiates a new redis cluster 408.
     *
     * @param rc the rc
     */
    public RedisCluster408(RedisCluster rc) {
        this(rc.sentinels(), rc.servers());
    }

    /**
     * Instantiates a new redis cluster 408.
     *
     * @param sentinels the sentinels
     * @param servers the servers
     */
    public RedisCluster408(List<Redis> sentinels, List<Redis> servers) {
        super(transformSentinels(sentinels), transformServers(servers));
    }

    /**
     * Transform servers.
     *
     * @param servers the servers
     * @return the list
     */
    private static List<Redis> transformServers(List<Redis> servers) {
        return servers.stream().map(s -> createNewRedisServer408(s)).collect(Collectors.toList());
    }

    /**
     * Creates the new redis server 408.
     *
     * @param s the s
     * @return the redis server 408
     */
    private static RedisServer408 createNewRedisServer408(Redis s) {
        try {
            return new RedisServer408((RedisServer) s);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Transform sentinels.
     *
     * @param sentinels the sentinels
     * @return the list
     */
    private static List<Redis> transformSentinels(List<Redis> sentinels) {
        return sentinels.stream().map(s -> new RedisSentinel408((RedisSentinel) s)).collect(Collectors.toList());
    }
}
