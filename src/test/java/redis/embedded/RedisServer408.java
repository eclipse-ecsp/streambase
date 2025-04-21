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


/**
 * Supporting class for Redis version 4.0.8.
 *
 * @author ssasidharan
 */
public class RedisServer408 extends RedisServer {

    /**
     * Instantiates a new redis server 408.
     *
     * @param redisExecProvider the redis exec provider
     * @param port the port
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public RedisServer408(RedisExecProvider redisExecProvider, Integer port) throws IOException {
        super(redisExecProvider, port);
    }

    /**
     * Instantiates a new redis server 408.
     *
     * @param r the r
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public RedisServer408(RedisServer r) throws IOException {
        super(r.ports().get(0));
        this.args = r.args;
    }

    /**
     * Redis ready pattern.
     *
     * @return the string
     */
    @Override
    protected String redisReadyPattern() {
        return ".*Ready to accept connections.*";
    }

}
