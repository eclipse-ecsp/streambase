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

package org.eclipse.ecsp.analytics.stream.base.http;

import jakarta.annotation.PostConstruct;
import okhttp3.Authenticator;
import okhttp3.ConnectionPool;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Http client which uses for HTTP call.
 */
@Component
public class HttpClientFactory {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(HttpClientFactory.class);

    /** The connection timeout in sec. */
    @Value("${" + PropertyNames.HTTP_CONNECTION_TIMEOUT_IN_SEC + ":120}")
    private long connectionTimeoutInSec;

    /** The read timeout in sec. */
    @Value("${" + PropertyNames.HTTP_READ_TIMEOUT_IN_SEC + ":60}")
    private long readTimeoutInSec;

    /** The write timeout in sec. */
    @Value("${" + PropertyNames.HTTP_WRITE_TIMEOUT_IN_SEC + ":60}")
    private long writeTimeoutInSec;

    /** The keep alive duration in sec. */
    @Value("${" + PropertyNames.HTTP_KEEP_ALIVE_DURATION_IN_SEC + ":120}")
    private long keepAliveDurationInSec;

    /** The max idle connections. */
    @Value("${" + PropertyNames.HTTP_MAX_IDLE_CONNECTIONS + ":20}")
    private int maxIdleConnections;

    /** The http auth header. */
    @Value("${" + PropertyNames.HTTP_VP_SERVICE_AUTH_HEADER + ":Authorization}")
    private String httpAuthHeader;

    /** The http vp service user. */
    @Value("${" + PropertyNames.HTTP_VP_SERVICE_USER + ":}")
    private String httpVpServiceUser;

    /** The http vp service password. */
    @Value("${" + PropertyNames.HTTP_VP_SERVICE_PASSWORD + ":}")
    private String httpVpServicePassword;

    /** The connection pool. */
    private ConnectionPool connectionPool;

    /**
     * init().
     */
    @PostConstruct
    public void init() {
        connectionPool = new ConnectionPool(maxIdleConnections, keepAliveDurationInSec, TimeUnit.SECONDS);
        logger.info(
                "Connection pool of HTTPClient. connectionTimeoutInSec:{} readTimeoutInSec:{}"
                        + " writeTimeoutInSec:{} keepAliveDurationInSec:{} maxIdleConnections:{} "
                        + "httpAuthHeader:{} httpVPServiceUser:{} ",
                connectionTimeoutInSec, readTimeoutInSec, writeTimeoutInSec,
                keepAliveDurationInSec, maxIdleConnections, httpAuthHeader,
                httpVpServiceUser);
    }

    /**
     * It returns the OkHttpClient which supports HTTP protocol.
     * It internally usages the connectionpool which is set in init method.
     *
     * @param authRequired         - true, if authentication is required while connection to server
     * @param retryOnConnectionFailure the retry on connection failure
     * @return OkHttpClient
     */
    public OkHttpClient createDefaultHttpClient(Optional<Boolean> authRequired, boolean retryOnConnectionFailure) {
        OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
        builder.connectTimeout(connectionTimeoutInSec, TimeUnit.SECONDS)
                .readTimeout(readTimeoutInSec, TimeUnit.SECONDS).writeTimeout(writeTimeoutInSec, TimeUnit.SECONDS)
                .connectionPool(connectionPool)
                .retryOnConnectionFailure(retryOnConnectionFailure);
        if (authRequired.isPresent() && Boolean.TRUE.equals(authRequired.get())) {
            builder.authenticator(new Authenticator() {

                @Override
                public Request authenticate(Route route, Response response) throws IOException {
                    String credential = Credentials.basic(httpVpServiceUser, httpVpServicePassword);
                    return response.request().newBuilder().header(httpAuthHeader, credential).build();
                }
            });
        }

        return builder.build();
    }
}