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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import okhttp3.Headers;
import okhttp3.Headers.Builder;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * This class does the Http hit using {@link OkHttpClient}.
 */
@Component
public class HttpClient {
    
    /** The response code. */
    public static String RESPONSE_CODE = "responseCode";
    
    /** The response json. */
    public static String RESPONSE_JSON = "responseJson";
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(HttpClient.class);
    
    /** The {@link HttpClientFactory} instance. */
    @Autowired
    private HttpClientFactory httpClientFactory;
    
    /** The {@link OkHttpClient} instance. */
    private OkHttpClient okHttpClient;
    
    /** The {@link ObjectMapper} instance for serialization /deserialization. */
    private ObjectMapper responseMapper;
    
    /** The {@link Response} instance. */
    private Response response;

    /**
     * Gets the request builder.
     *
     * @param method the method
     * @param parameters the parameters
     * @param requestBuilder the request builder
     * @return the request builder
     * @throws JsonProcessingException the json processing exception
     */
    @NotNull
    private static Request.Builder getRequestBuilder(HttpReqMethod method, Map<String, Object> parameters, 
            Request.Builder requestBuilder) throws JsonProcessingException {
        String jsonBody = "{}";
        if (parameters != null) {
            jsonBody = new ObjectMapper().writeValueAsString(parameters);
        }
        MediaType json = MediaType.parse("application/json; charset=utf-8");
        RequestBody requestBody = RequestBody.create(json, jsonBody);

        requestBuilder = HttpReqMethod.PUT.equals(method)
                ? requestBuilder.put(requestBody) : requestBuilder.post(requestBody);
        return requestBuilder;
    }
    
    /**
     * Enum for request type.
     */
    public enum HttpReqMethod {
        
        /** The put. */
        PUT, 
 /** The post. */
 POST, 
 /** The get. */
 GET;
    }

    /**
     * Invokes the API with the specified URL.
     *
     * @param httpUrl httpUrl
     * @return JsonNode Response data.
     */
    public JsonNode invokeJsonResource(String httpUrl) {
        Map<String, String> header = new HashMap<>();
        Map<String, Object> params = new HashMap<>();

        Map<String, Object> responseData = invokeJsonResource(HttpReqMethod.GET,
                httpUrl, header, params, Constants.THREE, Constants.LONG_60000);

        return (JsonNode) responseData.get(RESPONSE_JSON);
    }

    /**
     * Invokes the API with the specified URL and query params.
     *
     * @param httpUrl httpUrl
     * @param keyValue Query parameters.
     * @return JsonNode Response.
     */
    public JsonNode invokeJsonResource(String httpUrl, Map<String, String> keyValue) {
        Map<String, Object> parameters = new HashMap<>();
        keyValue.forEach(parameters::put);
        Map<String, String> header = new HashMap<>();

        Map<String, Object> responseData = invokeJsonResource(HttpReqMethod.GET,
                httpUrl, header, parameters, Constants.THREE, Constants.LONG_60000);

        return (JsonNode) responseData.get(RESPONSE_JSON);
    }

    /**
     * Executes given HTTP GET/PUT/POST request URL with headers and parameters.
     * For PUT/POST, parameters go into the request body as JSON;
     * and for GET, parameters are appended to the URL.
     *
     * @param method method
     * @param httpUrl httpUrl
     * @param headers headers
     * @param parameters parameters
     * @param retryCount the retry count
     * @param retryInterval the retry interval
     * @return responseData has HTTP response status code and JSON
     */
    public Map<String, Object> invokeJsonResource(HttpReqMethod method, String httpUrl, Map<String, String> headers,
            Map<String, Object> parameters, int retryCount, long retryInterval) {
        if ((!(HttpReqMethod.GET.equals(method) | HttpReqMethod.PUT
                .equals(method) | HttpReqMethod.POST.equals(method)))) {
            throw new IllegalArgumentException("Accepts only 'GET', 'PUT', 'POST' method.");
        }

        Map<String, Object> responseData = new HashMap<>();
        try {
            Builder requestHeader = new Headers.Builder();
            if (headers != null) {
                headers.forEach(requestHeader::add);
            }

            Request.Builder requestBuilder = new Request.Builder();

            if (HttpReqMethod.PUT.equals(method) || HttpReqMethod.POST.equals(method)) {
                requestBuilder = getRequestBuilder(method, parameters, requestBuilder);
            } else {
                HttpUrl.Builder urlBuilder = getUrlBuilder(httpUrl, parameters);
                requestBuilder = requestBuilder.get();
                HttpUrl url = urlBuilder.build();
                httpUrl = url.toString();
            }

            requestBuilder.url(httpUrl).headers(requestHeader.build());

            Request request = requestBuilder.build();

            int retryAttempt = 0;
            do {
                JsonNode jsonNode = sendRequest(request, responseData, httpUrl, headers, parameters);
                if (jsonNode != null) {
                    responseData.put(RESPONSE_JSON, jsonNode);
                    break;
                }
                retryAttempt++;
                logger.info("Retrying URL={}, headers={}, parameters={}, retryAttempt={}, retryInterval={}",
                        httpUrl, headers, parameters, retryAttempt, retryInterval);
                Thread.sleep(retryInterval);
            } while (retryAttempt <= retryCount);
            
            logger.debug("Executed URL={}, headers={}, parameters={}, responseData={}", httpUrl, headers, parameters,
                    responseData);
        } catch (InterruptedException exception) {
            logger.error("Interrupted exception occurred while executing URL={}, headers={}, "
                    + "parameters={}, exception={}", httpUrl, headers, parameters, exception);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Error while executing URL={}, headers={}, parameters={}, "
                    + "response={}, error={}", httpUrl, headers, parameters, e.getMessage());
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return responseData;
    }

    /**
     * Invokes the API.
     *
     * @param request the request
     * @param responseData the response data
     * @param httpUrl the http url
     * @param headers the headers
     * @param parameters the parameters
     * @return the json node
     */
    private JsonNode sendRequest(Request request, Map<String, Object> responseData, String httpUrl,
            Map<String, String> headers, Map<String, Object> parameters) {
        try {
            response = okHttpClient.newCall(request).execute();

            int respStatusCode = response.code();

            responseData.put(RESPONSE_CODE, String.valueOf(respStatusCode));

            String responseBody = response.body().string();
            return responseMapper.readTree(responseBody);

        } catch (Exception e) {
            logger.error("Error while executing URL={}, headers={}, parameters={}, response={}, "
                    + "error={}", httpUrl, headers, parameters, e.getMessage());
            return null;
        }
    }

    /**
     * Gets the url builder.
     *
     * @param httpUrl the http url
     * @param parameters the parameters
     * @return the url builder
     */
    @NotNull
    private static HttpUrl.Builder getUrlBuilder(String httpUrl, Map<String, Object> parameters) {
        HttpUrl.Builder urlBuilder = HttpUrl.parse(httpUrl).newBuilder();

        if (parameters != null) {
            for (Entry<String, Object> entry : parameters.entrySet()) {
                urlBuilder.addQueryParameter(entry.getKey(), entry.getValue().toString());
            }
        }
        return urlBuilder;
    }
    
    /**
     * Initializer for this class.
     */
    @PostConstruct
    public void init() {
        responseMapper = new ObjectMapper();
        okHttpClient = httpClientFactory.createDefaultHttpClient(Optional.empty(), false);
        logger.info("HttpClient initialized.");
    }
}