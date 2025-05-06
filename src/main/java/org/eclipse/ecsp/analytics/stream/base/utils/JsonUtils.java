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

package org.eclipse.ecsp.analytics.stream.base.utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Util class: {@link JsonUtils}.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class JsonUtils {
    
    /**
     * Instantiates a new json utils.
     */
    protected JsonUtils() {}

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(JsonUtils.class);
    
    /** The Constant JSON_MAPPER. */
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    
    /** The Constant ISO_DT_FORMATTER. */
    private static final DateTimeFormatter ISO_DT_FORMATTER = ISODateTimeFormat.dateTime().withZoneUTC();

    static {
        JSON_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        JSON_MAPPER.setSerializationInclusion(Include.NON_NULL);
        JSON_MAPPER.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));
    }

    /**
     * getValueAsString().
     *
     * @param key key
     * @param data data
     * @return String
     */
    public static String getValueAsString(String key, String data) {

        JsonNode json = null;
        try {
            json = JSON_MAPPER.readValue(data, JsonNode.class);
        } catch (IOException e) {
            logger.info("Unable to parse the event data: {} ", data);
            return null;
        }
        return safeGetStringFromJsonNode(key, json);

    }

    /**
     * safeGetStringFromJsonNode().
     *
     * @param key key
     * @param json json
     * @return String
     */
    public static String safeGetStringFromJsonNode(String key, JsonNode json) {

        if (json == null) {
            return null;
        }

        JsonNode node = json.get(key);

        if (node != null) {
            return node.asText();
        } else {
            Iterator<?> it = json.fieldNames();
            while (it.hasNext()) {
                String str = (String) it.next();
                if (str.equalsIgnoreCase(key)) {
                    return json.get(str).asText();
                }
            }
        }

        return null;
    }

    /**
     * safeGetBooleanFromJsonNode().
     *
     * @param key key
     * @param json json
     * @return boolean
     */
    public static boolean safeGetBooleanFromJsonNode(String key, JsonNode json) {
        if (json == null) {
            return false;
        }

        JsonNode node = json.get(key);
        if (node != null) {
            return node.asBoolean();
        }
        return false;
    }

    /**
     * getObjectValueAsString().
     *
     * @param obj obj
     * @return String
     */
    public static String getObjectValueAsString(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            logger.info("Unable to create the class for the object {}", obj.toString());
            return null;
        }
    }

    /**
     * getObjectValueAsBytes().
     *
     * @param obj obj
     * @return byte{@code [}{@code ]}
     */
    public static byte[] getObjectValueAsBytes(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            logger.error("Unable to create the class for the object {} error {}", obj.toString(), e);
            return new byte[0];
        }
    }

    /**
     * getJsonNode().
     *
     * @param key key
     * @param data data
     * @return JsonNode
     */
    public static JsonNode getJsonNode(String key, String data) {
        JsonNode json = null;
        try {
            json = JSON_MAPPER.readValue(data, JsonNode.class);
        } catch (IOException e) {
            logger.error("Unable to parse the event data: {}, error {}", data, e);
            return null;
        }

        if (json == null) {
            return null;
        }

        return json.get(key);

    }

    /**
     * getJsonAsMap().
     *
     * @param eventData eventData
     * @return Map
     */
    public static Map<String, Object> getJsonAsMap(String eventData) {
        try {
            return JSON_MAPPER.readValue(eventData, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            logger.error("Unable to convert the eventData to object and error is {}", e);
            return Collections.emptyMap();
        }

    }

    /**
     * getObjectAsMap().
     *
     * @param object object
     * @return Map
     */
    public static Map<String, Object> getObjectAsMap(Object object) {
        try {
            return JSON_MAPPER.convertValue(object, Map.class);
        } catch (Exception e) {
            logger.error("Unable to convert the eventData {} to object and error is {}", object, e);
            return Collections.emptyMap();
        }

    }

    /**
     * From the given JsonNode, fetches the value for a key and put the value in a list.
     *
     * @param node node
     * @param key key
     * @return List
     */
    public static List<String> getValuesAsList(JsonNode node, String key) {
        List<String> list = new ArrayList<>();
        JsonNode val = node.get(key);
        if (null != val) {
            if (val.isArray()) {
                Iterator<JsonNode> iter = val.iterator();
                while (iter.hasNext()) {
                    String value = iter.next().asText();
                    list.add(value);
                }

            } else if (val.isTextual()) {
                // should be single value
                list.add(val.asText());
            } else {
                logger.error("Only single string value or arrays of string values are supported");
            }
        }
        return list;
    }

    /**
     * Method data takes the json data and binds to the POJO.
     *
     * @param <T> the generic type
     * @param eventData eventData
     * @param clazz the clazz
     * @return the t
     * @throws IOException IOException
     */
    public static <T> T bindData(String eventData, Class<T> clazz) throws IOException {
        return JSON_MAPPER.readValue(eventData, clazz);
    }

    /**
     * Gets the list objects.
     *
     * @param <T> the generic type
     * @param data the data
     * @param cl the cl
     * @return the list objects
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static <T> List<T> getListObjects(String data, Class<T> cl) throws IOException {
        return JSON_MAPPER.readValue(data, JSON_MAPPER.getTypeFactory().constructCollectionType(List.class, cl));
    }

    /**
     * For converting the joda time to ISO date which is used by MongoDB.
     */
    public static class IsoDateSerializer extends JsonSerializer<DateTime> {
        
        /**
         * Serialize.
         *
         * @param value the value
         * @param jgen the jgen
         * @param provider the provider
         * @throws IOException Signals that an I/O exception has occurred.
         */
        @Override
        public void serialize(DateTime value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            String isoDate = ISO_DT_FORMATTER.print(value);
            jgen.writeString(isoDate);
        }
    }
}
