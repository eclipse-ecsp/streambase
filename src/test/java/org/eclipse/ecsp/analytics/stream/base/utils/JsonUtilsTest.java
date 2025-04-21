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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


/**
 * Test class to verify the functionalities of JsonUtils class.
 */
public class JsonUtilsTest {

    /** The Constant JSON_STRING. */
    private static final String JSON_STRING = "{\"EVENT_ID\":\"DongleStatus\",\"Data\":"
            + "{\"status\":\"detached\"},\"enabled\":\"true\"}";
    
    /** The Constant JSON_STRING_FOR_ARRAY. */
    private static final String JSON_STRING_FOR_ARRAY = "{\"Data\":[\"a\",\"b\",\"c\"],"
            + "\"message\":\"String message\",\"message1\":{\"name\":\"IGnite\"}}";

    /** The Constant EVENT_ID_COLUMN. */
    private static final String EVENT_ID_COLUMN = "EVENT_ID";

    /**
     * Extract column from json string.
     */
    @Test
    public void testGetValueAsString() {
        String eventId = "DongleStatus";
        String eventIdNew = JsonUtils.getValueAsString(EVENT_ID_COLUMN, JSON_STRING);
        assertEquals(eventId, eventIdNew);
    }

    /**
     * Extract column from json string.
     */
    @Test
    public void testGetValueAsStringFromInvalidJson() {
        String eventId = JsonUtils.getValueAsString(EVENT_ID_COLUMN, "{name}");
        assertNull(eventId);
    }

    /**
     * Extract column from json Node.
     */
    @Test
    public void testForGetJsonNode() {
        String status = "detached";
        JsonNode dataNode = JsonUtils.getJsonNode("Data", JSON_STRING);
        assertEquals(status, JsonUtils.safeGetStringFromJsonNode("status", dataNode));
    }

    /**
     * Extract column from json Node.
     */
    @Test
    public void testForGetJsonNodeInvalidJson() {
        JsonNode dataNode = JsonUtils.getJsonNode("Data", "{name}");
        assertNull(dataNode);
    }

    /**
     * Getting json content as map.
     */
    @Test
    public void testForGetJsonAsMapValidJson() {
        Map<String, Object> jsonMap = JsonUtils.getJsonAsMap(JSON_STRING);
        assertNotNull(jsonMap);
        assertEquals(Constants.THREE, jsonMap.size());
    }

    /**
     * Getting json content as map for invalid scenario.
     */
    @Test
    public void testForGetJsonAsMapInValidJson() {
        Map<String, Object> jsonMap = JsonUtils.getJsonAsMap("{invalid}");
        assertEquals(Collections.emptyMap(), jsonMap);
    }

    /**
     * Getting json content as String for invalid scenario.
     *
     * @throws JsonParseException JsonParseException
     * @throws JsonMappingException JsonMappingException
     * @throws IOException IOException
     */
    @Test
    public void testForSafeGetStringFromJsonNodeInvalidScenario() throws JsonParseException,
            JsonMappingException, IOException {
        String column = "invalid";
        ObjectMapper om = new ObjectMapper();
        String jsonString = JsonUtils.safeGetStringFromJsonNode(column, om.readValue(JSON_STRING, JsonNode.class));
        assertNull(jsonString);
        jsonString = JsonUtils.safeGetStringFromJsonNode(column, null);
        assertNull(jsonString);
    }

    /**
     * Test for safe get boolean from json node.
     *
     * @throws JsonParseException the json parse exception
     * @throws JsonMappingException the json mapping exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test
    public void testForSafeGetBooleanFromJsonNode() throws JsonParseException, JsonMappingException, IOException {
        String column = "enabled";
        JsonNode jsonNode = new ObjectMapper().readValue(JSON_STRING, JsonNode.class);
        Boolean enabled = JsonUtils.safeGetBooleanFromJsonNode(column, jsonNode);
        assertNotNull(enabled);
        assertEquals(true, enabled);
        enabled = JsonUtils.safeGetBooleanFromJsonNode(column, null);
        assertFalse(enabled);
        enabled = JsonUtils.safeGetBooleanFromJsonNode("invaild", jsonNode);
        assertFalse(enabled);
    }

    /**
     * Get object value as byte array.
     *
     * @throws JsonParseException JsonParseException
     * @throws JsonMappingException JsonMappingException
     * @throws IOException IOException
     */
    @Test
    public void testForGetObjectValueAsBytes() throws JsonParseException, JsonMappingException, IOException {
        SimpleClass obj = new SimpleClass();
        String resultString = JsonUtils.getObjectValueAsString(obj);
        assertEquals(resultString, new String(JsonUtils.getObjectValueAsBytes(obj)));
        assertEquals(0, JsonUtils.getObjectValueAsBytes(new JsonUtils()).length);
        assertNull(JsonUtils.getObjectValueAsString(new JsonUtils()));
    }

    /**
     * Get object value as List.
     *
     * @throws JsonParseException JsonParseException
     * @throws JsonMappingException JsonMappingException
     * @throws IOException IOException
     */
    @Test
    public void testForGetValuesAsList() throws JsonParseException, JsonMappingException, IOException {
        List<String> result = Arrays.asList("a", "b", "c");
        List<String> resultString = JsonUtils.getValuesAsList(new ObjectMapper()
                .readValue(JSON_STRING_FOR_ARRAY, JsonNode.class), "Data");
        assertEquals(result, resultString);

        resultString = JsonUtils.getValuesAsList(new ObjectMapper()
                .readValue(JSON_STRING_FOR_ARRAY, JsonNode.class), "message");
        assertEquals(Arrays.asList("String message"), resultString);
        // invalid case. column not present in json
        resultString = JsonUtils.getValuesAsList(new ObjectMapper().readValue(JSON_STRING_FOR_ARRAY, 
                JsonNode.class), "message1");
        Assert.assertEquals(0, resultString.size());
    }

    /**
     * Test to verify whether a simple object just consist member
     * variables and no complex object is able to convert to map or .
     */
    @Test
    public void testSimpleMapConversion() {
        SimpleClass obj = new SimpleClass();
        Map<String, Object> map = JsonUtils.getObjectAsMap(obj);
        Assert.assertEquals("testIntVal value not matching", Constants.TEN, map.get("testIntVal"));
        Assert.assertEquals("testStringVal value not matching", "test", map.get("testStringVal"));
    }

    /**
     * Negative scenario for getObjectAsMap.
     */
    @Test
    public void testSimpleMapConversionForInvalidObject() {
        Map<String, Object> map = JsonUtils.getObjectAsMap(new ObjectUtils());
        assertEquals(Collections.emptyMap(), map);
    }

    /**
     * Testing whether a complex object is able to convert to map or not.
     */
    @Test
    public void testNestedObjectMapConversion() {
        ComplexClass obj = new ComplexClass();
        // the returned object will be map of map
        Map<String, Object> map = JsonUtils.getObjectAsMap(obj);

        Assert.assertEquals("complexVal value not matching", "complex", map.get("complexVal"));

        Assert.assertEquals("testIntVal value not matching", Constants.TEN,
                ((Map<String, Object>) map.get("simpleClass")).get("testIntVal"));
        Assert.assertEquals("testStringVal value not matching", "test",
                ((Map<String, Object>) map.get("simpleClass")).get("testStringVal"));
    }

    /**
     * inner class {@link SimpleClass}.
     */
    public class SimpleClass {
        
        /** The test int val. */
        private int testIntVal;
        
        /** The test string val. */
        private String testStringVal;

        /**
         * Instantiates a new simple class.
         */
        public SimpleClass() {
            testIntVal = Constants.TEN;
            testStringVal = "test";
        }

        /**
         * Gets the test int val.
         *
         * @return the test int val
         */
        public int getTestIntVal() {
            return testIntVal;
        }

        /**
         * Sets the test int val.
         *
         * @param testIntVal the new test int val
         */
        public void setTestIntVal(int testIntVal) {
            this.testIntVal = testIntVal;
        }

        /**
         * Gets the test string val.
         *
         * @return the test string val
         */
        public String getTestStringVal() {
            return testStringVal;
        }

        /**
         * Sets the test string val.
         *
         * @param testStringVal the new test string val
         */
        public void setTestStringVal(String testStringVal) {
            this.testStringVal = testStringVal;
        }
    }

    /**
     * inner class {@link ComplexClass}.
     */
    public class ComplexClass {
        
        /** The complex val. */
        private String complexVal;
        
        /** The simple class. */
        private SimpleClass simpleClass;

        /**
         * Instantiates a new complex class.
         */
        public ComplexClass() {
            complexVal = "complex";
            simpleClass = new SimpleClass();
        }

        /**
         * Gets the complex val.
         *
         * @return the complex val
         */
        public String getComplexVal() {
            return complexVal;
        }

        /**
         * Sets the complex val.
         *
         * @param complexVal the new complex val
         */
        public void setComplexVal(String complexVal) {
            this.complexVal = complexVal;
        }

        /**
         * Gets the simple class.
         *
         * @return the simple class
         */
        public SimpleClass getSimpleClass() {
            return simpleClass;
        }

        /**
         * Sets the simple class.
         *
         * @param simpleClass the new simple class
         */
        public void setSimpleClass(SimpleClass simpleClass) {
            this.simpleClass = simpleClass;
        }
    }
}
