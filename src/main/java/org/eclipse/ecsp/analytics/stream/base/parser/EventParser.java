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

package org.eclipse.ecsp.analytics.stream.base.parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * EventParser: utility class.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class EventParser<K, V> {
    
    /** The jf. */
    private JsonFactory jf = new JsonFactory();
    
    /** The mapper. */
    private ObjectMapper mapper = new ObjectMapper();
    
    /**
     * parseEventToMap().
     *
     * @param source source
     * @return Map
     * @throws EventParseException EventParseException
     */
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<K, V> parseEventToMap(byte[] source) throws EventParseException {
        try {
            Class<Map<K, V>> clazz = (Class) Map.class;
            return mapper.readValue(jf.createParser(source), clazz);
        } catch (IOException e) {
            throw new EventParseException(e);
        }
    }

    /**
     * parseEventToList().
     *
     * @param source source
     * @return List
     * @throws EventParseException EventParseException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<V> parseEventToList(byte[] source) throws EventParseException {
        try {
            Class<List<V>> clazz = (Class) List.class;
            return mapper.readValue(jf.createParser(source), clazz);
        } catch (IOException e) {
            throw new EventParseException(e);
        }
    }

    /**
     * parseEventMapToWrapper().
     *
     * @param source source
     * @return EventWrapperBase
     */
    public EventWrapperBase parseEventMapToWrapper(byte[] source) {
        try {
            return new EventWrapperForMap(mapper.readValue(jf.createParser(source), Map.class));
        } catch (IOException e) {
            return new EventWrapperForMap(source, e);
        }
    }

    /**
     * parseEventSequenceToWrapper().
     *
     * @param source source
     * @return EventWrapperForSequence
     */
    public EventWrapperForSequence parseEventSequenceToWrapper(byte[] source) {
        try {
            return new EventWrapperForSequence(mapper.readValue(jf.createParser(source), List.class));
        } catch (IOException e) {
            return new EventWrapperForSequence(source, e);
        }
    }
}
