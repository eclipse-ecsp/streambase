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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * class EventWrapperForMap implements EventWrapperBase.
 */
public class EventWrapperForMap implements EventWrapperBase {
    
    /** The event data. */
    private Map<?, ?> eventData;
    
    /** The parse exception. */
    private Exception parseException;
    
    /** The raw event. */
    private byte[] rawEvent;

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(EventWrapperForMap.class);
    
    /** The Constant NAME_PATTERN. */
    private static final Pattern NAME_PATTERN = Pattern.compile("\\.");
    
    /** The Constant QUALIFIER_SEP_PATTERN. */
    private static final Pattern QUALIFIER_SEP_PATTERN = Pattern.compile("\\,");
    
    /** The Constant COMPILED_QUALIFIERS. */
    private static final ConcurrentHashMap<String, List<Optional<Qualifier[]>>> COMPILED_QUALIFIERS = 
            new ConcurrentHashMap<>();
    
    /** The Constant OBJECT_MAPPER. */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Instantiates a new event wrapper for map.
     *
     * @param events the events
     */
    public EventWrapperForMap(Map<?, ?> events) {
        this.eventData = events;
    }

    /**
     * Instantiates a new event wrapper for map.
     *
     * @param rawEvent the raw event
     * @param e the e
     */
    public EventWrapperForMap(byte[] rawEvent, Exception e) {
        this.rawEvent = rawEvent;
        this.parseException = e;
    }

    /**
     * Checks if is valid.
     *
     * @return true, if is valid
     */
    @Override
    public boolean isValid() {
        return parseException == null;
    }

    /**
     * Gets the parses the exception.
     *
     * @return the parses the exception
     */
    @Override
    public Exception getParseException() {
        return parseException;
    }

    /**
     * Gets the raw event.
     *
     * @return the raw event
     */
    @Override
    public byte[] getRawEvent() {
        return rawEvent;
    }

    /**
     * Gets the property.
     *
     * @param wrapper the wrapper
     * @param name the name
     * @return the property
     */
    @Override
    public Object getProperty(Map wrapper, String name) {
        if (wrapper == null) {
            return null;
        }
        String[] splits = NAME_PATTERN.split(name);
        // optimize for 1-2 levels of nesting
        if (splits.length == 1) {
            return wrapper.get(splits[0]);
        }
        if (splits.length == Constants.TWO) {
            Object r = wrapper.get(splits[0]);
            if (r instanceof Map) {
                return ((Map) r).get(splits[1]);
            } else if (r instanceof List) {
                return ((Map) ((List) r).get(0)).get(splits[1]);
            } else {
                return null;
            }
        }
        // deeper nesting
        Object ret = eventData;
        for (int i = 0; i < splits.length; i++) {
            if (ret instanceof Map<?, ?> map) {
                ret = map.get(splits[i]);
            } else if (ret instanceof List<?> s) {
                List<Object> l = new ArrayList<>(s.size());
                Iterator<?> it = s.iterator();
                while (it.hasNext()) {
                    l.add(((Map) it.next()).get(splits[i]));
                }
                ret = l;
            }
        }
        return ret;
    }

    /**
     * Gets the property.
     *
     * @param name the name
     * @return the property
     */
    @Override
    public Object getProperty(String name) {
        if (eventData == null) {
            return null;
        }
        String[] splits = NAME_PATTERN.split(name);
        List<Optional<Qualifier[]>> qualifiers = getQualifiers(name, splits);
        clearQualifiers(splits);

        Object ret = eventData;
        for (int i = 0; i < splits.length; i++) {
            if (ret instanceof Map) {
                ret = getObjectForMap(splits, qualifiers, ret, i);
            } else if (ret instanceof List<?> s) {
                List<Object> l = new ArrayList<>(s.size());
                Iterator<?> it = s.iterator();
                while (it.hasNext()) {
                    l.add(((Map) it.next()).get(splits[i]));
                }
                ret = l;
            }
            if (ret == null) {
                break;
            }
        }
        return ret;
    }

    /**
     * Gets the object for map.
     *
     * @param splits the splits
     * @param qualifiers the qualifiers
     * @param ret the ret
     * @param i the i
     * @return the object for map
     */
    private Object getObjectForMap(String[] splits, List<Optional<Qualifier[]>> qualifiers, Object ret, int i) {
        ret = splits[i].length() == 0 ? ret : ((Map) ret).get(splits[i]);
        Optional<Qualifier[]> qlist = qualifiers.get(i);
        if (ret instanceof List<?> list && qlist.isPresent()) {
            List<Map<?, ?>> flist = filterList(list, qlist.get());
            ret = (flist.size() == 1) ? flist.get(0) : flist;
        }
        return ret;
    }

    /**
     * Clear qualifiers.
     *
     * @param splits the splits
     */
    private void clearQualifiers(String[] splits) {
        for (int i = 0; i < splits.length; i++) {
            int qualifierStart = splits[i].indexOf("[");
            if (qualifierStart != Constants.NEGATIVE_ONE) {
                splits[i] = splits[i].substring(0, qualifierStart);
            }
        }
    }

    /**
     * Gets the qualifiers.
     *
     * @param name the name
     * @param splits the splits
     * @return the qualifiers
     */
    private List<Optional<Qualifier[]>> getQualifiers(String name,
            String[] splits) {
        List<Optional<Qualifier[]>> qualifiers = COMPILED_QUALIFIERS.get(name);
        if (qualifiers == null) {
            qualifiers = buildQualifiers(splits);
            COMPILED_QUALIFIERS.putIfAbsent(name, qualifiers);
        }
        return qualifiers;
    }

    /**
     * Filter list.
     *
     * @param retAsList the ret as list
     * @param qualifiers the qualifiers
     * @return the list
     */
    private List<Map<?, ?>> filterList(List<?> retAsList, Qualifier[] qualifiers) {
        List<Map<?, ?>> l = new ArrayList<>();
        for (int j = 0; j < retAsList.size(); j++) {
            Map<?, ?> v = (Map) retAsList.get(j);
            boolean all = true;
            for (Qualifier q : qualifiers) {
                all = all && (q.matches(v));
            }
            if (all) {
                l.add(v);
            }
        }
        return l;
    }

    /**
     * Return one event after sorting by sorting criteria. Ex : Say
     * we have multiple EID=Location events in the message, and we want the
     * Location event which has the latest TimeStamp. Client will call like :
     * generalEvent.getPropertyByExpr("data[EventID=Location].Data.longitude", "TimeStamp", "descending")
     *
     * @param name
     *         : Ex :
     * @param byFieldToBeSorted
     *         : TimeStamp, Version
     * @param sort
     *         : ascending or descending
     * @return Object
     */
    @Override
    public Object getPropertyByExpr(String name, String byFieldToBeSorted,
            String sort) {
        if (this.eventData == null) {
            return null;
        }
        String[] splits = NAME_PATTERN.split(name);
        List<Optional<Qualifier[]>> qualifiers = this.getQualifiers(name,
                splits);
        this.clearQualifiers(splits);
        Object ret = this.eventData;
        for (int i = 0; i < splits.length; ++i) {
            if (ret instanceof Map<?, ?> map) {
                ret = map.get(splits[i]);
                Optional<Qualifier[]> qlist = qualifiers.get(i);
                if (ret instanceof List<?> list && qlist.isPresent()) {
                    ret = this.getEventBySortedField(list, qlist.get(),
                            byFieldToBeSorted, sort);
                }
            } else if (ret instanceof List<?> s) {
                ArrayList<Object> l = new ArrayList<>(s.size());
                Iterator<?> it = s.iterator();
                while (it.hasNext()) {
                    l.add(((Map) it.next()).get(splits[i]));
                }
                ret = l;
            }
            if (ret == null) {
                break;
            }
        }
        return ret;
    }


    /**
     * Can qualify the results of navigating a path with additional
     * operators. For ex data[EventID=EngineRPM].Data.value will look up the
     * event object for 'data' the result of which is filtered for
     * the expr 'EventID=EngineRPM'; the result of which is looked up for 'Data'
     * followed by 'value' and the final result is returned.
     *
     * @param name name
     * @return Object
     */
    @Override
    public Object getPropertyByExpr(String name) {
        if (eventData == null) {
            return null;
        }
        String[] splits = NAME_PATTERN.split(name);
        List<Optional<Qualifier[]>> qualifiers = getQualifiers(name, splits);
        clearQualifiers(splits);

        Object ret = eventData;
        for (int i = 0; i < splits.length; i++) {
            if (ret instanceof Map) {
                ret = getObjectForMap(splits, qualifiers, ret, i);
            } else if (ret instanceof List<?> s) {
                List<Object> l = new ArrayList<>(s.size());
                Iterator<?> it = s.iterator();
                while (it.hasNext()) {
                    l.add(((Map) it.next()).get(splits[i]));
                }
                ret = l;
            }
            if (ret == null) {
                break;
            }
        }
        return ret;

    }

    /**
     * Return one event after sorting by sorting criteria. Ex : Say we
     * have multiple EID=Location events in the message, and we want the
     * Location event which has the latest TimeStamp.
     *
     * @param retAsList retAsList
     * @param qualifiers qualifiers
     *         :
     * @param byFieldToBeSorted
     *         : Ex : TimeStamp, Version
     * @param sort
     *         : ascending/descending
     * @return Map
     */
    private Map<?, ?> getEventBySortedField(List<?> retAsList, Qualifier[] qualifiers,
            String byFieldToBeSorted, String sort) {
        Comparator<Map<?, ?>> comparator = "asc".equals(sort) ? new AscendingComparator(
                byFieldToBeSorted) : new DescendingComparator(byFieldToBeSorted);
        ArrayList<Map<?, ?>> list = new ArrayList<>();
        for (int j = 0; j < retAsList.size(); ++j) {
            Map<?, ?> v = (Map) retAsList.get(j);
            for (Qualifier q : qualifiers) {
                if (q.matches(v)) {
                    // empty if block
                }
                list.add(v);
            }
        }
        list.sort(comparator);
        return list.get(0);
    }

    /**
     * Builds the qualifiers.
     *
     * @param splits the splits
     * @return the list
     */
    private List<Optional<Qualifier[]>> buildQualifiers(String[] splits) {
        List<Optional<Qualifier[]>> qualifiers = new ArrayList<>(splits.length);
        for (int i = 0; i < splits.length; i++) {
            int qualifierStart = splits[i].indexOf("[");
            if (qualifierStart != Constants.NEGATIVE_ONE) {
                String qstring = splits[i].substring(qualifierStart + 1,
                        splits[i].length() - 1);
                String[] qstrings = QUALIFIER_SEP_PATTERN.split(qstring);
                Qualifier[] qs = new Qualifier[qstrings.length];
                for (int j = 0; j < qstrings.length; j++) {
                    qs[j] = new Qualifier(qstrings[j]);
                }
                qualifiers.add(Optional.of(qs));
            } else {
                qualifiers.add(Optional.empty());
            }
        }
        return qualifiers;
    }

    /**
     * The Class Qualifier.
     */
    private static class Qualifier {
        
        /** The Constant OPERATORS. */
        private static final Pattern OPERATORS = Pattern.compile("=");
        
        /** The field. */
        private String field;
        
        /** The value. */
        private String value;

        /**
         * Instantiates a new qualifier.
         *
         * @param expr the expr
         */
        public Qualifier(String expr) {
            String[] tokens = OPERATORS.split(expr);
            field = tokens[0].trim();
            value = tokens[1].trim();
        }

        /**
         * The Enum Op.
         */
        private enum Op {
            
            /** The equals. */
            EQUALS("=");

            /** The operator. */
            private String operator;

            /**
             * Instantiates a new op.
             *
             * @param op the op
             */
            private Op(String op) {
                this.operator = op;
            }

            /**
             * Find.
             *
             * @param op the op
             * @return the op
             */
            public static Op find(String op) {
                if (op.equals("=")) {
                    return EQUALS;
                }
                return EQUALS;
            }
        }
        
        /**
         * Matches.
         *
         * @param data the data
         * @return true, if successful
         */
        public boolean matches(Map<?, ?> data) {
            Object v = data.get(field);
            if (v == null) {
                return false;
            } else {
                return value.equals(v.toString());
            }
        }
    }

    /**
     * To json.
     *
     * @return the string
     */
    @Override
    public String toJson() {
        Map<String, Object> m = new HashMap<>();
        m.put("exception", parseException);
        m.put("payload", rawEvent);
        try {
            return OBJECT_MAPPER.writeValueAsString(m);
        } catch (JsonProcessingException e) {
            logger.error("Unexpected exception: ", e); // this really shouldn't be happening
            return e.getMessage();
        }
    }

    /**
     * The Class AscendingComparator.
     */
    private class AscendingComparator implements Comparator<Map<?, ?>> {
        
        /** The by field to be sorted. */
        private String byFieldToBeSorted;

        /**
         * Instantiates a new ascending comparator.
         *
         * @param byFieldToBeSorted the by field to be sorted
         */
        public AscendingComparator(String byFieldToBeSorted) {
            this.byFieldToBeSorted = null;
            this.byFieldToBeSorted = byFieldToBeSorted;
        }

        /**
         * Compare.
         *
         * @param o1 the o 1
         * @param o2 the o 2
         * @return the int
         */
        @Override
        public int compare(Map o1, Map o2) {
            int index = Constants.NEGATIVE_ONE;
            if (o1.get(this.byFieldToBeSorted) instanceof String o1string
                    && o2.get(this.byFieldToBeSorted) instanceof String o2string) {
                index = o1string.compareTo(o2string);
            } else if (o1.get(this.byFieldToBeSorted) instanceof Integer o1integer
                    && o2.get(this.byFieldToBeSorted) instanceof Integer o2integer) {
                index = o1integer.compareTo(o2integer);
            } else if (o1.get(this.byFieldToBeSorted) instanceof Long o1long
                    && o2.get(this.byFieldToBeSorted) instanceof Long o2long) {
                index = o1long.compareTo(o2long);
            } else if (o1.get(this.byFieldToBeSorted) instanceof Double o1Double
                    && o2.get(this.byFieldToBeSorted) instanceof Double o2Double) {
                index = o1Double.compareTo(o2Double);
            }
            return index;
        }
    }

    /**
     * The Class DescendingComparator.
     */
    private class DescendingComparator implements Comparator<Map<?, ?>> {
        
        /** The by field to be sorted. */
        private String byFieldToBeSorted;

        /**
         * Instantiates a new descending comparator.
         *
         * @param byFieldToBeSorted the by field to be sorted
         */
        public DescendingComparator(String byFieldToBeSorted) {
            this.byFieldToBeSorted = byFieldToBeSorted;
        }

        /**
         * Compare.
         *
         * @param o1 the o 1
         * @param o2 the o 2
         * @return the int
         */
        @Override
        public int compare(Map o1, Map o2) {
            int index = Constants.NEGATIVE_ONE;
            if (o1.get(this.byFieldToBeSorted) instanceof String o1String
                    && o2.get(this.byFieldToBeSorted) instanceof String o2String) {
                index = o2String.compareTo(o1String);
            } else if (o1.get(this.byFieldToBeSorted) instanceof Integer o1Integer
                    && o2.get(this.byFieldToBeSorted) instanceof Integer o2integer) {
                index = o2integer.compareTo(o1Integer);
            } else if (o1.get(this.byFieldToBeSorted) instanceof Long o1Long
                    && o2.get(this.byFieldToBeSorted) instanceof Long o2Long) {
                index = o2Long.compareTo(o1Long);
            } else if (o1.get(this.byFieldToBeSorted) instanceof Double o1Double
                    && o2.get(this.byFieldToBeSorted) instanceof Double o2Double) {
                index = o2Double.compareTo(o1Double);
            }
            return index;
        }
    }
}
