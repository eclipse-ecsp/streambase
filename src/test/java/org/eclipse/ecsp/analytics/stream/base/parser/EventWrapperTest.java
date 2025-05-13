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

import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * {@link EventWrapperTest}.
 */
public class EventWrapperTest {

    /** The event json. */
    private String eventJson = "{ \"uploadTimeStamp\": \"1475151885909\", \"PDID\": "
            + "\"85e744138ccb48a996c9395bae8d2a23\", \"data\": [{ \"Data\": { \"seqNum\":"
            + " 2904, \"status\": \"chunkUpldSuccessful\" }, \"EventID\": \"UploadStatus\","
            + " \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151870888,"
            + " \"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\": "
            + "{ \"seqNum\": 2905, \"status\": \"chunkUpldSuccessful\" }, \"EventID\":"
            + " \"UploadStatus\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", "
            + "\"Timestamp\": 1475151870896, \"Timezone\": 180, \"Version\": \"1.0\""
            + ", \"pii\": {} }, { \"Data\": { \"seqNum\": 2906, \"status\": \"successful\","
            + " \"uploadScheduled\": 15 }, \"EventID\": \"UploadStatus\", \"PDID\": "
            + "\"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151870901,"
            + " \"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\":"
            + " { \"value\": \"12345678901232609\" }, \"EventID\": \"VIN\", \"PDID\":"
            + " \"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880122,"
            + " \"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\":"
            + " { \"unit\": \"Celsius\", \"value\": \"100\" }, \"EventID\": "
            + "\"AmbientAirTemp\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\","
            + " \"Timestamp\": 1475151880123, \"Timezone\": 180, \"Version\": \"1.0\","
            + " \"pii\": {} }, { \"Data\": { \"unit\": \"percentage\", \"value\": "
            + "\"80\" }, \"EventID\": \"CalculatedEngineLoadValue\", \"PDID\": "
            + "\"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880123, "
            + "\"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\": "
            + "{ \"unit\": \"mtrsPerSec\", \"value\": \"100\" }, \"EventID\": "
            + "\"DistanceTravelledWhileMILisActivated\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", "
            + "\"Timestamp\": 1475151880123, \"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }"
            + ", { \"Data\": { \"unit\": \"tempInFh\", \"value\": \"100\" }, \"EventID\": "
            + "\"EngineCoolantTemperature\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", "
            + "\"Timestamp\": 1475151880123, \"Timezone\": 180, \"Version\": \"1.0\", "
            + "\"pii\": {} }, { \"Data\": { \"unit\": \"ltrsperhour\", \"value\": \"100\" }, "
            + "\"EventID\": \"EngineFuelRate\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\","
            + " \"Timestamp\": 1475151880123, \"Timezone\": 180, \"Version\": \"1.0\", "
            + "\"pii\": {} }, { \"Data\": { \"unit\": \"Celcius\", \"value\": \"100\" }, "
            + "\"EventID\": \"EngineOilTemperature\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\","
            + " \"Timestamp\": 1475151880123, \"Timezone\": 180, \"Version\": \"1.0\", "
            + "\"pii\": {} }, { \"Data\": { \"heading\": \"N\", \"isLastKnownLocation\": "
            + "\"false\", \"latitude\": \"66.22458\", \"longitude\": \"43.852421\", "
            + "\"noLastKnownLocation\": \"false\", \"speed\": \"30\" }, \"EventID\": \"Location\","
            + " \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880123,"
            + " \"Timezone\": 180, \"Version\": \"1.1\", \"pii\": {} }, { \"Data\":"
            + " { \"heading\": \"N\", \"isLastKnownLocation\": \"false\", \"latitude\":"
            + " \"66.22458\", \"longitude\": \"43.852432\", \"noLastKnownLocation\": "
            + "\"false\", \"speed\": \"30\" }, \"EventID\": \"Location\", \"PDID\": "
            + "\"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880123, "
            + "\"Timezone\": 180, \"Version\": \"1.1\", \"pii\": {} }, { \"Data\": "
            + "{ \"unit\": \"rpm\", \"value\": \"111\" }, \"EventID\": \"EngineRPM\", "
            + "\"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880125,"
            + " \"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\": "
            + "{ \"unit\": \"newtonMeter\", \"value\": \"80\" }, \"EventID\": "
            + "\"EngineReferenceTorque\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\""
            + ", \"Timestamp\": 1475151880128, \"Timezone\": 180, \"Version\": "
            + "\"1.0\", \"pii\": {} }, { \"Data\": { \"unit\": \"percentage\", \"value\":"
            + " \"25\" }, \"EventID\": \"FuelLevel\", \"PDID\": "
            + "\"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880130, "
            + "\"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\": "
            + "{ \"unit\": \"Celcius\", \"value\": \"100\" }, \"EventID\":"
            + " \"IntakeAirTemperature\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\","
            + " \"Timestamp\": 1475151880132, \"Timezone\": 180, \"Version\": \"1.0\","
            + " \"pii\": {} }, { \"Data\": { \"unit\": \"milligramPerCubicMeter\", "
            + "\"value\": \"100\" }, \"EventID\": \"PMSensorMassConcentration\", "
            + "\"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880134,"
            + " \"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\":"
            + " { \"unit\": \"sec\", \"value\": \"100\" }, \"EventID\": "
            + "\"TimeSinceEngineStart\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\","
            + " \"Timestamp\": 1475151880134, \"Timezone\": 180, \"Version\": \"1.0\","
            + " \"pii\": {} }, { \"Data\": { \"unit\": \"minute\", \"value\": \"100\" }"
            + ", \"EventID\": \"TimesinceDTCscleared\", \"PDID\": "
            + "\"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880135, "
            + "\"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\":"
            + " { \"unit\": \"sec\", \"value\": \"100\" }, \"EventID\": \"TotalEngineRunTime\""
            + ", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": "
            + "1475151880135, \"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, "
            + "{ \"Data\": { \"value\": \"0\" }, \"EventID\": \"Typeoffuel\", \"PDID\":"
            + " \"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880137, "
            + "\"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\": "
            + "{ \"unit\": \"sec\", \"value\": \"100\" }, \"EventID\": \"TimeConnectivityLost\","
            + " \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": "
            + "1475151880137, \"Timezone\": 180, \"Version\": \"1.0\", \"pii\": {} },"
            + " { \"Data\": { \"xvalue\": \"100\", \"yvalue\": \"100\", \"zvalue\": "
            + "\"100\" }, \"EventID\": \"GyroScopeinfo\", \"PDID\": "
            + "\"85e744138ccb48a996c9395bae8d2a23\", \"Timestamp\": 1475151880137, \"Timezone\": "
            + "180, \"Version\": \"1.0\", \"pii\": {} }, { \"Data\": { \"xvalue\":"
            + " \"100\", \"yvalue\": \"100\", \"zvalue\": \"100\" }, \"EventID\": "
            + "\"AccelerometerInfo\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\", "
            + "\"Timestamp\": 1475151880138, \"Timezone\": 180, \"Version\": \"1.0\", "
            + "\"pii\": {} }, { \"Data\": { \"unit\": \"mtrsPerSec\", \"value\": \"23\" }, "
            + "\"EventID\": \"VehicleSpeed\", \"PDID\": \"85e744138ccb48a996c9395bae8d2a23\","
            + " \"Timestamp\": 1475151880138, \"Timezone\": 180, \"Version\": \"1.0\", "
            + "\"pii\": {} }]}";

    /** The headless event json. */
    private String headlessEventJson = "[{\"EventID\":\"AmbientAirTemp\",\"Data\":"
            + "{\"value\":\"100\", \"unit\":\"Celcius\"},\"Version\":\"1.0\",\"TimeStamp\""
            + ":1443717903851,\"PDID\": \"8f30cddc15d44848ac65a676fe49b144\",\"Timezone\": "
            + "330}, {\"EventID\":\"AmbientAirTemp\",\"Data\":{\"value\":\"150\", \"unit\":"
            + "\"Celcius\"},\"Version\":\"1.0\",\"TimeStamp\":1443717903851,\"PDID\":"
            + " \"8f30cddc15d44848ac65a676fe49b144\",\"Timezone\": 330}, {  \"EventID\": "
            + "\"Location\",  \"Version\": \"1.1\", \"TimeStamp\":1443717903851,   "
            + "\"Data\": {\"heading\": \"N\", \"speed\": \"30\", \"latitude\": "
            + "\"12.92458\", \"longitude\": \"77.852421\"  }}]";

    /**
     * Test parse with expr.
     */
    @Test
    public void testParseWithExpr() {
        EventParser p = new EventParser();
        EventWrapperBase w = p.parseEventMapToWrapper(eventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("data[EventID=EngineRPM].Data.value");
        Assert.assertNotNull(v);
        GenericValue gv = new GenericValue(v);
        Assert.assertEquals(Constants.LONG_111, gv.asLong());
    }

    /**
     * Test parse with expr fetch map.
     */
    @Test
    public void testParseWithExprFetchMap() {
        EventParser p = new EventParser();
        EventWrapperBase w = p.parseEventMapToWrapper(eventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("data[EventID=EngineRPM].Data");
        Assert.assertNotNull(v);
        Map m = (Map) v;
        Assert.assertEquals(new String("111"), m.get("value"));
    }

    /**
     * Test parse with expr fetch event.
     */
    @Test
    public void testParseWithExprFetchEvent() {
        EventParser p = new EventParser();
        EventWrapperBase w = p.parseEventMapToWrapper(eventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("data[EventID=EngineRPM]");
        Assert.assertNotNull(v);
        Map m = (Map) v;
        Assert.assertEquals(Long.valueOf(Constants.LONG_1475151880125),
                w.getProperty(m, "Timestamp"));
        Assert.assertEquals(new String("111"), w.getProperty(m, "Data.value"));
    }

    /**
     * Test parse with expr fetch list of maps.
     */
    @Test
    public void testParseWithExprFetchListOfMaps() {
        EventParser p = new EventParser();
        EventWrapperBase w = p.parseEventMapToWrapper(eventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("data[EventID=Location]");
        Assert.assertNotNull(v);
        List l = (List) v;
        String data1 = (String) ((Map) ((Map) l.get(0)).get("Data")).get("longitude");
        String data2 = (String) ((Map) ((Map) l.get(1)).get("Data")).get("longitude");
        Assert.assertEquals("43.852421", data1);
        Assert.assertEquals("43.852432", data2);
    }

    /**
     * Test parse with expr for wrapped sequence.
     */
    @Test
    public void testParseWithExprForWrappedSequence() {
        EventParser p = new EventParser();
        EventWrapperForSequence w = p.parseEventSequenceToWrapper(headlessEventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("[EventID=Location].Data.heading");
        Assert.assertNotNull(v);
        GenericValue gv = new GenericValue(v);
        Assert.assertEquals("N", gv.asString());
    }

    /**
     * Test parse with expr fetch map for wrapped sequence.
     */
    @Test
    public void testParseWithExprFetchMapForWrappedSequence() {
        EventParser p = new EventParser();
        EventWrapperForSequence w = p.parseEventSequenceToWrapper(headlessEventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("[EventID=Location].Data");
        Assert.assertNotNull(v);
        Map m = (Map) v;
        Assert.assertEquals(new String("N"), m.get("heading"));
    }

    /**
     * Test parse with expr fetch event for wrapped sequence.
     */
    @Test
    public void testParseWithExprFetchEventForWrappedSequence() {
        EventParser p = new EventParser();
        EventWrapperForSequence w = p.parseEventSequenceToWrapper(headlessEventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("[EventID=Location]");
        Assert.assertNotNull(v);
        Map m = (Map) v;
        Assert.assertEquals(Long.valueOf(Constants.LONG_1443717903851),
                w.getProperty(m, "TimeStamp"));
        Assert.assertEquals(new String("N"), w.getProperty(m, "Data.heading"));
    }

    /**
     * Test parse with expr fetch events for wrapped sequence.
     */
    @Test
    public void testParseWithExprFetchEventsForWrappedSequence() {
        EventParser p = new EventParser();
        EventWrapperForSequence w = p.parseEventSequenceToWrapper(headlessEventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("[EventID=AmbientAirTemp]");
        Assert.assertNotNull(v);
        System.out.println(v);
        List l = (List) v;
        Map m = (Map) l.get(0);
        Assert.assertEquals(Long.valueOf(Constants.LONG_1443717903851),
                w.getProperty(m, "TimeStamp"));
        Assert.assertTrue(w.getProperty(m, "Data.value").equals("100")
                || w.getProperty(m, "Data.value").equals("150"));
    }

    /**
     * testParseWithExprFetchListOfMapsForWrappedSequence().
     */
    public void testParseWithExprFetchListOfMapsForWrappedSequence() {
        EventParser p = new EventParser();
        EventWrapperForSequence w = p.parseEventSequenceToWrapper(headlessEventJson.getBytes());
        Assert.assertNull(w.getParseException());
        Assert.assertNull(w.getRawEvent());
        Object v = w.getPropertyByExpr("data[EventID=AmbientAirTemp]");
        Assert.assertNotNull(v);
        List l = (List) v;
        String data1 = (String) ((Map) ((Map) l.get(0)).get("Data")).get("value");
        String data2 = (String) ((Map) ((Map) l.get(1)).get("Data")).get("value");
        Assert.assertEquals("100", data1);
        Assert.assertEquals("150", data2);
    }
}
