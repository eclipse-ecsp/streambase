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

package org.eclipse.ecsp.analytics.stream.base;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;


/**
 * TestKryo class.
 */
public class TestKryo {

    /**
     * Instantiates a new test kryo.
     */
    private  TestKryo() {
    }

    /**
     * main method.
     *
     * @param args args
     */
    public static void main(String[] args) {
        Kryo k = new Kryo();
        k.setDefaultSerializer(CompatibleFieldSerializer.class);
        k.setCopyReferences(false);
        // Output output = new Output(1024, -1);
        // kryo.writeClassAndObject(output, data);
        // return output.toBytes();

        Name n = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1 * Constants.BYTE_1024 * Constants.BYTE_1024);
        Output output = new Output(baos);
        k.writeClassAndObject(output, n);
        output.close();
        byte[] bytes = baos.toByteArray();
        System.out.println("Null object");
        System.out.println(bytes.length);
        System.out.println(bytes[0]);
        Input input = new Input(new ByteArrayInputStream(bytes));
        Object o = k.readClassAndObject(input);
        input.close();
        System.out.println(o);
        n = new Name();
        n.name = "Fido";
        baos = new ByteArrayOutputStream(1 * Constants.BYTE_1024 * Constants.BYTE_1024);
        output = new Output(baos);
        k.writeClassAndObject(output, n);
        output.close();
        bytes = baos.toByteArray();
        System.out.println("Not null object");
        System.out.println(bytes.length);
        System.out.println(bytes[0]);
        input = new Input(new ByteArrayInputStream(bytes));
        o = k.readClassAndObject(input);
        input.close();
        System.out.println(o);
    }

    /**
     * inner class Name.
     */
    public static class Name {
        
        /** The name. */
        public String name;
    }
}
