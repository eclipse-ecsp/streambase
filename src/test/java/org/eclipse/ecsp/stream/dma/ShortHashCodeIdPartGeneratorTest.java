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

package org.eclipse.ecsp.stream.dma;

import org.eclipse.ecsp.analytics.stream.base.idgen.internal.ShortHashCodeIdPartGenerator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * ShortHashCodeIdPartGenerator test for short code generating.
 *
 * @author Binoy
 */
public class ShortHashCodeIdPartGeneratorTest {

    /** The Constant SHORT_CODE. */
    private static final String SHORT_CODE = "18351";
    
    /** The Constant SERVICE_NAME. */
    private static final String SERVICE_NAME = "ECall";

    /** The short hash code id part generator. */
    private ShortHashCodeIdPartGenerator shortHashCodeIdPartGenerator;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        shortHashCodeIdPartGenerator = new ShortHashCodeIdPartGenerator();
    }

    /**
     * Testing for non null object.
     */
    @Test
    public void testGenerateIdPart() {
        String actualCode = shortHashCodeIdPartGenerator.generateIdPart(SERVICE_NAME);
        assertEquals(SHORT_CODE, actualCode);
    }

    /**
     * Testing for empty String.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenerateIdPartWithEmptyString() {
        shortHashCodeIdPartGenerator.generateIdPart("");
    }

}
