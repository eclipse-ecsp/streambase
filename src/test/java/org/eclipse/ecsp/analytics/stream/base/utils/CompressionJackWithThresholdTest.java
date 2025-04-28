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

import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.exception.InputStreamMaxSizeExceededException;
import org.eclipse.ecsp.analytics.stream.base.utils.CompressionJack;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;



/**
 * Test class for {@link CompressionJack}.
 */
public class CompressionJackWithThresholdTest {

    /** The compression jack. */
    private final CompressionJack compressionJack = new CompressionJack();
    
    /**
     * Test decompression zip with threshold reached.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test(expected = InputStreamMaxSizeExceededException.class)
    public void testDecompressionZipWithThresholdReached() throws IOException {
        CompressionJack.setThresholdSize(TestConstants.TWENTY_FIVE);
        String testString = "test-subject-string-looooooooooooooooooooooooooooooooooooong";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zipOut = new ZipOutputStream(baos);
        ByteArrayInputStream bais = new ByteArrayInputStream(testString.getBytes());
        ZipEntry zipEntry = new ZipEntry("TestResults.xml");
        zipOut.putNextEntry(zipEntry);
        byte[] bytes = new byte[TestConstants.INT_1024];
        int length;
        while ((length = bais.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
        }
        zipOut.close();
        bais.close();
        baos.close();
        CompressionJack.setThresholdSize(TestConstants.TWENTY_FIVE);
        compressionJack.decompress(baos.toByteArray());
    }
}
