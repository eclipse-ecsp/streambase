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

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.utils.CompressionJack;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;




/**
 * CompressionJackTest UT class for {@link CompressionJack}.
 */

public class CompressionJackTest {

    /** The compression jack. */
    private final CompressionJack compressionJack = new CompressionJack();
    
    /**
     * Before.
     */
    @BeforeEach
    public void before() {
        CompressionJack.setThresholdSize(TestConstants.ONE_BILLION);
    }

    /**
     * Test decompression G zip.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test
    public void testDecompressionGZip() throws IOException {
        String testString = "test-Subject";
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(testString.getBytes(StandardCharsets.UTF_8));
        gzip.flush();
        gzip.close();
        obj.close();
        byte[] outputByteArray = compressionJack.decompress(obj.toByteArray());
        Assert.assertNotNull(outputByteArray);
        Assert.assertEquals(testString, new String(outputByteArray, StandardCharsets.UTF_8));
    }

    /**
     * Test decompression B zip 2.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testDecompressionBZip2() throws IOException {
        String testString = "test-subject";
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        BZip2CompressorOutputStream bzip = new BZip2CompressorOutputStream(obj);
        bzip.write(testString.getBytes(StandardCharsets.UTF_8));
        bzip.close();
        obj.close();
        compressionJack.decompress(obj.toByteArray());
    }

    /**
     * Test decompression default.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test
    public void testDecompressionDefault() throws IOException {
        String testString = "test-subject";
        byte[] outputByteArray = compressionJack.decompress(testString.getBytes(StandardCharsets.UTF_8));
        Assert.assertNotNull(outputByteArray);
        Assert.assertEquals(testString, new String(outputByteArray, StandardCharsets.UTF_8));
    }

    /**
     * Test decompression zlib.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test
    public void testDecompressionZlib() throws IOException {
        String testString = "test-subject";
        Deflater deflater = new Deflater();
        deflater.setInput(testString.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream obj = new ByteArrayOutputStream(testString.length());
        deflater.finish();
        byte[] buffer = new byte[Constants.BYTE_1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            obj.write(buffer, 0, count);
        }
        obj.close();
        byte[] outputByteArray = compressionJack.decompress(obj.toByteArray());
        Assert.assertNotNull(outputByteArray);
        Assert.assertEquals(testString, new String(outputByteArray, StandardCharsets.UTF_8));
    }

    /**
     * Test decompression zip.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Test
    public void testDecompressionZip() throws IOException {
        String testString = "test-subject";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zipOut = new ZipOutputStream(baos);
        ByteArrayInputStream bais = new ByteArrayInputStream(testString.getBytes());
        ZipEntry zipEntry = new ZipEntry("TestResults.xml");
        zipOut.putNextEntry(zipEntry);
        byte[] bytes = new byte[Constants.BYTE_1024];
        int length;
        while ((length = bais.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
        }
        zipOut.close();
        bais.close();
        baos.close();
        byte[] outputByteArray = compressionJack.decompress(baos.toByteArray());
        Assert.assertNotNull(outputByteArray);
        Assert.assertEquals(testString, new String(outputByteArray, StandardCharsets.UTF_8));
    }

}
