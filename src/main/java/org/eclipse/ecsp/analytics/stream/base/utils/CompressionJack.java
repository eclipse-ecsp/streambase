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

import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.exception.InputStreamMaxSizeExceededException;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipInputStream;

/**
 * {@link CompressionJack}.
 */

@Component
public class CompressionJack {

    /** The threshold size. */
    @Value("${" + PropertyNames.MAX_DECOMPRESS_INPUT_STREAM_SIZE_IN_BYTES + ":1000000000}")
    private static long thresholdSize;

    /**
     * sniff method.
     *
     * @param bytes bytes
     * @return CompressionType
     * @throws IOException {@link IOException}
     */
    public CompressionType sniff(byte[] bytes) throws IOException {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            short coupleBytes = dis.readShort();
            switch (coupleBytes) {
                case Constants.SHORT_0_X_1_F_8_B:
                    return CompressionType.GZIP;
                case Constants.SHORT_0_X_1_F_9_D:
                    return CompressionType.Z;
                case Constants.SHORT_0_X_425_A:
                    return CompressionType.BZIP2;
                case Constants.SHORT_0_X_7801:
                case Constants.SHORT_0_X_789_C:
                case Constants.SHORT_0_X_78_DA:
                    return CompressionType.ZLIB;
                case Constants.SHORT_0_X_504_B:
                    return CompressionType.ZIP;
                default:
                    return CompressionType.PLAIN;
            }
        }
    }

    /**
     * Decompress.
     *
     * @param bytes the bytes
     * @return the byte[]
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public byte[] decompress(byte[] bytes) throws IOException {
        CompressionType codec = sniff(bytes);
        return codec.decompress(bytes);
    }
    
    /**
     * Sets the threshold size.
     *
     * @param thresholdSizeParam the new threshold size
     */
    // setter for unit test cases
    public static void setThresholdSize(long thresholdSizeParam) {
        thresholdSize = thresholdSizeParam;
    }

    /**
     * {@link CompressionType}.
     */
    public enum CompressionType {
        
        /** The gzip. */
        GZIP(new GzipDecompressor()),
        
        /** The zlib. */
        ZLIB(new ZlibDecompressor()),
        
        /** The zip. */
        ZIP(new ZipDecompressor()),
        
        /** The bzip2. */
        BZIP2(new UnsupportedDecompressor()),
        
        /** The z. */
        Z(new UnsupportedDecompressor()),
        
        /** The plain. */
        PLAIN(new NoOpDecompressor());

        /** The inflater. */
        private Decompressor inflater;

        /**
         * Instantiates a new compression type.
         *
         * @param inflater the inflater
         */
        private CompressionType(Decompressor inflater) {
            this.inflater = inflater;
        }

        /**
         * Decompress.
         *
         * @param bytes the bytes
         * @return the byte[]
         * @throws IOException Signals that an I/O exception has occurred.
         */
        public byte[] decompress(byte[] bytes) throws IOException {
            return inflater.decompress(bytes);
        }
    }

    /**
     * The Interface Decompressor.
     */
    private interface Decompressor {
        
        /**
         * Decompress.
         *
         * @param in the in
         * @return the byte[]
         * @throws IOException Signals that an I/O exception has occurred.
         */
        public byte[] decompress(byte[] in) throws IOException;
    }

    /**
     * The Class BaseDecompressor.
     */
    private abstract static class BaseDecompressor implements Decompressor {

        /**
         * Decompress.
         *
         * @param in the in
         * @return the byte[]
         * @throws IOException Signals that an I/O exception has occurred.
         */
        @Override
        public byte[] decompress(byte[] in) throws IOException {
            try (InputStream gin = createDecompressionInputStream(in)) {
                ByteArrayOutputStream out = new ByteArrayOutputStream(Constants.INT_6144);
                byte[] buffer = new byte[Constants.INT_6144];
                int nr = 0;
                while ((nr = gin.read(buffer, 0, Constants.INT_6144)) != Constants.NEGATIVE_ONE) {
                    out.write(buffer, 0, nr);
                }
                return out.toByteArray();
            }
        }

        /**
         * Creates the decompression input stream.
         *
         * @param in the in
         * @return the input stream
         * @throws IOException Signals that an I/O exception has occurred.
         */
        public abstract InputStream createDecompressionInputStream(byte[] in) throws IOException;

    }

    /**
     * The Class GzipDecompressor.
     */
    private static final class GzipDecompressor extends BaseDecompressor {

        /**
         * Creates the decompression input stream.
         *
         * @param in the in
         * @return the input stream
         * @throws IOException Signals that an I/O exception has occurred.
         */
        @Override
        public InputStream createDecompressionInputStream(byte[] in) throws IOException {
            return new GZIPInputStream(new BufferedInputStream(new ByteArrayInputStream(in)));
        }
    }

    /**
     * The Class ZlibDecompressor.
     */
    private static final class ZlibDecompressor extends BaseDecompressor {

        /**
         * Creates the decompression input stream.
         *
         * @param in the in
         * @return the input stream
         * @throws IOException Signals that an I/O exception has occurred.
         */
        @Override
        public InputStream createDecompressionInputStream(byte[] in) throws IOException {
            return new InflaterInputStream(new BufferedInputStream(new ByteArrayInputStream(in)));
        }

    }

    /**
     * The Class ZipDecompressor.
     */
    private static final class ZipDecompressor extends BaseDecompressor {
        
        /** The logger. */
        private static IgniteLogger logger = IgniteLoggerFactory.getLogger(ZipDecompressor.class);
        
        /** The Constant THRESHOLD_SIZE. */
        private static final long THRESHOLD_SIZE = CompressionJack.thresholdSize;
        
        /** The total size archive. */
        int totalSizeArchive = 0;

        /**
         * Creates the decompression input stream.
         *
         * @param in the in
         * @return the input stream
         * @throws IOException Signals that an I/O exception has occurred.
         */
        @Override
        public InputStream createDecompressionInputStream(byte[] in) throws IOException {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(in);
            if (isValidSizeOfStream(inputStream)) {
                ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new ByteArrayInputStream(in)));
                zis.getNextEntry();
                return zis;
            } else {
                throw new InputStreamMaxSizeExceededException("Threshold for size of input stream is : " 
                        + THRESHOLD_SIZE + "The current size of input stream is : " +  totalSizeArchive 
                        + " and has exceeded the security threshold");
            }
        }
        
        /**
         * Checks if is valid size of stream.
         *
         * @param byteInputStream the byte input stream
         * @return true, if is valid size of stream
         * @throws IOException Signals that an I/O exception has occurred.
         */
        private boolean isValidSizeOfStream(ByteArrayInputStream byteInputStream) throws IOException {
            logger.debug("Validating size of input stream for ZipDecompressor against threshold size : {}", 
                    THRESHOLD_SIZE);
            if (THRESHOLD_SIZE < 1) {
                return true;
            }
            int numBytes = Constants.NEGATIVE_ONE;
            byte[] buffer = new byte[Constants.BYTE_1024];
            int currTotalSizeArchive = 0;
            while ((numBytes = byteInputStream.read(buffer)) > 0) {
                currTotalSizeArchive += numBytes;
                if (currTotalSizeArchive > THRESHOLD_SIZE) {
                    logger.error("Current size : {} has exceeded the security threshold for size "
                            + "during ZipDecompressor", currTotalSizeArchive);
                    totalSizeArchive = currTotalSizeArchive;
                    return false;
                }
            }
            logger.debug("Size of input stream for ZipDecompressor is : {}", currTotalSizeArchive);
            return true;
        }
    }

    /**
     * The Class NoOpDecompressor.
     */
    private static final class NoOpDecompressor implements Decompressor {
        
        /**
         * Decompress.
         *
         * @param in the in
         * @return the byte[]
         * @throws IOException Signals that an I/O exception has occurred.
         */
        @Override
        public byte[] decompress(byte[] in) throws IOException {
            return in;
        }
    }

    /**
     * The Class UnsupportedDecompressor.
     */
    private static final class UnsupportedDecompressor implements Decompressor {
        
        /**
         * Decompress.
         *
         * @param in the in
         * @return the byte[]
         * @throws IOException Signals that an I/O exception has occurred.
         */
        @Override
        public byte[] decompress(byte[] in) throws IOException {
            throw new UnsupportedOperationException("Unsupported compression format");
        }
    }
}
