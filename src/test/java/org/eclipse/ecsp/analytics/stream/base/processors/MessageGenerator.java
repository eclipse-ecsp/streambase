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

package org.eclipse.ecsp.analytics.stream.base.processors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.domain.BlobDataV1_0;
import org.eclipse.ecsp.domain.IgniteEventSource;
import org.eclipse.ecsp.entities.IgniteBlobEvent;
import org.eclipse.ecsp.serializer.IngestionSerializerFstImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;



/**
 * class MessageGenerator.
 */
public class MessageGenerator {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageGenerator.class);
    
    /** The exec. */
    private static ScheduledExecutorService exec = null;

    /**
     * main().
     *
     * @param args args
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        exec = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                String name = Thread.currentThread().getName();
                t.setName("pool:" + name);
                t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        LOGGER.error("Uncaught exception for pool thread " + t.getName(), e);
                    }
                });
                return t;
            }
        });
        // Push data to every 5 seconds
        exec.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    produce(args);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Exception in punctuateData", e);
                }
            }
        }, Constants.SIXTY, Constants.FIVE, TimeUnit.SECONDS);

        int argsLength = args.length;
        if (argsLength != Constants.FIVE) {
            LOGGER.error("Expecting 4 arguements, but received {}", argsLength);
            usage();
            return;
        }
        produce(args);

    }

    /**
     * usage().
     */
    public static void usage() {
        String textBlock = """
            Requires 5 arguements.
                    1) Kafka topic name
                    2) Key
                    3) Value
                    4) bootstrapserver
                    5) sslEnabled
               """;
        System.out.println(textBlock);
    }

    /**
     * produce().
     *
     * @param args args
     * @throws ExecutionException ExecutionException
     * @throws InterruptedException InterruptedException
     */
    public static void produce(String[] args) throws ExecutionException, InterruptedException {
        String topicName = args[0];
        String key = args[1];
        String value = args[Constants.TWO];
        String bootstrapServers = args[Constants.THREE];
        boolean sslEnabled = false;
        try {
            sslEnabled = Boolean.parseBoolean(args[Constants.FOUR]);
        } catch (Exception e) {
            LOGGER.error("SSL enabling error");
        }

        LOGGER.info("Sending key={}, value={} to the topic {}", key, value, topicName);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Serdes.ByteArray().serializer().getClass().getName());

        if (sslEnabled) {
            producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            producerProps.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
            producerProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/kafka/ssl/kafka.client.keystore.jks");
            producerProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "shcuwNHARcNuag8SgYdsG8cWuPExY3Tx");
            producerProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "pUBPHXM9mP5PrRBrTEpF5cV2TpjvWtb5");
            producerProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/kafka/ssl/kafka.client.truststore.jks");
            producerProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "9vq9ghbSFd7JMFSgGMSCEuAzE3q27Xd3");
        }


        IgniteBlobEvent igniteBlobData = new IgniteBlobEvent();
        igniteBlobData.setSourceDeviceId(key);
        BlobDataV1_0 blobdatav10 = new BlobDataV1_0();
        blobdatav10.setEventSource(IgniteEventSource.IGNITE);
        blobdatav10.setPayload(value.getBytes());
        igniteBlobData.setEventData(blobdatav10);
        igniteBlobData.setVersion(org.eclipse.ecsp.domain.Version.V1_0);
        igniteBlobData.setRequestId(key + "-id");
        byte[] serialedBtyes = new IngestionSerializerFstImpl().serialize(igniteBlobData);

        ProducerRecord<byte[], byte[]> data = new ProducerRecord<byte[], byte[]>(
                topicName, (key).getBytes(), serialedBtyes);

        Producer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(producerProps);
        Future<RecordMetadata> future = producer.send(data);
        LOGGER.info("future.get ={}, data={} to the topic {}", future.get(), data, topicName);

    }

    /**
     * Instantiates a new message generator.
     */
    private MessageGenerator() {

    }
}
