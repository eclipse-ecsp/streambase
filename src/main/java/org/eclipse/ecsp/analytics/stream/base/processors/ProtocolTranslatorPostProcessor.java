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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidKeyOrValueException;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.threadlocal.ContextKey;
import org.eclipse.ecsp.analytics.stream.threadlocal.TaskContextHandler;
import org.eclipse.ecsp.domain.IgniteEventSource;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.transform.IgniteKeyTransformer;
import org.eclipse.ecsp.transform.Transformer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * ProtocolTranslatorPostProcessor is one of the post processors
 * who intercepts the IgniteEvent and IgniteKey and converts it to byte array
 * format before pushing it to the streaming brocker (eg : kafka).
 *
 * @author avadakkootko
 * @param <T> the generic type
 */

public class ProtocolTranslatorPostProcessor<T> implements StreamProcessor<IgniteKey<T>, IgniteEvent, byte[], byte[]> {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(ProtocolTranslatorPostProcessor.class);
    
    /** The spc. */
    private StreamProcessingContext<byte[], byte[]> spc;

    /** The handler. */
    private TaskContextHandler handler;
    
    /** The task id. */
    private String taskId;

    /** The transformer map. */
    private Map<String, Transformer> transformerMap = new HashMap<>();
    
    /** The ignite key transformer. */
    private Optional<IgniteKeyTransformer<T>> igniteKeyTransformer;

    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /**
     * Inits the.
     *
     * @param spc the spc
     */
    @Override
    public void init(StreamProcessingContext<byte[], byte[]> spc) {
        this.spc = spc;
        taskId = spc.getTaskID();
        handler = TaskContextHandler.getTaskContextHandler();
    }

    /**
     * Name.
     *
     * @return the string
     */
    @Override
    public String name() {
        return "protocol-translator-post-processor";
    }

    /**
     * In the process method IgniteKey and IgniteEvent is received which
     * is then converted to byte[] format to push to the streaming
     * broker.
     * IgniteEventTransformer and IgniteKeyTransformer is mandatory.
     * Hence if it is not available at initialization, run time exception is
     * thrown and process is exited. This is the same reason why a default transformer is not used.
     *
     * @param kafkaRecord the kafka record
     */
    @Override
    public void process(Record<IgniteKey<T>, IgniteEvent> kafkaRecord) {
        if (null == kafkaRecord) {
            logger.error("Input record to ProtocolTranslatorPostProcessor cannot be null");
            throw new IllegalArgumentException("Input record to ProtocolTranslatorPostProcessor cannot be null");
        }
        IgniteKey<T> key = kafkaRecord.key();
        IgniteEvent value = kafkaRecord.value();
        Optional<Object> sinkTopic = handler.getValue(taskId, ContextKey.KAFKA_SINK_TOPIC);
        if (sinkTopic.isPresent()) {
            if (key != null && value != null) {
                logger.debug(value, "Received key {}, value {} in ProtocolTranslatorPostProcessor", key, value);

                byte[] keyInbytes = null;
                byte[] msgInBytes = null;
                // igniteKeyTransformer availability and validation done in
                // igniteconfig
                keyInbytes = igniteKeyTransformer.get().toBlob(key);
                if (keyInbytes == null) {
                    logger.error("IgniteKeyTransformer returned null key for igniteKey : {}", key);
                    throw new InvalidKeyOrValueException("IgniteKeyTransformer returned null key for "
                            + "igniteKey : " + key);
                }

                // ProtocolPosttranslator always uses BlobSource Ignite
                Transformer trans = transformerMap.get(IgniteEventSource.IGNITE);
                msgInBytes = getMsgInBytes(value, trans);
                if (msgInBytes == null) {
                    logger.error(
                            "Transformed value in ProtocolTranslatorPostProcessor "
                                    + "from igniteEvent cannot be null for key {}, value {}",
                            key, value);
                    throw new InvalidKeyOrValueException(
                            "Transformed value in ProtocolTranslatorPostProcessor from igniteEvent cannot be null");
                }
                String topic = sinkTopic.get().toString();
                logger.debug(value, "Ignite Key {} and Ignite Value {} after "
                                + "conversion to byte array is being forwarded to topic {}", key,
                        value, topic);
                spc.forward(new Record<>(keyInbytes, msgInBytes, kafkaRecord.timestamp()), topic);
            } else {
                logger.error("key or value to be processed in ProtocolTranslatorPostProcessor cannot be null");
                throw new InvalidKeyOrValueException("key or value to be processed in ProtocolTranslatorPostProcessor "
                        + "cannot be null");
            }
        }

    }

    /**
     * Gets the msg in bytes.
     *
     * @param value the value
     * @param trans the trans
     * @return the msg in bytes
     */
    private static byte[] getMsgInBytes(IgniteEvent value, Transformer trans) {
        byte[] msgInBytes;
        if (trans != null) {
            msgInBytes = trans.toBlob(value);
        } else {
            logger.error("Transformer Implementation for IgniteEvent to byte[] with source IGNITE is missing");
            throw new InvalidKeyOrValueException(
                    "Transformer Implementation for IgniteEvent to byte[] with source IGNITE is missing");
        }
        return msgInBytes;
    }

    /**
     * Inits the config.
     *
     * @param properties the properties
     */
    @Override
    public void initConfig(Properties properties) {
        String transformerList = properties.getProperty(PropertyNames.EVENT_TRANSFORMER_CLASSES);
        String igniteKeyTransformerImpl = properties.getProperty(PropertyNames.IGNITE_KEY_TRANSFORMER);

        if (StringUtils.isBlank(transformerList)) {
            logger.error("Event transformer list cannot be blank");
            throw new IllegalArgumentException("Event transformer list cannot be blank");
        }

        if (StringUtils.isBlank(igniteKeyTransformerImpl)) {
            logger.error("Ignite key transformer cannot be blank");
            throw new IllegalArgumentException("Ignite key transformer cannot be blank");
        }

        logger.info("Event transformer List from property file : {}", transformerList);
        logger.info("Ignite key transformer from property file : {}", igniteKeyTransformerImpl);

        IgniteKeyTransformer<T> kt = null;

        // Initialize transformers from List
        String[] transformerArr = transformerList.split(",");
        boolean transformerInjectPropertyFlg = 
                Boolean.parseBoolean(properties.getProperty(PropertyNames.TRANSFORMER_INJECT_PROPERTY_ENABLE));
        for (String transformer : transformerArr) {
            Transformer t = null;
            // Flag which will ensure that the instances of transformers
            // created via runtime class loader( Non Spring Based Bean
            // creation) will have the properties available to it via the
            // parameterized constructor.
            if (transformerInjectPropertyFlg) {
                try {
                    logger.info("Loading parameterized constructor for transformer");
                    t = (Transformer) ctx.getAutowireCapableBeanFactory().getBean(transformer, properties);
                } catch (Exception ex) {
                    throw new IllegalArgumentException(
                            transformer + " parameterized constructor is not available to accept Properties.");
                }
            } else {
                t = (Transformer) ctx.getAutowireCapableBeanFactory().getBean(transformer);
            }

            transformerMap.put(t.getSource(), t);
        }
        // Initialize IgniteKeyTransformer is present
        if (StringUtils.isNotBlank(igniteKeyTransformerImpl)) {
            try {
                kt = (IgniteKeyTransformer) getClass().getClassLoader().loadClass(igniteKeyTransformerImpl)
                        .getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException 
                    | ClassNotFoundException | InvocationTargetException | NoSuchMethodException e) {
                logger.error(PropertyNames.IGNITE_KEY_TRANSFORMER + " refers to a class that is "
                        + "not available on the classpath");
            }
        }
        igniteKeyTransformer = Optional.ofNullable(kt);
    }

    /**
     * Punctuate.
     *
     * @param timestamp the timestamp
     */
    @Override
    public void punctuate(long timestamp) {
        //overridden method
    }

    /**
     * Close.
     */
    @Override
    public void close() {
        //overridden method
    }

    /**
     * Config changed.
     *
     * @param props the props
     */
    @Override
    public void configChanged(Properties props) {
        //overridden method
    }

    /**
     * Creates the state store.
     *
     * @return the harman persistent KV store
     */
    @Override
    public HarmanPersistentKVStore createStateStore() {
        return null;
    }

}
