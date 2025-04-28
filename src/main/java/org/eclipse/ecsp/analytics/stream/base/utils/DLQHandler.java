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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import jakarta.annotation.PostConstruct;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamBaseConstant;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.domain.IgniteBaseException;
import org.eclipse.ecsp.domain.IgniteExceptionDataV1_0;
import org.eclipse.ecsp.domain.IgniteExceptionDataV1_1;
import org.eclipse.ecsp.domain.NestedDLQExceptionData;
import org.eclipse.ecsp.domain.Version;
import org.eclipse.ecsp.entities.CompositeIgniteEvent;
import org.eclipse.ecsp.entities.EventData;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.transform.GenericIgniteEventTransformer;
import org.eclipse.ecsp.transform.IgniteKeyTransformer;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;

/**
 * This class checks whether failed event needs to be re-processed
 * else forwards it to the DLQ (Dead lettered queue)
 * topic.IgniteBaseException exception class is contract between
 * stream processor and service based processors. In order to proceed with DLQ
 * re-processing , this exception should be thrown with retryable
 * flag set to true else exception will be sent directly to the DLQ topic
 * without re-processing. At the time DLQ re-processing , if the
 * exception is not required to be processed further due to whatsoever reason
 * (business logic etc) ,its mandatory to throw the exception either
 * with IgniteBaseException with retryable set to false or any other
 * exception in order to ensure that this event failure is ultimately
 * stored into DLQ topic for future analysis.
 */
@Component
public class DLQHandler {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(DLQHandler.class);
    
    /** The key transformer. */
    private IgniteKeyTransformer<?> keyTransformer;
    
    /** The mapper. */
    private ObjectMapper mapper = new ObjectMapper();
    
    /** The topic dlq. */
    private String topicDlq;
    
    /** The key transformer impl. */
    @Value("${ignite.key.transformer.class}")
    private String keyTransformerImpl;
    
    /** The service name. */
    @Value("${service.name}")
    private String serviceName;
    
    /** The convert topic to lower case. */
    @Value("${" + PropertyNames.CONVERT_BACKDOOR_KAFKA_TOPIC_TO_LOWERCASE + ":true}")
    private boolean convertTopicToLowerCase;
    
    /** The max retry count. */
    @Value("${" + PropertyNames.DLQ_MAX_RETRY_COUNT + ":5}")
    private int maxRetryCount;
    
    /** The reprocessing enabled. */
    @Value("${" + PropertyNames.DLQ_REPROCESSING_ENABLED + ":false}")
    private boolean reprocessingEnabled;
    
    /** The transformer. */
    @Autowired
    private GenericIgniteEventTransformer transformer;

    /**
     * Gets the max retry count.
     *
     * @return the max retry count
     */
    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    /**
     * Sets the max retry count.
     *
     * @param maxRetryCount the new max retry count
     */
    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    /**
     * Checks if is reprocessing enabled.
     *
     * @return true, if is reprocessing enabled
     */
    public boolean isReprocessingEnabled() {
        return reprocessingEnabled;
    }

    /**
     * Sets the reprocessing enabled.
     *
     * @param reprocessingEnabled the new reprocessing enabled
     */
    public void setReprocessingEnabled(boolean reprocessingEnabled) {
        this.reprocessingEnabled = reprocessingEnabled;
    }

    /**
     * init().
     */
    @PostConstruct
    public void init() {
        try {
            keyTransformer = (IgniteKeyTransformer) getClass().getClassLoader().loadClass(keyTransformerImpl)
                    .getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException 
                | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(PropertyNames.IGNITE_KEY_TRANSFORMER 
                    + " refers to a class that is not available on the classpath", e);
        }
        this.mapper.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));
        this.topicDlq = serviceName + StreamBaseConstant.DLQ_TOPIC_POSFIX;
        if (convertTopicToLowerCase) {
            this.topicDlq = this.topicDlq.toLowerCase();
        }
    }

    /**
     * It forwards the failed event to DLQ topic.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param context the context
     * @param key key
     * @param value value
     * @param ex ex
     */
    public <K, V> void forwardToDlq(StreamProcessingContext<?, ?> context, K key, V value, Exception ex) {
        try {
            @SuppressWarnings("rawtypes")
            IgniteKey igniteKey = null;
            IgniteExceptionDataV1_0 exceptiondata = new IgniteExceptionDataV1_0();
            if (key == null) {
                igniteKey = keyTransformer.fromBlob((Constants.DLQ_NULL_KEY + System.currentTimeMillis()).getBytes());
            }
            if (key instanceof byte[] bytes) {
                igniteKey = keyTransformer.fromBlob(bytes);
                exceptiondata.setRawData(value);
            } else if (key instanceof IgniteKey<?> igniteKey1) {
                igniteKey = igniteKey1;
                if (value instanceof IgniteEventImpl igniteEventImpl) {
                    exceptiondata.setIgniteEvent(igniteEventImpl);
                } else if (value instanceof CompositeIgniteEvent compositeIgniteEvent) {
                    exceptiondata.setCompositeIgniteEvent(compositeIgniteEvent);
                }
            }
            IgniteEventImpl eventImpl = new IgniteEventImpl();
            eventImpl.setEventId(EventID.IGNITE_EXCEPTION_EVENT);
            exceptiondata.setErrorTimeInMilis(System.currentTimeMillis());
            exceptiondata.setException(ex);
            eventImpl.setEventData(exceptiondata);
            eventImpl.setVersion(Version.V1_0);
            logger.debug("Forwarding to DLQ {} key {} value {}", topicDlq, igniteKey, eventImpl);
            context.forwardDirectly(igniteKey, eventImpl, topicDlq);
        } catch (Exception exception) {
            logger.error("Unexpected error for key {} value {}", key, value, exception);
        }
    }

    /**
     * This method decides whether the event exception can sent for
     * DLQ re-processing or should be forwarded to DLQ topic directly.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param context context
     * @param key key
     * @param value value
     * @param ex ex
     * @param processorName processorName
     */
    public <K, V> void forwardToDlq(StreamProcessingContext<?, ?> context, K key, V value, 
            Exception ex, String processorName) {
        logger.error(
                "Failed to process key {} with value {} by worker:{} "
                        + "with exception. Checking DLQ reprocessing criteria before forwarding to DLQ !",
                key, value, processorName, ex);
        if (checkIfDLQReprocessingRequired(key, value, ex)) {
            IgniteEvent igniteEvent = (IgniteEvent) value;
            logger.debug(igniteEvent, "Initiating the DLQ re-processing for key {}", key);
            performDlqReprocessing(context, (IgniteKey) key, igniteEvent, (IgniteBaseException) ex, processorName);
        } else {
            logger.info("Forwarding to DLQ topic for key {}", key);
            forwardToDlq(context, key, value, ex);
        }
    }

    /**
     * Below conditions are checked: 1> dlq.reprocessing.enabled
     * instance of byte[] i.e. previously event didn't failed at the
     * TaskContextInitializer or ProtocolTranslatorPreProcessor. 4>
     * Exception should be retractable IgniteBaseException.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param key key
     * @param value value
     * @param ex The exception
     * @return boolean Returns true in case DLQ Reprocessing needs to be performed.
     */
    public <K, V> boolean checkIfDLQReprocessingRequired(K key, V value, Exception ex) {

        if (reprocessingEnabled && key != null && value != null
                && !(key instanceof byte[]) && ex instanceof IgniteBaseException
                && ((IgniteBaseException) ex).isRetryable()) {
            int retryCount = 0;
            IgniteEvent igniteEvent = (IgniteEvent) value;
            if (igniteEvent.getEventData() instanceof IgniteExceptionDataV1_1 igniteExceptionDataV1) {
                retryCount = igniteExceptionDataV1.getRetryCount();
            }
            if (retryCount < maxRetryCount) {
                logger.info("DLQ Reprocessing criteria are satisfied. Forwarding the key: "
                                + "{} retryCount {} maxRetryCount {} for DLQ reprocessing",
                        key, retryCount, maxRetryCount);
                return true;
            }
        }
        return false;
    }

    /**
     * This method performs DLQ re-processing and forwards the event along
     * with the Exception data so that informed decision can be made
     * during re-processing by service.
     *
     * @param context context
     * @param key key
     * @param value value
     * @param ex ex
     * @param processorName processorName
     */
    public void performDlqReprocessing(StreamProcessingContext<?, ?> context, IgniteKey<?> key, 
            IgniteEvent value, IgniteBaseException ex, String processorName) {
        IgniteEventImpl clonedIgniteEventImpl = null;
        IgniteExceptionDataV1_1 igniteWrapperExceptionData = new IgniteExceptionDataV1_1();
        IgniteExceptionDataV1_1 igniteExceptionData = null;
        EventData data = value.getEventData();
        byte[] igniteEventBlob;

        if (data instanceof IgniteExceptionDataV1_1 exceptionData) {
            /*
             * Exception is retried atleast once.
             */
            if (value instanceof IgniteEventImpl igniteEvent) {
                clonedIgniteEventImpl = igniteEvent;
                igniteExceptionData = (IgniteExceptionDataV1_1) clonedIgniteEventImpl.getEventData();
                igniteEventBlob = getBlobForIgniteEventImpl(ex, igniteWrapperExceptionData, igniteExceptionData);
            } else {
                igniteExceptionData = new IgniteExceptionDataV1_1();
                if (ex.getIgniteEvent() != null) {
                    igniteWrapperExceptionData.setCompositeIgniteEvent((CompositeIgniteEvent) ex.getIgniteEvent());
                    igniteExceptionData = (IgniteExceptionDataV1_1) ex.getIgniteEvent().getEventData();
                } else {
                    if (igniteExceptionData.getCompositeIgniteEvent() != null) {
                        igniteWrapperExceptionData
                        .setCompositeIgniteEvent(igniteExceptionData.getCompositeIgniteEvent());
                    }
                }
                igniteEventBlob = transformer.toBlob(igniteWrapperExceptionData.getCompositeIgniteEvent());
            }
            /*
             * Overwriting the nestedEvent, we are maintaining the previous
             * event exception info.
             */
            NestedDLQExceptionData nestedDlqExceptionData = new NestedDLQExceptionData(igniteEventBlob,
                    igniteExceptionData.getRetryCount(), igniteExceptionData.getProcessorName(),
                    igniteExceptionData.getException(), igniteExceptionData.getContext());

            igniteWrapperExceptionData.setNestedDLQExceptionData(nestedDlqExceptionData);
            igniteWrapperExceptionData.setRetryCount(exceptionData.getRetryCount() + 1);

        } else {
            /*
             * Exception is not re-processed yet.
             */
            setIgniteWrapperExceptionData(value, igniteWrapperExceptionData);
        }

        igniteWrapperExceptionData.setErrorTimeInMilis(System.currentTimeMillis());
        igniteWrapperExceptionData.setException((Exception) ex.getCause());
        igniteWrapperExceptionData.setProcessorName(processorName);
        igniteWrapperExceptionData.setContext((ex).getServiceContext());
        IgniteEventImpl igniteExceptionEvent = new IgniteEventImpl();
        igniteExceptionEvent.setVersion(Version.V1_1);
        igniteExceptionEvent.setEventId(EventID.IGNITE_EXCEPTION_EVENT);
        igniteExceptionEvent.setEventData(igniteWrapperExceptionData);
        IgniteEvent igniteEvent = igniteWrapperExceptionData.getIgniteEvent();
        igniteExceptionEvent.setVehicleId(igniteEvent.getVehicleId());
        igniteExceptionEvent.setRequestId(igniteEvent.getRequestId());
        igniteExceptionEvent.setBizTransactionId(igniteEvent.getBizTransactionId());
        igniteExceptionEvent.setMessageId(igniteEvent.getMessageId());
        igniteExceptionEvent.setCorrelationId(igniteEvent.getCorrelationId());
        igniteExceptionEvent.setTimezone(igniteEvent.getTimezone());
        igniteExceptionEvent.setTimestamp(igniteEvent.getTimestamp());
        logger.debug("Forwarding for DLQ reprocessing {} key {} value {}"
                        + " retryCount {} maxRetryCount {}", context.streamName(),
                key, igniteExceptionEvent,
                igniteWrapperExceptionData.getRetryCount(), maxRetryCount);
        context.forwardDirectly(key, igniteExceptionEvent, context.streamName());

    }

    /**
     * Sets the ignite wrapper exception data.
     *
     * @param value the value
     * @param igniteWrapperExceptionData the ignite wrapper exception data
     */
    private static void setIgniteWrapperExceptionData(IgniteEvent value, 
            IgniteExceptionDataV1_1 igniteWrapperExceptionData) {
        CompositeIgniteEvent clonedCompositeIgniteEvent;
        if (value instanceof IgniteEventImpl clonedIgniteEventImpl) {
            igniteWrapperExceptionData.setIgniteEvent(clonedIgniteEventImpl);
        } else {
            clonedCompositeIgniteEvent = (CompositeIgniteEvent) value;
            igniteWrapperExceptionData.setCompositeIgniteEvent(clonedCompositeIgniteEvent);
        }
        igniteWrapperExceptionData.setRetryCount(1);
    }

    /**
     * Gets the blob for ignite event impl.
     *
     * @param ex the ex
     * @param igniteWrapperExceptionData the ignite wrapper exception data
     * @param igniteExceptionData the ignite exception data
     * @return the blob for ignite event impl
     */
    private byte[] getBlobForIgniteEventImpl(IgniteBaseException ex, IgniteExceptionDataV1_1 igniteWrapperExceptionData,
            IgniteExceptionDataV1_1 igniteExceptionData) {
        byte[] igniteEventBlob;
        if (ex.getIgniteEvent() != null) {
            igniteWrapperExceptionData.setIgniteEvent((IgniteEventImpl) ex.getIgniteEvent());
        } else {
            igniteWrapperExceptionData.setIgniteEvent(igniteExceptionData.getIgniteEvent());
        }
        igniteEventBlob = transformer.toBlob(igniteWrapperExceptionData.getIgniteEvent());
        return igniteEventBlob;
    }

}
