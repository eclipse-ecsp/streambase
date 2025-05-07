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

package org.eclipse.ecsp.analytics.stream.base.discovery;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessorFilter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * The purpose of this class is to chain the mandatory pre and post processors along with the service processors.
 * The pre and post processor classes are pluggable via the following configs exposed by the stream-base
 * library: {@link PropertyNames#PRE_PROCESSORS} and {@link PropertyNames#POST_PROCESSORS}.
 * 
 * <p>
 * In between pre and post processors, service integrating the stream-base library can provide its own 
 * {@link StreamProcessor} which will be chained like: pre-processors -> service processor -> post-processors.
 * </p>
 *
 * @author avadakkootko
 * @param <KIn> the type parameter for incoming key.
 * @param <VIn> the type parameter for incoming value.
 * @param <KOut> the type parameter for outgoing key.
 * @param <VOut> the type parameter for outgoing value.
 */
public class PropBasedDiscoveryServiceImpl<KIn, VIn, KOut, VOut> implements 
    StreamProcessorDiscoveryService<KIn, VIn, KOut, VOut> {

    /** The Properties instance. */
    private Properties props;

    /**
     * Sets the properties.
     *
     * @param props the new properties
     */
    @Override
    public void setProperties(Properties props) {
        this.props = props;
    }

    /**
     * Chaining of service's processor nodes in the fashion: 
     * pre-processors nodes-> service stream processor node -> post-processor nodes.
     * This discovery supports both legacy as well as PRE and POST processor approach. Its backward compatible.
     *
     * @return the list of all discovered StreamProcessor
     */
    @Override
    public List<StreamProcessor<KIn, VIn, KOut, VOut>> discoverProcessors() {

        List<StreamProcessor<KIn, VIn, KOut, VOut>> processors = new ArrayList<>();
        Optional<List<StreamProcessor<KIn, VIn, KOut, VOut>>> preProcessors =
                getProcessorsFromProperties(PropertyNames.PRE_PROCESSORS, props);
        if (preProcessors.isPresent()) {
            processors.addAll(preProcessors.get());
        }

        if (props.containsKey(PropertyNames.SERVICE_STREAM_PROCESSORS)) {
            Optional<List<StreamProcessor<KIn, VIn, KOut, VOut>>> serviceProcessors = getProcessorsFromProperties(
                    PropertyNames.SERVICE_STREAM_PROCESSORS, props);
            if (serviceProcessors.isPresent()) {
                processors.addAll(serviceProcessors.get());
            }
        }

        Optional<List<StreamProcessor<KIn, VIn, KOut, VOut>>> postProcessors =
                getProcessorsFromProperties(PropertyNames.POST_PROCESSORS, props);
        if (postProcessors.isPresent()) {
            processors.addAll(postProcessors.get());
        }
        return processors;
    }

    /**
     * Gets the processors from properties.
     *
     * @param processorType the processor type
     * @param props the props
     * @return the processors from properties
     */
    private Optional<List<StreamProcessor<KIn, VIn, KOut, VOut>>>
        getProcessorsFromProperties(String processorType, Properties props) {
        List<StreamProcessor<KIn, VIn, KOut, VOut>> processorList = null;
        String processors = props.getProperty(processorType);
        if (StringUtils.isNotBlank(processors)) {
            processorList = new ArrayList<>();
            String[] processorsArr = processors.split(",");
            for (int i = 0; i < processorsArr.length; i++) {
                try {
                    createProcessorList(props, processorList, processorsArr[i]);
                } catch (InstantiationException | IllegalAccessException 
                        | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
                    throw new IllegalArgumentException("Unable to instantiate processor : " + processorsArr[i], e);
                }
            }
        }
        return Optional.ofNullable(processorList);
    }

    /**
     * Creates the processor list.
     *
     * @param props the props
     * @param processorList the processor list
     * @param processorsArr the processors arr
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     * @throws ClassNotFoundException the class not found exception
     * @throws NoSuchMethodException the no such method exception
     * @throws InvocationTargetException the invocation target exception
     */
    private void createProcessorList(Properties props,
            List<StreamProcessor<KIn, VIn, KOut, VOut>> processorList, String processorsArr)
            throws InstantiationException, IllegalAccessException, 
            ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        Object streamProcessor = getClass().getClassLoader().loadClass(processorsArr)
                .getDeclaredConstructor().newInstance();
        if (streamProcessor instanceof StreamProcessorFilter streamProcessorFilter) {
            if (streamProcessorFilter.includeInProcessorChain(props)) {
                processorList.add((StreamProcessor<KIn, VIn, KOut, VOut>) streamProcessor);
            }
        } else {
            processorList.add((StreamProcessor<KIn, VIn, KOut, VOut>) streamProcessor);
        }
    }

}
