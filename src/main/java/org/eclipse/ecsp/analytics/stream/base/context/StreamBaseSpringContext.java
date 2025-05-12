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

package org.eclipse.ecsp.analytics.stream.base.context;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * If we want to access a spring managed class from a non-spring class then that can be achieved by.
 * {@link StreamBaseSpringContext#getBean(Class)}
 */
@Component
public class StreamBaseSpringContext implements ApplicationContextAware {

    private static ApplicationContext context;

    /**
     * Retrieves a Spring-managed bean of the specified class type.
     *
     * @param <T>       the type of the bean to retrieve
     * @param beanClass the class type of the bean to retrieve
     * @return the Spring-managed bean instance of the specified type
     * @throws BeansException if the bean could not be created or retrieved
     */
    public static <T extends Object> T getBean(Class<T> beanClass) {
        return context.getBean(beanClass);
    }

    /**
     * Sets the Spring's {@link ApplicationContext}.
     *
     * @param context the {@link ApplicationContext}.
     */
    private static synchronized void setContext(ApplicationContext context) {
        StreamBaseSpringContext.context = context;
    }

    /**
     * Sets the Spring's {@link ApplicationContext}.
     *
     * @param context the {@link ApplicationContext}.
     */
    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        // store ApplicationContext reference to access required beans later on
        setContext(context);
    }
}
