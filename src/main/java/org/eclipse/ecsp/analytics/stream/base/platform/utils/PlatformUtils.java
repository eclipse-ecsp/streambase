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

package org.eclipse.ecsp.analytics.stream.base.platform.utils;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * {@link PlatformUtils} Util class for {@link scala.compat.Platform}.
 */
@Component
public class PlatformUtils {

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(PlatformUtils.class);
    
    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /**
     * getInstanceByClassName():  to get instance by class name.
     *
     * @param canonicalClassName canonicalClassName
     * @return Object
     */
    public Object getInstanceByClassName(String canonicalClassName) {
        logger.info("Attempting to load class {}", canonicalClassName);
        Object instance = null;
        Class<?> classObject = null;
        try {
            classObject = getClass().getClassLoader().loadClass(canonicalClassName);
            instance = ctx.getBean(classObject);
            logger.info("Class {} loaded from spring application context", classObject.getName());
        } catch (Exception ex) {
            try {
                if (classObject == null) {
                    throw new IllegalArgumentException("Could not load the class " + canonicalClassName);
                }
                logger.info("Class {} could not be loaded as spring bean. "
                       + "Attempting to create new instance.", canonicalClassName);
                instance = classObject.getDeclaredConstructor().newInstance();
            } catch (Exception exception) {
                String msg = String.format("Class %s could not be loaded. Not found on classpath.%n",
                        canonicalClassName);
                logger.error(msg + ExceptionUtils.getStackTrace(exception));
                throw new IllegalArgumentException(msg);
            }
        }
        return instance;
    }
}
