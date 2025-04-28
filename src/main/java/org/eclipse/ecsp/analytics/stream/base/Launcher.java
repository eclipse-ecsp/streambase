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

package org.eclipse.ecsp.analytics.stream.base;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.eclipse.ecsp.analytics.stream.base.idgen.MessageIdGenerator;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.eclipse.ecsp.analytics.stream.base.utils.Constants.COLON_9100;

/**
 * Entry point for running the stream processors.
 *
 * @author ssasidharan
 */
@Configuration
@ComponentScan(basePackages = { "org.eclipse.ecsp" }, excludeFilters
        = {@ComponentScan.Filter(type = FilterType.REGEX, pattern = "org.eclipse.ecsp.hashicorp.*") })
@PropertySource("classpath:/application-base.properties")
@PropertySource(ignoreResourceNotFound = true, value = "classpath:/application.properties")
@PropertySource(ignoreResourceNotFound = true, value = "classpath:/application-ext.properties")
@Component
public class Launcher {

    /** The Constant VALUE_END. */
    private static final String VALUE_END = "}";
    
    /** The Constant VALUE_START. */
    private static final String VALUE_START = "${";

    /** The launcher class name. */
    @Value("${launcher.impl.class.fqn}")
    private String launcherClassName;
    
    /** The shutdown hook wait time ms. */
    @Value("${shutdown.hook.wait.ms}")
    private long shutdownHookWaitTimeMs;
    
    /** Indicates execution of shutdown. */
    @Value("${exec.shutdown.hook}")
    private boolean executeShutdownHook;
    
    /** The message id generator type. */
    @Value("${messageid.generator.type}")
    private String messageIdGeneratorType;
    
    /** The env. */
    @Autowired
    private Environment env;
    
    /** The ctx. */
    @Autowired
    private ApplicationContext ctx;

    /** The prometheus export server. */
    private HTTPServer prometheusExportServer;

    /** Prometheus Agent Port Number. **/
    @Value(VALUE_START + PropertyNames.PROMETHEUS_AGENT_PORT_KEY + COLON_9100 + VALUE_END)
    private int prometheusExportPort;

    /** The enable prometheus. */
    @Value(VALUE_START + PropertyNames.ENABLE_PROMETHEUS + VALUE_END)
    private boolean enablePrometheus;

    /** The dynamic props. */
    protected static Properties dynamicProps = new Properties();
    
    /** The {@link LauncherProvider} implementation. */
    volatile LauncherProvider provider = null;

    /** The Constant LOGGER. */
    private static final IgniteLogger LOGGER = IgniteLoggerFactory.getLogger(Launcher.class);

    /**
     * inMemoryPropertySource().
     *
     * @param cenv the cenv
     * @return EnumerablePropertySource
     */
    @Bean
    @Lazy(value = false)
    public EnumerablePropertySource<Map<String, Object>> inMemoryPropertySource(ConfigurableEnvironment cenv) {
        MutablePropertySources propSources = cenv.getPropertySources();
        PropertiesPropertySource pps = new PropertiesPropertySource("in-mem", dynamicProps);
        propSources.addFirst(pps);
        return pps;
    }

    /**
     * Creates a bean of {@link MessageIdGenerator} implementation.
     *
     * @return MessageIdGenerator
     */
    @Bean
    public MessageIdGenerator msgIdGenerator() {
        try {
            Class<?> c = getClass().getClassLoader().loadClass(messageIdGeneratorType);
            return (MessageIdGenerator) ctx.getBean(c);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("MessageId generator type " + messageIdGeneratorType + " ,is undefined");
        }
    }
    
    /**
     * Entry point for stream processors.

     * @param args Arguments to main method
     * @throws Exception exception
     */
    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctxt = new AnnotationConfigApplicationContext(Launcher.class);
        Launcher l = ctxt.getBean(Launcher.class);
        l.launch();
    }

    /**
     * Extract all the properties supplied via ".properties" file & config-map and, launch the 
     * streams application.
     *
     * @throws IllegalArgumentException if {@link LauncherProvider} implementation isn't available on the
     *                                  classpath.
     */

    public void launch() throws IllegalArgumentException {
        Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e) ->
            LOGGER.error("Uncaught exception for thread " + t.getName(), e));
        
        if (executeShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutdown hook executing");
                closeStream();
                LOGGER.info("Shutdown hook waiting");
                try {
                    Thread.sleep(shutdownHookWaitTimeMs);
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted when waiting in shutdown hook");
                    Thread.currentThread().interrupt();
                }
                LOGGER.info("Shutdown hook complete");
            }));
        }

        Properties props = extractProperties(env);
        try {
            Class<?> c = getClass().getClassLoader().loadClass(launcherClassName);
            provider = (LauncherProvider) ctx.getBean(c);
            if (enablePrometheus) {
                prometheusExportServer = new HTTPServer(prometheusExportPort, true);
                DefaultExports.initialize();
            }
            provider.launch(props);

        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(PropertyNames.LAUNCHER_IMPL
                    + " refers to a class that is not available on the classpath");
        } catch (IllegalArgumentException e1) {
            throw e1;
        } catch (IOException ie) {
            LOGGER.error("IOException occurred ", ie);
        }
    }

    /**
     * Extract properties.
     *
     * @param env the env
     * @return the properties
     */
    private Properties extractProperties(Environment env) {
        Properties props = new Properties();
        MutablePropertySources propSrcs = ((AbstractEnvironment) env).getPropertySources();
        for (org.springframework.core.env.PropertySource<?> ps : propSrcs) {
            if (ps instanceof EnumerablePropertySource<?> propertySource) {
                for (String pn : propertySource.getPropertyNames()) {
                    props.setProperty(pn, env.getProperty(pn));
                }
            }
        }
        props.putAll(dynamicProps);
        return props;
    }

    /**
     * Terminates the streams application.
     */
    public void closeStream() {
        if (provider != null) {
            LOGGER.info("Asked to terminate");
            provider.terminate();
        }
        if (null != prometheusExportServer) {
            LOGGER.info("Asked Prometheus Export Server to terminate");
            prometheusExportServer.stop();
        }
    }

    /**
     * closeStreamWithTimeout().
     */
    //WI-365808 For unit test cases
    public void closeStreamWithTimeout() {
        if (provider != null) {
            LOGGER.info("Asked to terminate with timeout");
            provider.terminateStreamsWithTimeout();
        }
        if (null != prometheusExportServer) {
            LOGGER.info("Asked Prometheus Export Server to terminate");
            prometheusExportServer.stop();
        }
    }

    /**
     * Checks if is execute shutdown hook.
     *
     * @return true, if is execute shutdown hook
     */
    public boolean isExecuteShutdownHook() {
        return executeShutdownHook;
    }

    /**
     * Sets the execute shutdown hook.
     *
     * @param executeShutdownHook the new execute shutdown hook
     */
    public void setExecuteShutdownHook(boolean executeShutdownHook) {
        this.executeShutdownHook = executeShutdownHook;
    }
    
    /**
     * Getter for dynamicProps.
     *
     * @return the dynamicProps
     */
    public static Properties getDynamicProps() {
        return dynamicProps;
    }

    /**
     * Setter for dynamicProps.
     *
     *@param dynamicProps the dynamicProps to set
     */
    public static void setDynamicProps(Properties dynamicProps) {
        Launcher.dynamicProps = dynamicProps;
    }
}
