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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import org.eclipse.ecsp.analytics.stream.base.discovery.StreamProcessorDiscoveryService;
import org.eclipse.ecsp.analytics.stream.base.exception.InvalidMetricSpecifiedException;
import org.eclipse.ecsp.analytics.stream.base.exception.UnsupportedTimeUnitException;
import org.eclipse.ecsp.analytics.stream.base.metrics.reporter.CumulativeLogger;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Defines the common functionalities that may be required by various Launcher classes. 
 * For eg. As of now, we have {@link KafkaStreamsLauncher} but there could be a need of writing a 
 * wrapper over some other messaging broker. Hence, that launcher class can extend from this abstract class
 * and get the common functionality.
 *
 * @param <KIn> the generic type for incoming key
 * @param <VIn> the generic type for incoming value
 * @param <KOut> the generic type for outgoing key
 * @param <VOut>> the generic type for outgoing value
 */

public abstract class AbstractLauncher<KIn, VIn, KOut, VOut> implements LauncherProvider {
    
    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(AbstractLauncher.class);
    
    /** The {@link MetricRegistry} instance. */
    protected MetricRegistry metricRegistry;
    
    /** The list of {@link Closeable} reporters. */
    protected List<Closeable> reporters = new ArrayList<>(4);
    
    /**
     * these are the only list of metric reporter supported my Dropwizard metric.
     */
    protected static final String CONSOLE = "console";
    
    /** The Constant JMX. */
    protected static final String JMX = "jmx";
    
    /** The Constant CSV. */
    protected static final String CSV = "csv";
    
    /** The Constant SL4J. */
    protected static final String SL4J = "sl4j";

    /** The Constant SECONDS. */
    protected static final String SECONDS = "seconds";
    
    /** The Constant MINUTES. */
    protected static final String MINUTES = "minutes";
    
    /** The Constant HOURS. */
    protected static final String HOURS = "hours";

    /**
     * Instantiates a new abstract launcher.
     */
    protected AbstractLauncher() {
        metricRegistry = new MetricRegistry();
    }

    /**
     * Launches the derived Launcher class.
     *
     * @param props the props
     */
    @Override
    public final void launch(Properties props) {
        if (Boolean.parseBoolean(props.getProperty(PropertyNames.LOG_COUNTS, "false"))) {
            CumulativeLogger.init(props);
        }
        doLaunch(props);
    }

    /**
     * Method to launch the application with streaming.
     *
     * @param props the props
     */
    protected abstract void doLaunch(Properties props);

    /**
     * Method to close all the reporters.
     */
    public void terminate() {
        for (Closeable r : reporters) {
            try {
                r.close();
            } catch (IOException e) {
                logger.error("Exception when closing reporter", e);
            }
        }
    }


    /**
     * You can provide comma separated list of reporter names.
     *
     * @param config the config
     */
    protected void initializeMetricReporter(Properties config) {
        String reporterNames = config.getProperty("metric.reporters.name", "");
        String[] names = reporterNames.split(",");
        for (String reporter : names) {
            if (isSupportedMetricReporter(reporter)) {
                startReporter(reporter, config);
            } else {
                logger.info("Metric Reporter {}  not supported.", reporter);
            }
        }
    }

    /**
     * Load discovery service.
     *
     * @param props the props
     * @return the stream processor discovery service
     */
    protected StreamProcessorDiscoveryService<KIn, VIn, KOut, VOut> loadDiscoveryService(Properties props) {
        try {
            return (StreamProcessorDiscoveryService) getClass().getClassLoader()
                    .loadClass(props.getProperty(PropertyNames.DISCOVERY_SERVICE_IMPL))
                    .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    PropertyNames.DISCOVERY_SERVICE_IMPL
                            + " refers to a class that is not available on the classpath", e);
        }
    }

    /**
     * Checks if is supported metric reporter.
     *
     * @param reporterName the reporter name
     * @return true, if is supported metric reporter
     */
    private boolean isSupportedMetricReporter(String reporterName) {
        return (reporterName.equals(CONSOLE) || reporterName.equals(CSV)
                || reporterName.equals(JMX) || reporterName.equals(SL4J)) ? true
                : false;
    }

    /**
     * Method to start the reporter.
     * Only those reporter are supported which are supported by dropwizard metrics. console, csv, jmx, sl4j
     *
     * @param reporterName represents reporter's name as String
     *
     * @param config represents config properties as Properties body
     */
    private void startReporter(String reporterName, Properties config) {
        long interval = getLoggingInterval(config);
        TimeUnit unit = getTimeunit(config);
        logger.info("Metric will be generated every {} {}", interval, unit);
        if (reporterName.equals(CONSOLE)) {
            ConsoleReporter consoleReporter =
                    ConsoleReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            consoleReporter.start(interval, unit);
            reporters.add(consoleReporter);
            logger.info("Console reporter successfully registered to metric registry");
        } else if (reporterName.equals(CSV)) {
            String dirName = config.getProperty("csv.reporter.dir.name", "/tmp");
            CsvReporter csvReporter = CsvReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build(new File(dirName));
            csvReporter.start(interval, unit);
            reporters.add(csvReporter);
            logger.info("CSV reporter successfully registered to metric registry");
        } else if (reporterName.equals(JMX)) {
            JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();
            jmxReporter.start();
            reporters.add(jmxReporter);
            logger.info("JMX reporter successfully registered to metric registry");
        } else if (reporterName.equals(SL4J)) {
            Slf4jReporter sl4jReporter = Slf4jReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build();
            sl4jReporter.start(interval, unit);
            reporters.add(sl4jReporter);
            logger.info("SL4J reporter successfully registered to metric registry");
        } else {
            logger.error("Metric reporter {} not supported.", reporterName);
        }
    }

    /**
     * Gets the value of the property: metric.logging.interval.
     *
     * @param config the Properties instance.
     * @return the logging interval
     */
    private long getLoggingInterval(Properties config) {
        // if property is not defined, set the interval as 2
        long val = Long.parseLong(config.getProperty("metric.logging.interval", "60"));
        if (val < 1) {
            logger.error("{} is invalid logging interval. Value should be greater than 1. ");
            throw new InvalidMetricSpecifiedException("Invalid Metric logging value is specified.");
        }
        return val;
    }

    /**
     * Gets the timeunit.
     *
     * @param config the config
     * @return the timeunit
     */
    private TimeUnit getTimeunit(Properties config) {
        // default logging unit is minutes
        String timeunit = config.getProperty("metric.logging.unit", MINUTES);
        if (timeunit.equals(MINUTES)) {
            return TimeUnit.MINUTES;
        } else if (timeunit.equals(SECONDS)) {
            return TimeUnit.SECONDS;
        } else if (timeunit.equals(HOURS)) {
            return TimeUnit.HOURS;
        }
        logger.error("{} is unsupported time unit. Returning minutes.", timeunit);
        throw new UnsupportedTimeUnitException("Unspported time unit specified");
    }
}
