package org.eclipse.ecsp.analytics.stream.base.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.utils.logger.IgniteLogger;
import org.eclipse.ecsp.utils.logger.IgniteLoggerFactory;

import java.util.Properties;


/** 
 * A utility class to apply the SSL or SASL config depending on which one is enabled, 
 * on Kafka Producer or Kafka Consumer.
 *
 * @author karora
 *
 */
public final class KafkaSslUtils {

    /**
     * Instantiates a new kafka ssl utils.
     */
    private KafkaSslUtils() {
        // This is an utility class and should not be initialized
    }

    /** The logger. */
    private static IgniteLogger logger = IgniteLoggerFactory.getLogger(KafkaSslUtils.class);

    /**
     * Checks and applies necessary configurations related to SASL or SSL.
     *
     * @param props The properties instance.
     */
    public static void checkAndApplySslProperties(Properties props) {
        if (isSslEnabled(props) || isOneWayTlsEnabled(props)) {
            applySslProperties(props);
        }
    }

    /**
     * Utility method to set the required properties for enabling SASL_SSL or SSL.
     *
     * @param props The properties instance.
     * @return The properties instance with desired configs applied.
     */
    public static Properties applySslProperties(Properties props) {
        String sslClientAuth = SslClientAuth.NONE.toString();
        if (isSslEnabled(props)) {
            String keystore = props.getProperty(PropertyNames.KAFKA_CLIENT_KEYSTORE);
            ObjectUtils.requireNonEmpty(keystore, "Kafka client key store must be provided as SSL is enabled");
            String keystorePwd = props.getProperty(PropertyNames.KAFKA_CLIENT_KEYSTORE_PASSWORD);
            ObjectUtils.requireNonEmpty(keystorePwd, "Kafka client key store password must be "
                + "provided as SSL is enabled");
            String keyPwd = props.getProperty(PropertyNames.KAFKA_CLIENT_KEY_PASSWORD);
            ObjectUtils.requireNonEmpty(keyPwd, "Kafka client key password must be provided as SSL is enabled");
            sslClientAuth = props.getProperty(PropertyNames.KAFKA_SSL_CLIENT_AUTH, SslClientAuth.REQUIRED.toString());
            props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
            props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
            props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePwd);
            props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPwd);
            logger.debug("Properties set for SSL");
        } else if (isOneWayTlsEnabled(props)) {
            String saslMechanism = props.getProperty(PropertyNames.KAFKA_SASL_MECHANISM);
            ObjectUtils.requireNonEmpty(saslMechanism, "Kafka SASL Mechanism must be provided "
                + "as Kafka SASL is enabled.");
            String saslJaasConfig = props.getProperty(PropertyNames.KAFKA_SASL_JAAS_CONFIG);
            ObjectUtils.requireNonEmpty(saslJaasConfig, "Kafka SASL JAAS configuration must be "
                + "provided as Kafka SASL is enabled.");
            props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
            props.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
            props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
            logger.debug("Properties set for SASL");
        }

        String truststore = props.getProperty(PropertyNames.KAFKA_CLIENT_TRUSTSTORE);
        ObjectUtils.requireNonEmpty(truststore, "Kafka client trust store must be provided");
        String truststorePwd = props.getProperty(PropertyNames.KAFKA_CLIENT_TRUSTSTORE_PASSWORD);
        ObjectUtils.requireNonEmpty(truststorePwd, "Kafka client trust store password must be provided");
        String sslEndpointAlgo = props.getProperty(PropertyNames.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");
        props.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointAlgo);
        props.setProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, sslClientAuth);
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePwd);
        
        return props;
    }

    /**
     * Utility method to set the required properties for enabling SASL_SSL or SSL.
     *
     * @param targetProps The target properties instance to set the configs into.
     * @param sourceProps The source properties instance.
     * @return The properties instance with desired configs set. 
     */
    public static Properties applySslProperties(Properties targetProps, Properties sourceProps) {

        String sslClientAuth = SslClientAuth.NONE.toString();
        if (isSslEnabled(sourceProps)) {
            String keystore = sourceProps.getProperty(PropertyNames.KAFKA_CLIENT_KEYSTORE);
            ObjectUtils.requireNonEmpty(keystore, "Kafka client key store must be provided");
            String keystorePwd = sourceProps.getProperty(PropertyNames.KAFKA_CLIENT_KEYSTORE_PASSWORD);
            ObjectUtils.requireNonEmpty(keystorePwd, "Kafka client key store password must be provided");
            String keyPwd = sourceProps.getProperty(PropertyNames.KAFKA_CLIENT_KEY_PASSWORD);
            ObjectUtils.requireNonEmpty(keyPwd, "Kafka client key password must be provided");
            sslClientAuth = sourceProps.getProperty(PropertyNames.KAFKA_SSL_CLIENT_AUTH, 
                SslClientAuth.REQUIRED.toString());
            targetProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
            targetProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
            targetProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePwd);
            targetProps.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPwd);
            logger.debug("Properties set for SSL");
        } else if (isOneWayTlsEnabled(sourceProps)) {
            String saslMechanism = sourceProps.getProperty(PropertyNames.KAFKA_SASL_MECHANISM);
            ObjectUtils.requireNonEmpty(saslMechanism, "Kafka SASL Mechanism must be provided "
                + "because Kafka SASL is enabled.");
            String saslJaasConfig = sourceProps.getProperty(PropertyNames.KAFKA_SASL_JAAS_CONFIG);
            ObjectUtils.requireNonEmpty(saslJaasConfig, "Kafka SASL JAAS configuration must "
                 + "be provided because Kafka SASL is enabled.");
            targetProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
            targetProps.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
            targetProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
            logger.debug("Properties set for SASL");
        }
        String truststore = sourceProps.getProperty(PropertyNames.KAFKA_CLIENT_TRUSTSTORE);
        ObjectUtils.requireNonEmpty(truststore, "Kafka client trust store must be provided");
        String truststorePwd = sourceProps.getProperty(PropertyNames.KAFKA_CLIENT_TRUSTSTORE_PASSWORD);
        ObjectUtils.requireNonEmpty(truststorePwd, "Kafka client trust store password must be provided");
        String sslEndpointAlgo = sourceProps.getProperty(PropertyNames.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");
        
        targetProps.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointAlgo);
        targetProps.setProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, sslClientAuth);
        targetProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
        targetProps.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePwd);
        
        return targetProps;
    }

    /**
     * Returns whether SSL is enabled or not.
     *
     * @param props The properties instance.
     * @return boolean
     */
    private static boolean isSslEnabled(Properties props) {
        Object isSslEnabledObj = props.get(PropertyNames.KAFKA_SSL_ENABLE);
        return (isSslEnabledObj instanceof Boolean isSslEnabled 
            &&  Boolean.TRUE.equals(isSslEnabled)) || (isSslEnabledObj 
                instanceof String sslEnabledStr && sslEnabledStr.equalsIgnoreCase(Constants.TRUE));
    }

    /**
     * Returns whether one way TLS is enabled or not.
     *
     * @param props The properties instance.
     * @return boolean
     */
    private static boolean isOneWayTlsEnabled(Properties props) {
        Object isOneWayTlsEnabledObj = props.get(PropertyNames.KAFKA_ONE_WAY_TLS_ENABLE);
        return (isOneWayTlsEnabledObj instanceof Boolean isOneWayTlsEnabled 
            && Boolean.TRUE.equals(isOneWayTlsEnabled)) 
                || (isOneWayTlsEnabledObj instanceof String onewayTlsEnabledStr 
                    && onewayTlsEnabledStr.equalsIgnoreCase(Constants.TRUE));
    }

}