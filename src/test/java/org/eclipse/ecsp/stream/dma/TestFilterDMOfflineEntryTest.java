package org.eclipse.ecsp.stream.dma;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.Constants;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.domain.EventID;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntry;
import org.eclipse.ecsp.stream.dma.dao.DMOfflineBufferEntryDAOMongoImpl;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.handler.DeviceConnectionStatusHandler;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * The Class TestFilterDMOfflineEntryTest.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/filter-dma-offline-test.properties")
public class TestFilterDMOfflineEntryTest extends KafkaStreamsApplicationTestBase {
    
    /** The service name. */
    @Value("${service.name}")
    private String serviceName;
    
    /** The source topic. */
    @Value("${source.topic.name}")
    private String sourceTopic;
    
    /** The mqtt prefix. */
    @Value("${mqtt.service.topic.name.prefix}")
    private String mqttPrefix;
    
    /** The to device. */
    @Value("${" + PropertyNames.MQTT_TOPIC_TO_DEVICE_INFIX + ":" + Constants.TO_DEVICE + "}")
    private String toDevice;
    
    /** The mqtt topic. */
    @Value("${mqtt.service.topic.name}")
    private String mqttTopic;
    
    /** The device service. */
    @Autowired
    private DeviceStatusService deviceService;

    /** The offline buffer DAO. */
    @Autowired
    private DMOfflineBufferEntryDAOMongoImpl offlineBufferDAO;
    
    /** The device status back door kafka consumer. */
    @Autowired
    DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;
    
    /** The device connection status handler. */
    @Autowired
    DeviceConnectionStatusHandler deviceConnectionStatusHandler;

    /** The device status topic name. */
    private String deviceStatusTopicName;

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     * @throws MqttException the mqtt exception
     */
    @Before
    public void setUp() throws Exception, MqttException {
        super.setup();
        deviceStatusTopicName = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName.toLowerCase();
        createTopics(sourceTopic, deviceStatusTopicName);
        
        Properties kafkaConsumerProps = deviceStatusBackDoorKafkaConsumer.getKafkaConsumerProps();
        kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        deviceStatusBackDoorKafkaConsumer.addCallback(deviceConnectionStatusHandler.new DeviceStatusCallBack(), 0);
        deviceStatusBackDoorKafkaConsumer.startBackDoorKafkaConsumer();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        
        launchApplication();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        subscibeToMqttTopic(mqttPrefix + "12345" + toDevice + "/" + mqttTopic);
    }

    /** The vehicle id. */
    private String vehicleId = "Vehicle12345";

    /**
     * Test filter offline buffer.
     *
     * @throws ExecutionException the execution exception
     * @throws InterruptedException the interrupted exception
     * @throws TimeoutException the timeout exception
     */
    @Test
    public void testFilterOfflineBuffer() throws ExecutionException, InterruptedException, TimeoutException {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        final String speedEvent = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":20.0},"
                + "\"MessageId\": \"1234\",\"CorrelationId\": \"1234\",\"BizTransactionId\": \"Biz1234\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        final String speedEvent1 = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":30.0},"
                + "\"MessageId\": \"1235\",\"CorrelationId\": \"1237\",\"BizTransactionId\": \"Biz1235\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        final String speedEvent2 = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":40.0},"
                + "\"MessageId\": \"1236\",\"CorrelationId\": \"1238\",\"BizTransactionId\": \"Biz1236\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        final String speedEvent3 = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":50.0},"
                + "\"MessageId\": \"1237\",\"CorrelationId\": \"1239\",\"BizTransactionId\": \"Biz1237\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        final String speedEvent4 = "{\"EventID\": \"Speed\",\"Version\": \"1.0\",\"Data\": {\"value\":60.0},"
                + "\"MessageId\": \"1238\",\"CorrelationId\": \"2234\",\"BizTransactionId\": \"Biz1238\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";

        String deviceInactive = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"INACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(deviceStatusTopicName, producerProps,
                Arrays.asList(vehicleId.getBytes(), deviceInactive.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        assertNull(deviceService.get(vehicleId, Optional.empty()));

        sendMessages(sourceTopic, producerProps,
                Arrays.asList(vehicleId.getBytes(), speedEvent.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
        sendMessages(sourceTopic, producerProps,
                Arrays.asList(vehicleId.getBytes(), speedEvent1.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_3000);
        sendMessages(sourceTopic, producerProps,
                Arrays.asList(vehicleId.getBytes(), speedEvent2.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_3000);
        sendMessages(sourceTopic, producerProps,
                Arrays.asList(vehicleId.getBytes(), speedEvent3.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_3000);
        sendMessages(sourceTopic, producerProps,
                Arrays.asList(vehicleId.getBytes(), speedEvent4.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_3000);
        List<DMOfflineBufferEntry> bufferEntries = offlineBufferDAO
              .getOfflineBufferEntriesSortedByPriority(vehicleId, false, Optional.empty(), Optional.empty());
        assertEquals("Expected 5 entry", TestConstants.FIVE, bufferEntries.size());
        String deviceActive = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"12345\"}";
        sendMessages(deviceStatusTopicName, producerProps,
                Arrays.asList(vehicleId.getBytes(), deviceActive.getBytes()));
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);

        String completeMqttTopic = mqttPrefix + "12345" + toDevice + "/" + mqttTopic;
        List<byte[]> messages = getMessagesFromMqttTopic(completeMqttTopic, 1, TestConstants.INT_60000);
        assertEquals("No of message expected", TestConstants.FOUR, messages.size());
    }
    
    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        deviceStatusBackDoorKafkaConsumer.shutdown();
    }

    /**
     * The Class DMOfflineBufferTestStreamProcessor.
     */
    public static class DMOfflineBufferTestStreamProcessor implements IgniteEventStreamProcessor {
        
        /** The spc. */
        private StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc;

        /**
         * Inits the.
         *
         * @param spc the spc
         */
        @Override
        public void init(StreamProcessingContext<IgniteKey<?>, IgniteEvent> spc) {
            this.spc = spc;

        }

        /**
         * Name.
         *
         * @return the string
         */
        @Override
        public String name() {
            return "dma-sp";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            IgniteEvent value = kafkaRecord.value();
            if (!value.getEventId().equals(EventID.DEVICEMESSAGEFAILURE)) {
                ((AbstractIgniteEvent) value).setDeviceRoutable(true);
                spc.forward(kafkaRecord.withValue(value));
            }
        }

        /**
         * Punctuate.
         *
         * @param timestamp the timestamp
         */
        @Override
        public void punctuate(long timestamp) {
            // todo Auto-generated method stub
        }

        /**
         * Close.
         */
        @Override
        public void close() {
            // todo Auto-generated method stub

        }

        /**
         * Config changed.
         *
         * @param props the props
         */
        @Override
        public void configChanged(Properties props) {
            // todo Auto-generated method stub

        }

        /**
         * Creates the state store.
         *
         * @return the harman persistent KV store
         */
        @Override
        public HarmanPersistentKVStore createStateStore() {
            // todo Auto-generated method stub
            return null;
        }

    }
}