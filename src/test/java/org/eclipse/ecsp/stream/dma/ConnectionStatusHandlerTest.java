package org.eclipse.ecsp.stream.dma;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.ecsp.analytics.stream.base.IgniteEventStreamProcessor;
import org.eclipse.ecsp.analytics.stream.base.Launcher;
import org.eclipse.ecsp.analytics.stream.base.PropertyNames;
import org.eclipse.ecsp.analytics.stream.base.StreamProcessingContext;
import org.eclipse.ecsp.analytics.stream.base.constants.TestConstants;
import org.eclipse.ecsp.analytics.stream.base.stores.HarmanPersistentKVStore;
import org.eclipse.ecsp.analytics.stream.base.utils.DefaultMqttTopicNameGeneratorImpl;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaStreamsApplicationTestBase;
import org.eclipse.ecsp.analytics.stream.base.utils.KafkaTestUtils;
import org.eclipse.ecsp.analytics.stream.base.utils.PahoMqttDispatcher;
import org.eclipse.ecsp.entities.AbstractIgniteEvent;
import org.eclipse.ecsp.entities.IgniteEvent;
import org.eclipse.ecsp.entities.IgniteEventImpl;
import org.eclipse.ecsp.entities.dma.DeviceMessageHeader;
import org.eclipse.ecsp.key.IgniteKey;
import org.eclipse.ecsp.stream.dma.dao.DMAConstants;
import org.eclipse.ecsp.stream.dma.dao.DeviceStatusService;
import org.eclipse.ecsp.stream.dma.handler.DeviceConnectionStatusHandler;
import org.eclipse.ecsp.stream.dma.handler.DeviceStatusBackDoorKafkaConsumer;
import org.eclipse.ecsp.utils.ConcurrentHashSet;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;



/**
 * Integration test case for {@link DeviceConnectionStatusHandler} in Device Messaging module.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { Launcher.class })
@EnableRuleMigrationSupport
@TestPropertySource("/dma-connectionstatus-handler-test.properties")
public class ConnectionStatusHandlerTest extends KafkaStreamsApplicationTestBase {
    
    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionStatusHandlerTest.class);
    
    /** The conn status topic. */
    private static String connStatusTopic;
    
    /** The source topic name. */
    private static String sourceTopicName;
    
    /** The i. */
    private static int i = 0;
    
    /** The vehicle id. */
    private String vehicleId = "Vehicle12345";
    
    /** The device id. */
    private String deviceId = "Device12345";

    /** The test client. */
    private MqttClientTest testClient;

    /** The default mqtt topic name generator impl. */
    @Autowired
    private DefaultMqttTopicNameGeneratorImpl defaultMqttTopicNameGeneratorImpl;
    
    /** The device service. */
    @Autowired
    private DeviceStatusService deviceService;

    /** The paho mqtt dispatcher. */
    @Autowired
    private PahoMqttDispatcher pahoMqttDispatcher;
    
    /** The device status back door kafka consumer. */
    @Autowired
    DeviceStatusBackDoorKafkaConsumer deviceStatusBackDoorKafkaConsumer;
    
    /** The device connection status handler. */
    @Autowired
    DeviceConnectionStatusHandler deviceConnectionStatusHandler;

    /** The service name. */
    @Value("${" + PropertyNames.SERVICE_NAME + ":}")
    private String serviceName;

    /**
     * Gets the service name.
     *
     * @return the service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Sets the service name.
     *
     * @param serviceName the new service name
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /** The interval. */
    private long interval = 500L;
    
    /**
     * Sets up the environment for this test class before each test case run. 
     * Like creation of necessary kafka topics.
     *
     * @throws Exception the exception
     */
    @Before
    public void setup() throws Exception {
        super.setup();
        i++;
        sourceTopicName = "sourceTopic" + i;
        connStatusTopic = DMAConstants.DEVICE_STATUS_TOPIC_PREFIX + serviceName.toLowerCase();
        createTopics(connStatusTopic, sourceTopicName, "dff-dfn-updates");
        ksProps.put(PropertyNames.SOURCE_TOPIC_NAME, sourceTopicName);

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
             Serdes.ByteArray().serializer().getClass().getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
             Serdes.ByteArray().serializer().getClass().getName());
        
        Properties kafkaConsumerProps = deviceStatusBackDoorKafkaConsumer.getKafkaConsumerProps();
        kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers());
        deviceStatusBackDoorKafkaConsumer.addCallback(deviceConnectionStatusHandler.new DeviceStatusCallBack(), 0);
        deviceStatusBackDoorKafkaConsumer.startBackDoorKafkaConsumer();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);
    }

    /**
     * Test DMA based on device status.
     *
     * @throws Exception the exception
     */
    @Test
    public void testDMABasedOnDeviceStatus() throws Exception {
        ConcurrentHashSet<String> expectedValue = new ConcurrentHashSet<>();
        expectedValue.add(deviceId);
        ksProps.put(PropertyNames.SERVICE_STREAM_PROCESSORS, DMATestServiceProcessor.class.getName());
        ksProps.put(PropertyNames.APPLICATION_ID, "test-sp" + System.currentTimeMillis());
        launchApplication();
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_10000);
        
        String deviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
            + "\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";

        String deviceIdKey = vehicleId;
        Assert.assertNull(deviceService.get(deviceIdKey, Optional.empty()));
        
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(), 
             deviceConnStatusEvent.getBytes());

        Function<Void, ConcurrentHashSet<String>> getStatus = x -> deviceService.get(deviceIdKey, Optional.empty());

        ConcurrentHashSet<String> actual = retryWithException(TestConstants.TWENTY, getStatus);
        /*
         * Ensure incorrect status does not exist.
         */
        Assert.assertNull(deviceService.get(deviceId, Optional.empty()));
        /*
         * Once a status ACTIVE has been sent it should reflect in LocalCache.
         */
        Assert.assertEquals(expectedValue, actual);
        
        String speedEventWithVehicleId = "{\"EventID\": \"Speed\",\"Version\": \"1.0\","
             + "\"Data\": {\"value\":20.0},\"MessageId\": \"1234\",\"CorrelationId\": \"1234\","
                 + "\"BizTransactionId\": \"Biz1234\",\"VehicleId\": \"Vehicle12345\"}";

        /*
         * Send Data to source Topic.
         */
        testClient = new MqttClientTest(new TestKey(), new DeviceMessageHeader().withTargetDeviceId("Device12345"));
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(), 
               speedEventWithVehicleId.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_5000);       
        /*
         * Data send in source topic should come to mqtt
         */
        String msgRec = testClient.getMsgReceived();
        Assert.assertTrue(msgRec.contains("\"EventID\":\"Speed\""));
        Assert.assertTrue(msgRec.contains("\"Version\":\"1.0\""));
        Assert.assertTrue(msgRec.contains("\"Data\":{\"value\":20.0}"));
        Assert.assertTrue(msgRec.contains("\"BizTransactionId\":\"Biz1234\""));
        /*
         * Disconnect Device after sending connStatus=INACTIVE.
         */
        String terminateDeviceConnStatusEvent = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
              + "\"Data\": {\"connStatus\":\"INACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                   + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(), 
             terminateDeviceConnStatusEvent.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_2000);
        Assert.assertNull(deviceService.get(deviceIdKey, Optional.empty()));
        /*
         * Data send in source topic should NOT come to mqtt as current status
         * of device is INACTIVE.
         */
        testClient = new MqttClientTest(new TestKey(), new DeviceMessageHeader().withTargetDeviceId("Device12345"));
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(), 
            speedEventWithVehicleId.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_2000);
        Assert.assertNull(testClient.getMsgReceived());
        /*
         * Test if sourceDeviceId notPresent in DeviceConnStatus will the status
         * be set in cache.
         */
        String deviceConnStatusEventWithoutSourceDeviceId = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
              + "\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},"
                   + "\"MessageId\": \"1234\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(),
                deviceConnStatusEventWithoutSourceDeviceId.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_2000);
        Assert.assertNull(deviceService.get(deviceIdKey, Optional.empty()));
        /*
         * Test if eventId for DeviceConnStatus event is incorrect will the
         * status be set in cache
         */
        String deviceConnStatusEventWithIncorrectEventId = "{\"EventID\": \"DeviceConn\",\"Version\": \"1.0\","
               + "\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},"
                 + "\"MessageId\": \"1234\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(),

                deviceConnStatusEventWithIncorrectEventId.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_2000);
        Assert.assertNull(deviceService.get(deviceIdKey, Optional.empty()));
        /*
         * Test if vehicleId notPresent in Speed event being pushed to
         * sourceTopic of SP if data will be pushed to MQTT.
         */
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(),
                deviceConnStatusEvent.getBytes());
        actual = new ConcurrentHashSet<String>();
        actual = retryWithException(TestConstants.TWENTY, getStatus);

        Assert.assertEquals(expectedValue, actual);
        testClient = new MqttClientTest(new TestKey(), new DeviceMessageHeader().withTargetDeviceId("Device12345"));

        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_2000);

        if (testClient.getMsgReceived() != null) {
            testClient = new MqttClientTest(new TestKey(), new DeviceMessageHeader().withTargetDeviceId("Device12345"));
        }

        String speedEventWithoutVehicleId = "{\"EventID\": \"Speed\",\"Version\": \"1.0\","
             + "\"Data\": {\"value\":20.0},\"MessageId\": \"1234\",\"CorrelationId\": \"1234\","
                 + "\"BizTransactionId\": \"Biz1234\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(), 
             speedEventWithoutVehicleId.getBytes());

        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_2000);
        Assert.assertNull(testClient.getMsgReceived());

        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(), 
             terminateDeviceConnStatusEvent.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_3000);
        /*
         * When correct sourceDeviceId is set will data be forwarded to MQTT
         */
        testClient = new MqttClientTest(new TestKey(), new DeviceMessageHeader().withTargetDeviceId("Device12345"));
        String speedEventWithVehicleIdAndSourceDeviceId = "{\"EventID\": \"Speed\",\"Version\": \"1.0\","
                + "\"Data\": {\"value\":20.0},\"MessageId\": \"1237\","
                 + "\"BizTransactionId\": \"Biz1237\",\"VehicleId\": \"Vehicle12345\","
                   + "\"SourceDeviceId\": \"Device12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(),
                deviceConnStatusEvent.getBytes());
        Thread.sleep(TestConstants.THREAD_SLEEP_TIME_3000);
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(),
                speedEventWithVehicleIdAndSourceDeviceId.getBytes());
        String actualMsgRec = retryWithException(TestConstants.THIRTY, x -> testClient.getMsgReceived());
        actualMsgRec = retryWithException(TestConstants.TWENTY, x -> testClient.getMsgReceived());

        msgRec = testClient.getMsgReceived();
        Assert.assertTrue(msgRec.contains("\"EventID\":\"Speed\""));
        Assert.assertTrue(msgRec.contains("\"Version\":\"1.0\""));
        Assert.assertTrue(msgRec.contains("\"Data\":{\"value\":20.0}"));
        Assert.assertTrue(msgRec.contains("\"BizTransactionId\":\"Biz1237\""));
        Assert.assertTrue(msgRec.contains("\"SourceDeviceId\":\"Device12345\""));

        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(), 
              terminateDeviceConnStatusEvent.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_3000, TimeUnit.MILLISECONDS);

        /*
         * When incorrect sourceDeviceId is set will data be forwarded to MQTT
         */
        testClient = new MqttClientTest(new TestKey(), 
                new DeviceMessageHeader().withTargetDeviceId("IncorrectDevice12345"));
        String speedEventWithVehicleIdAndIncorrectSourceDeviceId = "{\"EventID\": \"Speed\",\"Version\": \"1.0\","
                 + "\"Data\": {\"value\":20.0},\"MessageId\": \"1234\",\"CorrelationId\": \"1234\","
                  + "\"BizTransactionId\": \"Biz1234\",\"VehicleId\": \"Vehicle12345\","
                  + "\"SourceDeviceId\": \"IncorrectDevice12345\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(),
                deviceConnStatusEvent.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_3000, TimeUnit.MILLISECONDS);
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(),
                speedEventWithVehicleIdAndIncorrectSourceDeviceId.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_2000, TimeUnit.MILLISECONDS);
        Assert.assertNull(testClient.getMsgReceived());
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(), 
               terminateDeviceConnStatusEvent.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_2000, TimeUnit.MILLISECONDS);

        /*
         * When multiple deviceIds are present for a vehcileId, will data be
         * forwarded to MQTT if sourceDeviceIdis not present
         */
        testClient = new MqttClientTest(new TestKey(), new DeviceMessageHeader().withTargetDeviceId("Device12345"));
        String deviceConnStatusEventDevice12346 = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                  + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12346\"}";
        String deviceConnStatusEventDevice12347 = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"ACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                  + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12347\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(), 
                deviceConnStatusEventDevice12346.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_2000, TimeUnit.MILLISECONDS);
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(), 
                deviceConnStatusEventDevice12347.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_2000, TimeUnit.MILLISECONDS);
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(), 
                speedEventWithVehicleId.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_3000, TimeUnit.MILLISECONDS);
        Assert.assertNull(testClient.getMsgReceived());

        /*
         * When multiple deviceIds are present for a vehcileId, will data be
         * forwarded to MQTT if sourceId is present
         */
        testClient = new MqttClientTest(new TestKey(), new DeviceMessageHeader().withTargetDeviceId("Device12347"));
        String speedEventWithVehicleIdAndSourceDeviceIdDevice12347 = "{\"EventID\": \"Speed\",\"Version\": \"1.0\","
                + "\"Data\": {\"value\":20.0},\"MessageId\": \"9001\",\"BizTransactionId\": \"Biz9001\","
                 + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12347\"}";
        KafkaTestUtils.sendMessages(sourceTopicName, producerProps, vehicleId.getBytes(),
                speedEventWithVehicleIdAndSourceDeviceIdDevice12347.getBytes());
        actualMsgRec = retryWithException(TestConstants.TWENTY, x -> testClient.getMsgReceived());
        msgRec = testClient.getMsgReceived();
        Assert.assertTrue(msgRec.contains("\"EventID\":\"Speed\""));
        Assert.assertTrue(msgRec.contains("\"Version\":\"1.0\""));
        Assert.assertTrue(msgRec.contains("\"Data\":{\"value\":20.0}"));
        Assert.assertTrue(msgRec.contains("\"BizTransactionId\":\"Biz9001\""));
        Assert.assertTrue(msgRec.contains("\"SourceDeviceId\":\"Device12347\""));

        String terminateDeviceConnStatusEventDevice12346 = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"INACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12346\"}";
        String terminateDeviceConnStatusEventDevice12347 = "{\"EventID\": \"DeviceConnStatus\",\"Version\": \"1.0\","
                + "\"Data\": {\"connStatus\":\"INACTIVE\",\"serviceName\":\"eCall\"},\"MessageId\": \"1234\","
                   + "\"VehicleId\": \"Vehicle12345\",\"SourceDeviceId\": \"Device12347\"}";
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(),
                terminateDeviceConnStatusEventDevice12346.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_2000, TimeUnit.MILLISECONDS);
        KafkaTestUtils.sendMessages(connStatusTopic, producerProps, vehicleId.getBytes(),
                terminateDeviceConnStatusEventDevice12347.getBytes());
        await().atMost(TestConstants.THREAD_SLEEP_TIME_2000, TimeUnit.MILLISECONDS);

    }
    
    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        deviceStatusBackDoorKafkaConsumer.shutdown();
    }

    /**
     * Test implementation of IgniteKey.
     */
    public class TestKey implements IgniteKey<String> {

        /**
         * Gets the key.
         *
         * @return the key
         */
        @Override
        public String getKey() {

            return vehicleId;
        }

    }
    
    /**
     * Test stream processor class for this integration test class.
     */
    public static final class DMATestServiceProcessor implements IgniteEventStreamProcessor {
        
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
            return "DMATestServiceProcessor";
        }

        /**
         * Process.
         *
         * @param kafkaRecord the kafka record
         */
        @Override
        public void process(Record<IgniteKey<?>, IgniteEvent> kafkaRecord) {
            AbstractIgniteEvent event = (AbstractIgniteEvent) kafkaRecord.value();
            event.setDeviceRoutable(true);
            spc.forward(kafkaRecord.withValue(event));
        }

        /**
         * Punctuate.
         *
         * @param timestamp the timestamp
         */
        @Override
        public void punctuate(long timestamp) {
            // default implementation 

        }

        /**
         * Close.
         */
        @Override
        public void close() {
            // default implementation

        }

        /**
         * Config changed.
         *
         * @param props the props
         */
        @Override
        public void configChanged(Properties props) {
            // default implementation

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

        /**
         * Sources.
         *
         * @return the string[]
         */
        @Override
        public String[] sources() {
            return new String[] { sourceTopicName };
        }

        /**
         * Sinks.
         *
         * @return the string[]
         */
        @Override
        public String[] sinks() {
            return new String[] {};
        }
    }

    /**
     * Test IgniteEvent class.
     */
    public class TestEvent extends IgniteEventImpl {

        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /** The device id. */
        private String deviceId;

        /**
         * Instantiates a new test event.
         *
         * @param deviceId the device id
         */
        public TestEvent(String deviceId) {
            this.deviceId = deviceId;
        }

        /**
         * Gets the event id.
         *
         * @return the event id
         */
        @Override
        public String getEventId() {
            return "Speed";
        }

        /**
         * Gets the target device id.
         *
         * @return the target device id
         */
        @Override
        public Optional<String> getTargetDeviceId() {
            return Optional.of(this.deviceId);
        }

    }

    /**
     * Test MQTT Client class.
     */
    public class MqttClientTest {

        /**
         * get a client to subscribe to the required topic topic.
         */

        private String mqttTopicToSubscribe;
        
        /** The msg received. */
        private String msgReceived;

        /**
         * Gets the msg received.
         *
         * @return the msg received
         */
        public String getMsgReceived() {
            return msgReceived;
        }

        /**
         * Creates the MQTT client.
         *
         * @param key IgniteKey
         * @param header DeviceMessageHeader
         */
        public MqttClientTest(IgniteKey key, DeviceMessageHeader header) {
            mqttTopicToSubscribe = defaultMqttTopicNameGeneratorImpl.getMqttTopicName(key, header, null).get();
            try {
                createClient();
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }

        /**
         * Creates the MQTT client and subscribes it to a topic for this test case.
         *
         * @throws MqttException MqttException
         */
        public void createClient() throws MqttException {
            MqttClient client = pahoMqttDispatcher.getMqttClient(PropertyNames.DEFAULT_PLATFORMID).get();
            client.subscribe(mqttTopicToSubscribe);
            client.setCallback(new MqttCallback() {

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    LOGGER.info("Msg received:{} on topic:{}", message, topic);
                    msgReceived = message.toString();

                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {

                }

                @Override
                public void connectionLost(Throwable cause) {

                }
            });
        }

    }

}