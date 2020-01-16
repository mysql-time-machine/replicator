package com.booking.replication.applier.validation;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.console.ConsoleApplier;
import com.booking.replication.applier.count.CountApplier;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.commons.metrics.Metrics;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kafka.Kafka;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

public class ValidationServiceTest {
    private static final Logger LOG = LogManager.getLogger(ValidationServiceTest.class);
    private static ServicesControl servicesControl;
    private static String TOPIC_NAME = "replicator_validation";
    private static Map<String, Object> configuration;

    @BeforeClass
    public static void Before() {
        configuration = new HashMap<>();
        initConfig();
        ValidationServiceTest.servicesControl = ServicesProvider.build(ServicesProvider.Type.CONTAINERS).startKafka(ValidationServiceTest.TOPIC_NAME, 1, 1);
    }

    public static void initConfig() {
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.put(ValidationService.Configuration.VALIDATION_BROKER, "localhost:9092");
        configuration.put(ValidationService.Configuration.VALIDATION_THROTTLE_ONE_EVERY, "2");
        configuration.put(ValidationService.Configuration.VALIDATION_DATA_SOURCE_NAME, "shard1");
        configuration.put(ValidationService.Configuration.VALIDATION_TOPIC, "replicator_validation");
        configuration.put(ValidationService.Configuration.VALIDATION_TAG, "test_hbase");
        configuration.put(ValidationService.Configuration.VALIDATION_SOURCE_DOMAIN, "mysql-schema");
        configuration.put(ValidationService.Configuration.VALIDATION_TARGET_DOMAIN, "hbase-cluster");
    }

    @Test
    public void testValidationServiceCounter() {
        LOG.info("ValidationServiceTest.testValidationServiceCounter() called");
        Metrics<?> metrics = Metrics.build(configuration, new Server());

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        ValidationService validationService = ValidationService.getInstance(configuration, metrics);
        Assert.assertNotNull(validationService);
        for (int i=0; i<10; i++){
            validationService.registerValidationTask("sample-id-"+ i, "mysql://","hbase://");
        }
        Assert.assertEquals(10L, validationService.getValidationTaskCounter());
    }

    @Test
    public void testValidationServiceNull() throws IllegalArgumentException {
        LOG.info("ValidationServiceTest.testValidationServiceNull() called");
        Metrics<?> metrics = Metrics.build(configuration, new Server());

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "CONSOLE");
        ValidationService validationService = ValidationService.getInstance(configuration, metrics);
        Assert.assertNull(validationService);

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_TOPIC);
        try {
            validationService = ValidationService.getInstance(configuration, metrics);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_BROKER);
        try {
            validationService = ValidationService.getInstance(configuration, metrics);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_SOURCE_DOMAIN);
        try {
            validationService = ValidationService.getInstance(configuration, metrics);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_TARGET_DOMAIN);
        try {
            validationService = ValidationService.getInstance(configuration, metrics);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }
    }

    @Test
    public void testApplierValidations() {
        initConfig();
        configuration.put(Applier.Configuration.TYPE, "CONSOLE");
        ConsoleApplier consoleApplier = (ConsoleApplier) Applier.build(configuration);
        ValidationService validationServiceConsole = consoleApplier.buildValidationService(configuration);
        Assert.assertNull(validationServiceConsole);

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "COUNT");
        CountApplier countApplier = (CountApplier) Applier.build(configuration);
        ValidationService validationServiceCount = countApplier.buildValidationService(configuration);
        Assert.assertNull(validationServiceCount);
    }

    @AfterClass
    public static void After() {
        ValidationServiceTest.servicesControl.close();
    }
}
