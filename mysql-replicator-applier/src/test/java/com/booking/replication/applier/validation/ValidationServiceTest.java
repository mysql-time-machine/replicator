package com.booking.replication.applier.validation;

import com.booking.replication.applier.Applier;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

public class ValidationServiceTest {
    private static final Logger LOG = LogManager.getLogger(ValidationServiceTest.class);
    private static ServicesControl servicesControl;
    private static String TOPIC_NAME = "replicator_validation";
    private static Map<String, Object> configuration;

    @BeforeClass
    public static void initializeContainer () {
        ValidationServiceTest.servicesControl = ServicesProvider.build(ServicesProvider.Type.CONTAINERS).startKafka(ValidationServiceTest.TOPIC_NAME, 1, 1, true);
    }

    @Before
    public void cleanConfiguration() {
        configuration = new HashMap<>();
        initConfig();
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
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        ValidationService validationService = ValidationService.getInstance(configuration);
        Assert.assertNotNull(validationService);
        for (int i=0; i<10; i++){
            validationService.registerValidationTask("sample-id-"+ i, "mysql://","hbase://");
        }
        Assert.assertEquals(10L, validationService.getValidationTaskCounter());
    }

    @Test
    public void testInstantiationWithInvalidApplier() {
        LOG.info("ValidationServiceTest.testInstantiationWithInvalidApplier() called");
        configuration.put(Applier.Configuration.TYPE, "CONSOLE");
        ValidationService validationService = ValidationService.getInstance(configuration);

        Assert.assertNull(validationService);
    }

    @Test
    public void testInstantiationWithoutTopic() {
        ValidationService validationService = null;

        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_TOPIC);

        try {
            validationService = ValidationService.getInstance(configuration);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }
    }

    @Test
    public void testInstantiationWithoutBroker() {
        ValidationService validationService = null;

        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_BROKER);

        try {
            validationService = ValidationService.getInstance(configuration);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }
    }

    @Test
    public void testInstantiationWithoutSourceDomain() {
        ValidationService validationService = null;

        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_SOURCE_DOMAIN);
        try {
            validationService = ValidationService.getInstance(configuration);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }
    }

    @Test
    public void testInstantiationWithoutTargetDomain() throws IllegalArgumentException {
        ValidationService validationService = null;
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_TARGET_DOMAIN);
        try {
            validationService = ValidationService.getInstance(configuration);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }
    }

    @AfterClass
    public static void After() {
        ValidationServiceTest.servicesControl.close();
    }
}
