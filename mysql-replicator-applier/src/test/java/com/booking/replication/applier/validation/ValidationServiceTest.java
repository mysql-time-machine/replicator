package com.booking.replication.applier.validation;

import com.booking.replication.applier.Applier;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
        configuration.put(Applier.Configuration.TYPE, "hbase");
        configuration.put(ValidationService.Configuration.VALIDATION_BROKER, "localhost:9092");
        configuration.put(ValidationService.Configuration.VALIDATION_THROTTLE_ONE_EVERY, "1");
        configuration.put(ValidationService.Configuration.VALIDATION_TOPIC, "replicator_validation");
        configuration.put(ValidationService.Configuration.VALIDATION_TAG, "test_hbase");
        configuration.put(ValidationService.Configuration.VALIDATION_SOURCE_DOMAIN, "mysql-schema");
        configuration.put(ValidationService.Configuration.VALIDATION_TARGET_DOMAIN, "hbase-cluster");
    }

    @Test
    public void testValidationServiceCounter() {
        LOG.info("ValidationServiceTest.testValidationServiceCounter() called");
        initConfig();
        configuration.put(Applier.Configuration.TYPE, "hbase");
        ValidationService validationService = ValidationService.getInstance(configuration);
        Assert.assertNotNull(validationService);
        validationService.registerValidationTask("sample-id", "mysql://","hbase://");
        validationService.registerValidationTask("sample-id 2", "mysql://","hbase://");
        Assert.assertEquals(2L, validationService.getValidationTaskCounter());
    }

    @Test
    public void testValidationServiceNull() throws IllegalArgumentException {
        LOG.info("ValidationServiceTest.testValidationServiceNull() called");
        initConfig();
        configuration.put(Applier.Configuration.TYPE, "console");
        ValidationService validationService = ValidationService.getInstance(configuration);
        Assert.assertNull(validationService);

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "hbase");
        configuration.remove(ValidationService.Configuration.VALIDATION_TOPIC);
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
