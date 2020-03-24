package com.booking.replication.applier.validation;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.console.ConsoleApplier;
import com.booking.replication.applier.count.CountApplier;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import com.booking.validator.data.source.DataSource;
import com.booking.validator.data.source.Types;
import com.booking.validator.data.source.constant.ConstantQueryOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.net.Socket;
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
        waitForKafkaPorts();
        ValidationServiceTest.servicesControl = ServicesProvider.build(ServicesProvider.Type.CONTAINERS).startKafka(ValidationServiceTest.TOPIC_NAME, 1, 1);
    }
    private static boolean isPortInUse(String host, int port) {
        // Assume no connection is possible.
        boolean result = false;
        try {
            (new Socket(host, port)).close();
            result = true;
        }
        catch(IOException e) {
            // Could not connect.
        }
        return result;
    }
    public static void waitForKafkaPorts() {
        boolean scanning=true;
        while(scanning)
        {
            if(!isPortInUse("localhost", 9092)) {
                scanning=false;
            } else {
                System.out.println("Kafka port already in use. Waiting for 2s.");
                try {
                    Thread.sleep(2000);//2 seconds
                } catch(InterruptedException ie){
                    ie.printStackTrace();
                }
            }
        }
    }

    public static void initConfig() {
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.put(ValidationService.Configuration.VALIDATION_BROKER, "localhost:9092");
        configuration.put(ValidationService.Configuration.VALIDATION_THROTTLE_ONE_EVERY, "2");
        configuration.put(ValidationService.Configuration.VALIDATION_TOPIC, "replicator_validation");
        configuration.put(ValidationService.Configuration.VALIDATION_TAG, "test_hbase");
        configuration.put(ValidationService.Configuration.VALIDATION_SOURCE_DATA_SOURCE, "av1msql");
        configuration.put(ValidationService.Configuration.VALIDATION_TARGET_DATA_SOURCE, "avbigtable");
    }

    @Test
    public void testValidationServiceCounter() {
        LOG.info("ValidationServiceTest.testValidationServiceCounter() called");
        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        ValidationService validationService = ValidationService.getInstance(configuration);
        Assert.assertNotNull(validationService);
        DataSource dummy = new DataSource("constant", new ConstantQueryOptions(Types.CONSTANT.getValue(), new HashMap<String, Object>(){{put("a", 1);}}, null));
        for (int i=0; i<10; i++){
            validationService.registerValidationTask("sample-id-"+ i, dummy, dummy);
        }
        Assert.assertEquals(10L, validationService.getValidationTaskCounter());
    }

    @Test
    public void testValidationServiceNull() throws IllegalArgumentException {
        LOG.info("ValidationServiceTest.testValidationServiceNull() called");
        initConfig();
        configuration.put(Applier.Configuration.TYPE, "CONSOLE");
        ValidationService validationService = ValidationService.getInstance(configuration);
        Assert.assertNull(validationService);

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_TOPIC);
        try {
            validationService = ValidationService.getInstance(configuration);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_BROKER);
        try {
            validationService = ValidationService.getInstance(configuration);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_SOURCE_DATA_SOURCE);
        try {
            validationService = ValidationService.getInstance(configuration);
        } catch(IllegalArgumentException e) {
            Assert.assertNotNull(e);
        } finally {
            Assert.assertNull(validationService);
        }

        initConfig();
        configuration.put(Applier.Configuration.TYPE, "HBASE");
        configuration.remove(ValidationService.Configuration.VALIDATION_TARGET_DATA_SOURCE);
        try {
            validationService = ValidationService.getInstance(configuration);
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
