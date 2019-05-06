package com.booking.utils;


import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.applier.schema.registry.BCachedSchemaRegistryClient;
import com.booking.replication.augmenter.ActiveSchemaManager;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.commons.conf.MySQLConfiguration;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class BootstrapReplicatorTest {
    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String KAFKA_REPLICATOR_TOPIC_NAME = "mytopic";
    private static final String MYSQL_CONF_FILE = "my.cnf";
    private static final String MYSQL_INIT_SCRIPT = "mysql.init.sql";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final boolean BOOTSTRAP_ACTIVE = true;
    private static ServicesControl mysqlBinaryLog;
    private static ServicesControl mysqlActiveSchema;
    private static ServicesControl kafkaZk;
    private static ServicesControl kafka;
    private static ServicesControl schemaRegistry;
    private static BootstrapReplicator bootstrapReplicator;

    @BeforeClass
    public static void before() {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        MySQLConfiguration mySQLConfiguration = new MySQLConfiguration(
                BootstrapReplicatorTest.MYSQL_SCHEMA,
                BootstrapReplicatorTest.MYSQL_USERNAME,
                BootstrapReplicatorTest.MYSQL_PASSWORD,
                BootstrapReplicatorTest.MYSQL_CONF_FILE,
                Collections.singletonList(BootstrapReplicatorTest.MYSQL_INIT_SCRIPT),
                null,
                null
        );

        MySQLConfiguration mySQLActiveSchemaConfiguration = new MySQLConfiguration(
                BootstrapReplicatorTest.MYSQL_ACTIVE_SCHEMA,
                BootstrapReplicatorTest.MYSQL_USERNAME,
                BootstrapReplicatorTest.MYSQL_PASSWORD,
                BootstrapReplicatorTest.MYSQL_CONF_FILE,
                Collections.emptyList(),
                null,
                null
        );

        BootstrapReplicatorTest.mysqlBinaryLog = servicesProvider.startMySQL(mySQLConfiguration);
        BootstrapReplicatorTest.mysqlActiveSchema = servicesProvider.startMySQL(mySQLActiveSchemaConfiguration);
        Network network = Network.newNetwork();
        BootstrapReplicatorTest.kafkaZk = servicesProvider.startZookeeper(network, "kafkaZk");
        BootstrapReplicatorTest.kafka = servicesProvider.startKafka(network, BootstrapReplicatorTest.KAFKA_REPLICATOR_TOPIC_NAME, 3, 1, "kafka");
        BootstrapReplicatorTest.schemaRegistry = servicesProvider.startSchemaRegistry(network);

    }

    @Test
    public void testBootstrap() throws Exception {
        HashMap<String, Object> configuration = new HashMap<>();

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(BootstrapReplicatorTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(BootstrapReplicatorTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, BootstrapReplicatorTest.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, BootstrapReplicatorTest.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, BootstrapReplicatorTest.MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, BootstrapReplicatorTest.mysqlActiveSchema.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(BootstrapReplicatorTest.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, BootstrapReplicatorTest.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, BootstrapReplicatorTest.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, BootstrapReplicatorTest.MYSQL_PASSWORD);
        configuration.put(Augmenter.Configuration.BOOTSTRAP, BootstrapReplicatorTest.BOOTSTRAP_ACTIVE);

        String schemaRegistryUrl = String.format("http://%s:%d", BootstrapReplicatorTest.schemaRegistry.getHost(), BootstrapReplicatorTest.schemaRegistry.getPort());
        configuration.put(KafkaApplier.Configuration.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        configuration.put(KafkaApplier.Configuration.FORMAT, "avro");

        bootstrapReplicator = new BootstrapReplicator(configuration);
        bootstrapReplicator.run();

        BCachedSchemaRegistryClient scClient = new BCachedSchemaRegistryClient(schemaRegistryUrl, 100);
        Assert.assertTrue(scClient.getAllSubjects().contains("bigdata-replicator-organisms-value"));
    }

    @AfterClass
    public static void after() {
        BootstrapReplicatorTest.kafka.close();
        BootstrapReplicatorTest.mysqlBinaryLog.close();
        BootstrapReplicatorTest.mysqlActiveSchema.close();
        BootstrapReplicatorTest.kafkaZk.close();
        BootstrapReplicatorTest.schemaRegistry.close();
    }


}