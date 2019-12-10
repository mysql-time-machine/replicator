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

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class BootstrapReplicatorIT {
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
                BootstrapReplicatorIT.MYSQL_SCHEMA,
                BootstrapReplicatorIT.MYSQL_USERNAME,
                BootstrapReplicatorIT.MYSQL_PASSWORD,
                BootstrapReplicatorIT.MYSQL_CONF_FILE,
                Collections.singletonList(BootstrapReplicatorIT.MYSQL_INIT_SCRIPT),
                null,
                null
        );

        MySQLConfiguration mySQLActiveSchemaConfiguration = new MySQLConfiguration(
                BootstrapReplicatorIT.MYSQL_ACTIVE_SCHEMA,
                BootstrapReplicatorIT.MYSQL_USERNAME,
                BootstrapReplicatorIT.MYSQL_PASSWORD,
                BootstrapReplicatorIT.MYSQL_CONF_FILE,
                Collections.emptyList(),
                null,
                null
        );

        BootstrapReplicatorIT.mysqlBinaryLog = servicesProvider.startMySQL(mySQLConfiguration);
        BootstrapReplicatorIT.mysqlActiveSchema = servicesProvider.startMySQL(mySQLActiveSchemaConfiguration);
        Network network = Network.newNetwork();
        BootstrapReplicatorIT.kafkaZk = servicesProvider.startZookeeper(network, "kafkaZk");
        BootstrapReplicatorIT.kafka = servicesProvider.startKafka(network, BootstrapReplicatorIT.KAFKA_REPLICATOR_TOPIC_NAME, 3, 1, "kafka");
        BootstrapReplicatorIT.schemaRegistry = servicesProvider.startSchemaRegistry(network);

    }

    @Test
    public void testBootstrap() throws Exception {
        HashMap<String, Object> configuration = new HashMap<>();

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(BootstrapReplicatorIT.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(BootstrapReplicatorIT.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, BootstrapReplicatorIT.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, BootstrapReplicatorIT.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, BootstrapReplicatorIT.MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, BootstrapReplicatorIT.mysqlActiveSchema.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(BootstrapReplicatorIT.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_ACTIVE_SCHEMA, BootstrapReplicatorIT.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, BootstrapReplicatorIT.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, BootstrapReplicatorIT.MYSQL_PASSWORD);
        configuration.put(Augmenter.Configuration.BOOTSTRAP, BootstrapReplicatorIT.BOOTSTRAP_ACTIVE);

        String schemaRegistryUrl = String.format("http://%s:%d", BootstrapReplicatorIT.schemaRegistry.getHost(), BootstrapReplicatorIT.schemaRegistry.getPort());
        configuration.put(KafkaApplier.Configuration.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        configuration.put(KafkaApplier.Configuration.FORMAT, "avro");

        bootstrapReplicator = new BootstrapReplicator(configuration);

        AtomicBoolean inProgress = new AtomicBoolean();
        bootstrapReplicator.run(inProgress);

        BCachedSchemaRegistryClient scClient = new BCachedSchemaRegistryClient(schemaRegistryUrl, 100);
        Assert.assertTrue(scClient.getAllSubjects().contains("bigdata-replicator-organisms-value"));
    }

    @AfterClass
    public static void after() {
        BootstrapReplicatorIT.kafka.close();
        BootstrapReplicatorIT.mysqlBinaryLog.close();
        BootstrapReplicatorIT.mysqlActiveSchema.close();
        BootstrapReplicatorIT.kafkaZk.close();
        BootstrapReplicatorIT.schemaRegistry.close();
    }


}