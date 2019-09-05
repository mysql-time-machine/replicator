package com.booking.utils;

import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.applier.schema.registry.BCachedSchemaRegistryClient;
import com.booking.replication.augmenter.ActiveSchemaManager;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.commons.conf.MySQLConfiguration;
import com.booking.replication.commons.services.containers.kafka.KafkaContainer;
import com.booking.replication.commons.services.containers.kafka.KafkaContainerNetworkConfig;
import com.booking.replication.commons.services.containers.kafka.KafkaContainerProvider;
import com.booking.replication.commons.services.containers.kafka.KafkaContainerTopicConfig;
import com.booking.replication.commons.services.containers.mysql.MySQLContainer;
import com.booking.replication.commons.services.containers.mysql.MySQLContainerProvider;
import com.booking.replication.commons.services.containers.schemaregistry.SchemaRegistryContainer;
import com.booking.replication.commons.services.containers.schemaregistry.SchemaRegistryContainerProvider;
import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainer;
import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainerProvider;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;

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

    private static MySQLContainer mysqlBinaryLogContainer;
    private static MySQLContainer mysqlActiveSchemaContainer;
    private static ZookeeperContainer kafkaZookeeperContainer;
    private static KafkaContainer kafkaContainer;
    private static SchemaRegistryContainer schemaRegistryContainer;

    private BootstrapReplicator bootstrapReplicator;

    @BeforeClass
    public static void before() {
        final MySQLConfiguration mySQLConfiguration = new MySQLConfiguration(
            MYSQL_SCHEMA,
            MYSQL_USERNAME,
            MYSQL_PASSWORD,
            MYSQL_CONF_FILE,
            Collections.singletonList(MYSQL_INIT_SCRIPT),
            null,
            null
        );

        final MySQLConfiguration mySQLActiveSchemaConfiguration = new MySQLConfiguration(
            MYSQL_ACTIVE_SCHEMA,
            MYSQL_USERNAME,
            MYSQL_PASSWORD,
            MYSQL_CONF_FILE,
            Collections.emptyList(),
            null,
            null
        );

        mysqlBinaryLogContainer = MySQLContainerProvider.startWithMySQLConfiguration(mySQLConfiguration);
        mysqlActiveSchemaContainer = MySQLContainerProvider.startWithMySQLConfiguration(mySQLActiveSchemaConfiguration);

        final Network network = Network.newNetwork();

        kafkaZookeeperContainer = ZookeeperContainerProvider.startWithNetworkAndPortBindings(network, "kafkaZk");
        kafkaContainer = KafkaContainerProvider.start(
            new KafkaContainerNetworkConfig(network, "kafka"),
            new KafkaContainerTopicConfig(KAFKA_REPLICATOR_TOPIC_NAME, 3, 1),
            kafkaZookeeperContainer
        );
        schemaRegistryContainer = SchemaRegistryContainerProvider.startWithKafka(kafkaZookeeperContainer);
    }

    @Test
    public void testBootstrap() throws Exception {
        HashMap<String, Object> configuration = new HashMap<>();

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(mysqlBinaryLogContainer.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(mysqlBinaryLogContainer.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, mysqlActiveSchemaContainer.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(mysqlActiveSchemaContainer.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, MYSQL_PASSWORD);
        configuration.put(Augmenter.Configuration.BOOTSTRAP, BOOTSTRAP_ACTIVE);

        String schemaRegistryUrl = String.format("http://%s:%d", schemaRegistryContainer.getHost(), schemaRegistryContainer.getPort());
        configuration.put(KafkaApplier.Configuration.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        configuration.put(KafkaApplier.Configuration.FORMAT, "avro");

        bootstrapReplicator = new BootstrapReplicator(configuration);
        bootstrapReplicator.run();

        BCachedSchemaRegistryClient scClient = new BCachedSchemaRegistryClient(schemaRegistryUrl, 100);
        Assert.assertTrue(scClient.getAllSubjects().contains("bigdata-replicator-organisms-value"));
    }

    @AfterClass
    public static void after() {
        kafkaContainer.stop();
        mysqlBinaryLogContainer.stop();
        mysqlActiveSchemaContainer.stop();
        kafkaZookeeperContainer.stop();
        schemaRegistryContainer.stop();
    }
}