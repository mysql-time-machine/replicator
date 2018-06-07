package com.booking.replication;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Seeker;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.applier.kafka.KafkaSeeker;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.active.schema.ActiveSchemaAugmenter;
import com.booking.replication.augmenter.active.schema.ActiveSchemaContext;
import com.booking.replication.augmenter.active.schema.ActiveSchemaManager;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.coordinator.ZookeeperCoordinator;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ReplicatorIT {
    private static final String ZOOKEEPER_LEADERSHIP_PATH = "/replicator/leadership";
    private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint";

    private static final String CHECKPOINT_DEFAULT = "{\"serverId\": 1, \"binlogFilename\": \"binlog.000001\", \"binlogPosition\": 0, \"pseudoGTID\": null, \"pseudoGTIDIndex\": 0}";

    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String MYSQL_INIT_SCRIPT = "mysql.init.sql";
    private static final int MYSQL_TRANSACTION_LIMIT = 5;

    private static final String KAFKA_TOPIC_NAME = "replicator";
    private static final String KAFKA_GROUP_ID = "replicator";
    private static final int KAFKA_TOPIC_PARTITIONS = 3;
    private static final int KAFKA_TOPIC_REPLICAS = 1;

    private static ServicesControl zookeeper;
    private static ServicesControl mysqlBinaryLog;
    private static ServicesControl mysqlActiveSchema;
    private static ServicesControl kafka;

    @BeforeClass
    public static void before() {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        ReplicatorIT.zookeeper = servicesProvider.startZookeeper();
        ReplicatorIT.mysqlBinaryLog = servicesProvider.startMySQL(ReplicatorIT.MYSQL_SCHEMA, ReplicatorIT.MYSQL_USERNAME, ReplicatorIT.MYSQL_PASSWORD, ReplicatorIT.MYSQL_INIT_SCRIPT);
        ReplicatorIT.mysqlActiveSchema = servicesProvider.startMySQL(ReplicatorIT.MYSQL_ACTIVE_SCHEMA, ReplicatorIT.MYSQL_USERNAME, ReplicatorIT.MYSQL_PASSWORD);
        ReplicatorIT.kafka = servicesProvider.startKafka(ReplicatorIT.KAFKA_TOPIC_NAME, ReplicatorIT.KAFKA_TOPIC_PARTITIONS, ReplicatorIT.KAFKA_TOPIC_REPLICAS);
    }

    @Test
    public void testReplicator() {
        Map<String, String> configuration = new HashMap<>();

        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ReplicatorIT.zookeeper.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, ReplicatorIT.ZOOKEEPER_LEADERSHIP_PATH);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, ReplicatorIT.mysqlBinaryLog.getHost());
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(ReplicatorIT.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, ReplicatorIT.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, ReplicatorIT.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, ReplicatorIT.MYSQL_PASSWORD);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, ReplicatorIT.mysqlActiveSchema.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(ReplicatorIT.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, ReplicatorIT.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, ReplicatorIT.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, ReplicatorIT.MYSQL_PASSWORD);
        configuration.put(ActiveSchemaContext.Configuration.MYSQL_TRANSACTION_LIMIT, String.valueOf(ReplicatorIT.MYSQL_TRANSACTION_LIMIT));
        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, "bootstrap.servers"), ReplicatorIT.kafka.getURL());
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, "bootstrap.servers"), ReplicatorIT.kafka.getURL());
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, "group.id"), ReplicatorIT.KAFKA_GROUP_ID);
        configuration.put(KafkaApplier.Configuration.TOPIC, ReplicatorIT.KAFKA_TOPIC_NAME);
        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());
        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
        configuration.put(Augmenter.Configuration.TYPE, Augmenter.Type.ACTIVE_SCHEMA.name());
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.KAFKA.name());
        configuration.put(Applier.Configuration.TYPE, Applier.Type.CONSOLE.name());
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, ReplicatorIT.ZOOKEEPER_CHECKPOINT_PATH);
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, ReplicatorIT.CHECKPOINT_DEFAULT);

        Replicator replicator = new Replicator(configuration);

        replicator.start();
        replicator.join();
    }

    @AfterClass
    public static void after() {
        ReplicatorIT.kafka.close();
        ReplicatorIT.mysqlBinaryLog.close();
        ReplicatorIT.mysqlActiveSchema.close();
        ReplicatorIT.zookeeper.close();
    }
}
