package com.booking.replication.it.kafka;

import com.booking.replication.Replicator;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.Seeker;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.applier.kafka.KafkaSeeker;
import com.booking.replication.augmenter.ActiveSchemaManager;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterContext;
import com.booking.replication.augmenter.model.event.AugmentedEventHeader;
import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.conf.MySQLConfiguration;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import com.booking.replication.controller.WebServer;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.coordinator.ZookeeperCoordinator;
import com.booking.replication.it.util.MySQLRunner;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.Driver;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.*;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

// ReplicatorActiveSchemaKafkaJSONTest
public class ReplicatorKafkaJSONTest {

    private static final Logger LOG = LogManager.getLogger(ReplicatorKafkaJSONTest.class);
    private static final MySQLRunner MYSQL_RUNNER = new MySQLRunner();

    private static final String ZOOKEEPER_LEADERSHIP_PATH = "/replicator/leadership";
    private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint";

    private static final String CHECKPOINT_DEFAULT = "{\"timestamp\": 0, \"serverId\": 1, \"gtid\": null, \"binlog\": {\"filename\": \"binlog.000001\", \"position\": 4}}";

    private static MySQLConfiguration MYSQL_CONFIG = new MySQLConfiguration(
            ReplicatorKafkaJSONTest.MYSQL_SCHEMA,
            ReplicatorKafkaJSONTest.MYSQL_USERNAME,
            ReplicatorKafkaJSONTest.MYSQL_PASSWORD,
            ReplicatorKafkaJSONTest.MYSQL_CONF_FILE,
            Collections.singletonList(ReplicatorKafkaJSONTest.MYSQL_INIT_SCRIPT),
            null,
            null
    );
    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String MYSQL_INIT_SCRIPT = "mysql.init.sql";
    private static final String MYSQL_TEST_SCRIPT = "mysql.binlog.test.sql";
    private static final String MYSQL_CONF_FILE = "my.cnf";
    private static final int TRANSACTION_LIMIT = 1000;
    private static final String CONNECTION_URL_FORMAT = "jdbc:mysql://%s:%d/%s";

    private static final String KAFKA_REPLICATOR_TOPIC_NAME = "replicator";
    private static final String KAFKA_REPLICATOR_GROUP_ID = "replicator";
    private static final String KAFKA_REPLICATOR_IT_GROUP_ID = "replicatorIT";
    private static final int KAFKA_TOPIC_PARTITIONS = 3;
    private static final int KAFKA_TOPIC_REPLICAS = 1;

    private static ServicesControl zookeeper;
    private static ServicesControl mysqlBinaryLog;
    private static ServicesControl mysqlActiveSchema;
    private static ServicesControl kafka;
    private static ServicesControl kafkaZk;

    @BeforeClass
    public static void before() {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);
        ReplicatorKafkaJSONTest.zookeeper = servicesProvider.startZookeeper();

        MySQLConfiguration mySQLActiveSchemaConfiguration = new MySQLConfiguration(
                ReplicatorKafkaJSONTest.MYSQL_ACTIVE_SCHEMA,
                ReplicatorKafkaJSONTest.MYSQL_USERNAME,
                ReplicatorKafkaJSONTest.MYSQL_PASSWORD,
                ReplicatorKafkaJSONTest.MYSQL_CONF_FILE,
                Collections.emptyList(),
                null,
                null
        );

        ReplicatorKafkaJSONTest.mysqlBinaryLog = servicesProvider.startMySQL(MYSQL_CONFIG);
        ReplicatorKafkaJSONTest.mysqlActiveSchema = servicesProvider.startMySQL(mySQLActiveSchemaConfiguration);
        Network network = Network.newNetwork();
        ReplicatorKafkaJSONTest.kafkaZk = servicesProvider.startZookeeper(network, "kafkaZk");
        ReplicatorKafkaJSONTest.kafka = servicesProvider.startKafka(network, ReplicatorKafkaJSONTest.KAFKA_REPLICATOR_TOPIC_NAME, ReplicatorKafkaJSONTest.KAFKA_TOPIC_PARTITIONS, ReplicatorKafkaJSONTest.KAFKA_TOPIC_REPLICAS, "kafka");
    }

    @Test
    public void testReplicator() throws Exception {
        Replicator replicator = new Replicator(this.getConfiguration());

        replicator.start();

        File file = new File("src/test/resources/" + ReplicatorKafkaJSONTest.MYSQL_TEST_SCRIPT);

        MYSQL_RUNNER.runMysqlScript( ReplicatorKafkaJSONTest.mysqlBinaryLog, MYSQL_CONFIG, file.getAbsolutePath(), null, true);

        replicator.wait(1L, TimeUnit.MINUTES);

        Map<String, Object> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ReplicatorKafkaJSONTest.kafka.getURL());
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, ReplicatorKafkaJSONTest.KAFKA_REPLICATOR_IT_GROUP_ID);
        kafkaConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper MAPPER = new ObjectMapper();

        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaConfiguration, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(Collections.singleton(ReplicatorKafkaJSONTest.KAFKA_REPLICATOR_TOPIC_NAME));

            boolean consumed = false;
            boolean isMarkedRow = false;
            while (!consumed) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(1000L)) {
                    AugmentedEventHeader h = MAPPER.readValue(record.key(), AugmentedEventHeader.class);

                    if (h.getTableName().equals("organisms")) {

                        if (h.getEventType().equals(AugmentedEventType.INSERT)) {

                            WriteRowsAugmentedEventData augmentedEventData = MAPPER.readValue(record.value(), WriteRowsAugmentedEventData.class);

                            for (AugmentedRow row : augmentedEventData.getRows()) {

                                for (String key : row.getValues().keySet()) {
                                    if (key.equals("id")) {
                                        if (row.getValues().get(key).toString().equals("2")) {
                                            isMarkedRow = true;
                                        }
                                    }
                                }

                                if (isMarkedRow) {
                                    for (String key : row.getValues().keySet()) {
                                        System.out.println(key + " => " + row.getValues().get(key).toString());
                                        String colVal = row.getValues().get(key).toString();

                                        switch (key) {
                                            case "name":
                                                Assert.assertEquals("name", "Ñandú", colVal);
                                                break;
                                            case "lifespan":
                                                Assert.assertEquals("lifespan", "240", colVal);
                                                break;
                                            case "lifespan_small":
                                                Assert.assertEquals("lifespan_small", "65500", colVal);
                                                break;
                                            case "lifespan_medium":
                                                Assert.assertEquals("lifespan_medium", "16770215", colVal);
                                                break;
                                            case "lifespan_int":
                                                Assert.assertEquals("lifespan_int", "4294897295", colVal);
                                                break;
                                            case "lifespan_bigint":
                                                Assert.assertEquals("lifespan_bigint", "18446744071615", colVal);
                                                break;
                                            case "bits":
                                                Assert.assertEquals("bits", "10101010", colVal);
                                                break;
                                            case "soylent_dummy_id":
                                                Assert.assertEquals("soylent_dummy_id", "000001348BB470A5129E6C8D332D89CC", colVal);
                                                break;
                                            case "mydecimal":
                                                Assert.assertEquals("mydecimal", "100.000000000", colVal);
                                                break;
                                            case "kingdom":
                                                Assert.assertEquals("kingdom", "animalia", colVal);
                                                break;
                                            default:
                                                break;
                                        }
                                    }
                                    isMarkedRow = false;
                                }
                            }
                        }
                    }
                }
                consumed = true;
            }
        }

        replicator.stop();
    }

    private Map<String, Object> getConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ReplicatorKafkaJSONTest.zookeeper.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, ReplicatorKafkaJSONTest.ZOOKEEPER_LEADERSHIP_PATH);

        configuration.put(WebServer.Configuration.TYPE, WebServer.ServerType.JETTY.name());

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(ReplicatorKafkaJSONTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(ReplicatorKafkaJSONTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, ReplicatorKafkaJSONTest.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, ReplicatorKafkaJSONTest.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, ReplicatorKafkaJSONTest.MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, ReplicatorKafkaJSONTest.mysqlActiveSchema.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(ReplicatorKafkaJSONTest.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_ACTIVE_SCHEMA, ReplicatorKafkaJSONTest.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, ReplicatorKafkaJSONTest.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, ReplicatorKafkaJSONTest.MYSQL_PASSWORD);

        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(ReplicatorKafkaJSONTest.TRANSACTION_LIMIT));
        configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true);

        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), ReplicatorKafkaJSONTest.kafka.getURL());
        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), ByteArraySerializer.class);
        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), KafkaAvroSerializer.class);
        configuration.put(KafkaApplier.Configuration.FORMAT, "json");

        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), ReplicatorKafkaJSONTest.kafka.getURL());
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.GROUP_ID_CONFIG), ReplicatorKafkaJSONTest.KAFKA_REPLICATOR_GROUP_ID);
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        configuration.put(KafkaApplier.Configuration.TOPIC, ReplicatorKafkaJSONTest.KAFKA_REPLICATOR_TOPIC_NAME);

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());

        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
        configuration.put(BinaryLogSupplier.Configuration.POSITION_TYPE, BinaryLogSupplier.PositionType.BINLOG);

        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.KAFKA.name());

        configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.TABLE_NAME.name());

        configuration.put(Applier.Configuration.TYPE, Applier.Type.KAFKA.name());
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, ReplicatorKafkaJSONTest.ZOOKEEPER_CHECKPOINT_PATH);
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, ReplicatorKafkaJSONTest.CHECKPOINT_DEFAULT);
        configuration.put(Replicator.Configuration.REPLICATOR_THREADS, String.valueOf(ReplicatorKafkaJSONTest.KAFKA_TOPIC_PARTITIONS));
        configuration.put(Replicator.Configuration.REPLICATOR_TASKS, String.valueOf(ReplicatorKafkaJSONTest.KAFKA_TOPIC_PARTITIONS));

        return configuration;
    }

    @AfterClass
    public static void after() {
        ReplicatorKafkaJSONTest.kafka.close();
        ReplicatorKafkaJSONTest.mysqlBinaryLog.close();
        ReplicatorKafkaJSONTest.mysqlActiveSchema.close();
        ReplicatorKafkaJSONTest.zookeeper.close();
        ReplicatorKafkaJSONTest.kafkaZk.close();
    }

}