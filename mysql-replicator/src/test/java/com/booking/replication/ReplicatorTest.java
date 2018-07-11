package com.booking.replication;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.Seeker;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.applier.kafka.KafkaSeeker;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterContext;
import com.booking.replication.augmenter.ActiveSchema;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.coordinator.ZookeeperCoordinator;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicatorTest {
    private static final Logger LOG = Logger.getLogger(ReplicatorTest.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ZOOKEEPER_LEADERSHIP_PATH = "/replicator/leadership";
    private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint";

    private static final String CHECKPOINT_DEFAULT = "{\"timestamp\": 0, \"serverId\": 1, \"gtid\": null, \"binlog\": {\"filename\": \"binlog.000001\", \"position\": 4}}";

    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String MYSQL_INIT_SCRIPT = "mysql.init.sql";
    private static final int TRANSACTION_LIMIT = 5;

    private static final String KAFKA_REPLICATOR_TOPIC_NAME = "replicator";
    private static final String KAFKA_REPLICATOR_GROUP_ID = "replicator";
    private static final String KAFKA_REPLICATOR_IT_GROUP_ID = "replicatorIT";
    private static final int KAFKA_TOPIC_PARTITIONS = 3;
    private static final int KAFKA_TOPIC_REPLICAS = 1;

    private static ServicesControl zookeeper;
    private static ServicesControl mysqlBinaryLog;
    private static ServicesControl mysqlActiveSchema;
    private static ServicesControl kafka;

    @BeforeClass
    public static void before() {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        ReplicatorTest.zookeeper = servicesProvider.startZookeeper();
        ReplicatorTest.mysqlBinaryLog = servicesProvider.startMySQL(ReplicatorTest.MYSQL_SCHEMA, ReplicatorTest.MYSQL_USERNAME, ReplicatorTest.MYSQL_PASSWORD, ReplicatorTest.MYSQL_INIT_SCRIPT);
        ReplicatorTest.mysqlActiveSchema = servicesProvider.startMySQL(ReplicatorTest.MYSQL_ACTIVE_SCHEMA, ReplicatorTest.MYSQL_USERNAME, ReplicatorTest.MYSQL_PASSWORD);
        ReplicatorTest.kafka = servicesProvider.startKafka(ReplicatorTest.KAFKA_REPLICATOR_TOPIC_NAME, ReplicatorTest.KAFKA_TOPIC_PARTITIONS, ReplicatorTest.KAFKA_TOPIC_REPLICAS);
    }

    @Test
    public void testReplicator() throws Exception {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ReplicatorTest.zookeeper.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, ReplicatorTest.ZOOKEEPER_LEADERSHIP_PATH);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(ReplicatorTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(ReplicatorTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, ReplicatorTest.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, ReplicatorTest.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, ReplicatorTest.MYSQL_PASSWORD);
        configuration.put(ActiveSchema.Configuration.MYSQL_HOSTNAME, ReplicatorTest.mysqlActiveSchema.getHost());
        configuration.put(ActiveSchema.Configuration.MYSQL_PORT, String.valueOf(ReplicatorTest.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchema.Configuration.MYSQL_SCHEMA, ReplicatorTest.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchema.Configuration.MYSQL_USERNAME, ReplicatorTest.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchema.Configuration.MYSQL_PASSWORD, ReplicatorTest.MYSQL_PASSWORD);
        configuration.put(AugmenterContext.Configuration.TRANSACTION_LIMIT, String.valueOf(ReplicatorTest.TRANSACTION_LIMIT));
        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), ReplicatorTest.kafka.getURL());
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), ReplicatorTest.kafka.getURL());
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.GROUP_ID_CONFIG), ReplicatorTest.KAFKA_REPLICATOR_GROUP_ID);
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        configuration.put(KafkaApplier.Configuration.TOPIC, ReplicatorTest.KAFKA_REPLICATOR_TOPIC_NAME);
        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());
        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.KAFKA.name());
        configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.TABLE_NAME.name());
        configuration.put(Applier.Configuration.TYPE, Applier.Type.KAFKA.name());
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, ReplicatorTest.ZOOKEEPER_CHECKPOINT_PATH);
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, ReplicatorTest.CHECKPOINT_DEFAULT);
        configuration.put(Replicator.Configuration.REPLICATOR_THREADS, String.valueOf(ReplicatorTest.KAFKA_TOPIC_PARTITIONS));
        configuration.put(Replicator.Configuration.REPLICATOR_TASKS, String.valueOf(ReplicatorTest.KAFKA_TOPIC_PARTITIONS));

        ReplicatorTest.LOG.log(Level.INFO, new ObjectMapper(new YAMLFactory()).writeValueAsString(configuration));

        Replicator replicator = new Replicator(configuration);

        replicator.start();

        replicator.wait(1L, TimeUnit.MINUTES);

        Map<String, Object> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ReplicatorTest.kafka.getURL());
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, ReplicatorTest.KAFKA_REPLICATOR_IT_GROUP_ID);
        kafkaConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaConfiguration, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(Collections.singleton(ReplicatorTest.KAFKA_REPLICATOR_TOPIC_NAME));

            boolean consumed = false;

            while (!consumed) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(1000L)) {
                    AugmentedEventHeader header = ReplicatorTest.MAPPER.readValue(record.key(), AugmentedEventHeader.class);
                    AugmentedEventData data = ReplicatorTest.MAPPER.readValue(record.value(), header.getEventType().getDefinition());

                    ReplicatorTest.LOG.log(Level.INFO, String.format(
                            "%s:%s",
                            ReplicatorTest.MAPPER.writeValueAsString(header),
                            ReplicatorTest.MAPPER.writeValueAsString(data)
                    ));

                    consumed = true;
                }
            }
        }

        replicator.stop();
    }

    @AfterClass
    public static void after() {
        ReplicatorTest.kafka.close();
        ReplicatorTest.mysqlBinaryLog.close();
        ReplicatorTest.mysqlActiveSchema.close();
        ReplicatorTest.zookeeper.close();
    }
}
