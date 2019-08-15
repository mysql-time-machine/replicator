package com.booking.replication.it.flink;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.BinlogEventPartitioner;
import com.booking.replication.applier.Seeker;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.ActiveSchemaManager;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterContext;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
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
import com.booking.replication.flink.ReplicatorFlinkApplication;
import com.booking.replication.flink.ReplicatorFlinkSink;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;

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

public class ReplicatorFlinkKafkaTest {

    private static final Logger LOG = LogManager.getLogger(ReplicatorFlinkKafkaTest.class);

    private static final String ZOOKEEPER_LEADERSHIP_PATH = "/replicator/leadership";
    private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint";

    private static final String CHECKPOINT_DEFAULT = "{\"timestamp\": 0, \"serverId\": 1, \"gtid\": null, \"binlog\": {\"filename\": \"binlog.000001\", \"position\": 4}}";

    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String MYSQL_INIT_SCRIPT = "mysql.init.sql";
    private static final String MYSQL_TEST_SCRIPT = "mysql.binlog.flink.kafka.test.sql";
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
    private static ServicesControl kafkaZk;
    private static ServicesControl kafka;
    private static ServicesControl schemaRegistry;

    @BeforeClass
    public static void before() {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        ReplicatorFlinkKafkaTest.zookeeper = servicesProvider.startZookeeper();

        MySQLConfiguration mySQLConfiguration = new MySQLConfiguration(
                ReplicatorFlinkKafkaTest.MYSQL_SCHEMA,
                ReplicatorFlinkKafkaTest.MYSQL_USERNAME,
                ReplicatorFlinkKafkaTest.MYSQL_PASSWORD,
                ReplicatorFlinkKafkaTest.MYSQL_CONF_FILE,
                Collections.singletonList(ReplicatorFlinkKafkaTest.MYSQL_INIT_SCRIPT),
                null,
                null
        );

        MySQLConfiguration mySQLActiveSchemaConfiguration = new MySQLConfiguration(
                ReplicatorFlinkKafkaTest.MYSQL_ACTIVE_SCHEMA,
                ReplicatorFlinkKafkaTest.MYSQL_USERNAME,
                ReplicatorFlinkKafkaTest.MYSQL_PASSWORD,
                ReplicatorFlinkKafkaTest.MYSQL_CONF_FILE,
                Collections.emptyList(),
                null,
                null
        );

        ReplicatorFlinkKafkaTest.mysqlBinaryLog = servicesProvider.startMySQL(mySQLConfiguration);
        ReplicatorFlinkKafkaTest.mysqlActiveSchema = servicesProvider.startMySQL(mySQLActiveSchemaConfiguration);

        try {
            // give some time for containers to start
            // TODO: make this nicer with a proper wait
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Kafka
        Network network = Network.newNetwork();
        ReplicatorFlinkKafkaTest.kafkaZk = servicesProvider.startZookeeper(network, "kafkaZk");
        ReplicatorFlinkKafkaTest.kafka = servicesProvider.startKafka(
                network,
                ReplicatorFlinkKafkaTest.KAFKA_REPLICATOR_TOPIC_NAME,
                ReplicatorFlinkKafkaTest.KAFKA_TOPIC_PARTITIONS,
                ReplicatorFlinkKafkaTest.KAFKA_TOPIC_REPLICAS,
                "kafka"
        );
        ReplicatorFlinkKafkaTest.schemaRegistry = servicesProvider.startSchemaRegistry(network);

    }

    @Test
    public void testReplicator() throws Exception {

        ReplicatorFlinkApplication replicator = new ReplicatorFlinkApplication(this.getConfiguration());

        System.out.println("Starting the replicator");

        try {
            replicator.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        File file = new File("src/test/resources/" + ReplicatorFlinkKafkaTest.MYSQL_TEST_SCRIPT);

        System.out.println("Try running mysql scripts");

        runMysqlScripts(this.getConfiguration(), file.getAbsolutePath());

        Thread.sleep(15000);

        System.out.println("Finished running mysql scripts");

        List<String> results = new ArrayList<>();

        new Thread(() -> {
            Map<String, Object> kafkaConfiguration = new HashMap<>();

            kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ReplicatorFlinkKafkaTest.kafka.getURL());
            kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, ReplicatorFlinkKafkaTest.KAFKA_REPLICATOR_IT_GROUP_ID);
            kafkaConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            System.out.println("Try to get data from Kafka");

            try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaConfiguration, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {

                consumer.subscribe(Collections.singleton(ReplicatorFlinkKafkaTest.KAFKA_REPLICATOR_TOPIC_NAME));

                boolean consumed = false;

                while (!consumed) {

                    System.out.println("poll...");

                    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(1000L)) {

                        AugmentedEvent augmentedEvent = AugmentedEvent.fromJSON(record.key(), record.value());

                        ReplicatorFlinkKafkaTest.LOG.info("Got new augmented event from Kafka: " + new String(augmentedEvent.toJSON()));

                        if ((augmentedEvent.getHeader().getEventType() == AugmentedEventType.WRITE_ROWS)) {
                            for (AugmentedRow augmentedRow : ((WriteRowsAugmentedEventData) augmentedEvent.getData()).getAugmentedRows()) {
                                String value = augmentedRow.getStringifiedRowColumns().get("name").get("value");
                                results.add(value);
                            }
                        }

                        consumed = true;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(5000);

        TreeSet resultSet = new TreeSet(results);

        TreeSet expectedSet =new TreeSet();
        expectedSet.add("lion");
        expectedSet.add("tiger");
        expectedSet.add("panther");
        expectedSet.add("cat");
        expectedSet.add("bird");

        Assert.assertTrue("At least on delivery test", resultSet.equals(expectedSet));

        replicator.stop();
    }

    private boolean runMysqlScripts(Map<String, Object> configuration, String scriptFilePath) {
        BufferedReader reader;
        Statement statement;
        BasicDataSource dataSource = initDatasource(configuration, Driver.class.getName());
        try (Connection connection = dataSource.getConnection()) {
            statement = connection.createStatement();
            reader = new BufferedReader(new FileReader(scriptFilePath));
            String line;
            // read script line by line
            ReplicatorFlinkKafkaTest.LOG.info("Executing query from " + scriptFilePath);
            String s;
            StringBuilder sb = new StringBuilder();

            FileReader fr = new FileReader(new File(scriptFilePath));
            BufferedReader br = new BufferedReader(fr);
            while ((s = br.readLine()) != null) {
                sb.append(s);
            }
            br.close();

            String[] inst = sb.toString().split(";");
            for (String query : inst) {
                if (!query.trim().equals("")) {
                    statement.execute(query);
                    ReplicatorFlinkKafkaTest.LOG.info(query);
                }
            }
            return true;
        } catch (Exception exception) {
            ReplicatorFlinkKafkaTest.LOG.warn(String.format("error executing query \"%s\": %s", scriptFilePath, exception.getMessage()));
            return false;
        }

    }

    private BasicDataSource initDatasource(Map<String, Object> configuration, Object driverClass) {
        List<String> hostnames = (List<String>) configuration.get(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME);
        Object port = configuration.getOrDefault(BinaryLogSupplier.Configuration.MYSQL_PORT, "3306");
        Object schema = configuration.get(BinaryLogSupplier.Configuration.MYSQL_SCHEMA);
        Object username = configuration.get(BinaryLogSupplier.Configuration.MYSQL_USERNAME);
        Object password = configuration.get(BinaryLogSupplier.Configuration.MYSQL_PASSWORD);

        Objects.requireNonNull(hostnames, String.format("Configuration required: %s", BinaryLogSupplier.Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", BinaryLogSupplier.Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", BinaryLogSupplier.Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", BinaryLogSupplier.Configuration.MYSQL_PASSWORD));

        return this.getDataSource(driverClass.toString(), hostnames.get(0), Integer.parseInt(port.toString()), schema.toString(), username.toString(), password.toString());
    }

    private BasicDataSource getDataSource(String driverClass, String hostname, int port, String schema, String username, String password) {
        BasicDataSource dataSource = new BasicDataSource();

        dataSource.setDriverClassName(driverClass);
        dataSource.setUrl(String.format(CONNECTION_URL_FORMAT, hostname, port, schema));
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    private Map<String, Object> getConfiguration() {

        Map<String, Object> configuration = new HashMap<>();

        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ReplicatorFlinkKafkaTest.zookeeper.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, ReplicatorFlinkKafkaTest.ZOOKEEPER_LEADERSHIP_PATH);

        configuration.put(WebServer.Configuration.TYPE, WebServer.ServerType.JETTY.name());

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(ReplicatorFlinkKafkaTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(ReplicatorFlinkKafkaTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, ReplicatorFlinkKafkaTest.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, ReplicatorFlinkKafkaTest.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, ReplicatorFlinkKafkaTest.MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, ReplicatorFlinkKafkaTest.mysqlActiveSchema.getHost());

        System.out.println("PORT => " + ReplicatorFlinkKafkaTest.mysqlActiveSchema.getPort());

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(ReplicatorFlinkKafkaTest.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, ReplicatorFlinkKafkaTest.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, ReplicatorFlinkKafkaTest.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, ReplicatorFlinkKafkaTest.MYSQL_PASSWORD);

        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(ReplicatorFlinkKafkaTest.TRANSACTION_LIMIT));
        configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true);

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());

        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());

        configuration.put(BinaryLogSupplier.Configuration.POSITION_TYPE, BinaryLogSupplier.PositionType.BINLOG);

        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.NONE.name());

        configuration.put(BinlogEventPartitioner.Configuration.TYPE, BinlogEventPartitioner.Type.TABLE_NAME.name());

        configuration.put(Applier.Configuration.TYPE, Applier.Type.KAFKA);

        configuration.put(ReplicatorFlinkSink.Configuration.BOOTSTRAP_SERVERS_CONFIG, ReplicatorFlinkKafkaTest.kafka.getURL());
        configuration.put(ReplicatorFlinkSink.Configuration.TOPIC, ReplicatorFlinkKafkaTest.KAFKA_REPLICATOR_TOPIC_NAME);

        configuration.put(
                KafkaApplier.Configuration.SCHEMA_REGISTRY_URL,
                String.format("http://%s:%d", ReplicatorFlinkKafkaTest.schemaRegistry.getHost(),
                        ReplicatorFlinkKafkaTest.schemaRegistry.getPort()));
        configuration.put(KafkaApplier.Configuration.FORMAT, "json");


        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
        configuration.put(ReplicatorFlinkApplication.Configuration.CHECKPOINT_PATH, ReplicatorFlinkKafkaTest.ZOOKEEPER_CHECKPOINT_PATH);
        configuration.put(ReplicatorFlinkApplication.Configuration.CHECKPOINT_DEFAULT, ReplicatorFlinkKafkaTest.CHECKPOINT_DEFAULT);

        configuration.put(BinaryLogSupplier.Configuration.GTID_FALLBACK_TO_PURGED, true);

        return configuration;
    }

    @AfterClass
    public static void after() {

        ReplicatorFlinkKafkaTest.mysqlBinaryLog.close();
        ReplicatorFlinkKafkaTest.mysqlActiveSchema.close();
        ReplicatorFlinkKafkaTest.zookeeper.close();
        ReplicatorFlinkKafkaTest.kafka.close();
        ReplicatorFlinkKafkaTest.kafkaZk.close();
        ReplicatorFlinkKafkaTest.schemaRegistry.close();

    }
}
