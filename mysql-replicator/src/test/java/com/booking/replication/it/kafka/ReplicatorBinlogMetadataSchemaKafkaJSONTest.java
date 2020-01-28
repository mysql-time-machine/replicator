package com.booking.replication.it.kafka;

import com.booking.replication.Replicator;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.Seeker;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.applier.kafka.KafkaSeeker;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ReplicatorBinlogMetadataSchemaKafkaJSONTest {

        private static final Logger LOG = LogManager.getLogger(com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.class);

        private static final String ZOOKEEPER_LEADERSHIP_PATH = "/replicator/leadership";
        private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint";

        private static final String CHECKPOINT_DEFAULT = "{\"timestamp\": 0, \"serverId\": 1, \"gtid\": null, \"binlog\": {\"filename\": \"binlog.000001\", \"position\": 4}}";

        private static final String MYSQL_SCHEMA = "replicator";
        private static final String MYSQL_ROOT_USERNAME = "root";
        private static final String MYSQL_USERNAME = "replicator";
        private static final String MYSQL_PASSWORD = "replicator";
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
        private static ServicesControl mysqlBinaryLogProvider;
        private static ServicesControl kafka;
        private static ServicesControl kafkaZk;

        @BeforeClass
        public static void before() {
            ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

            com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.zookeeper = servicesProvider.startZookeeper();

            MySQLConfiguration mySQLConfiguration = new MySQLConfiguration(
                    com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_SCHEMA,
                    com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_USERNAME,
                    com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_PASSWORD,
                    com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_CONF_FILE,
                    Collections.singletonList(com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_INIT_SCRIPT),
                    null,
                    null
            );

            com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.mysqlBinaryLogProvider =
                    servicesProvider.startMySQL(mySQLConfiguration);

            Network network = Network.newNetwork();
            com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.kafkaZk = servicesProvider.startZookeeper(network, "kafkaZk");
            com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.kafka = servicesProvider.startKafka(network, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_REPLICATOR_TOPIC_NAME, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_TOPIC_PARTITIONS, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_TOPIC_REPLICAS, "kafka");
        }

        @Test
        public void testReplicator() throws Exception {
            Replicator replicator = new Replicator(this.getConfiguration());

            replicator.start();

            File file = new File("src/test/resources/" + com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_TEST_SCRIPT);

            runMysqlScripts(this.getConfiguration(), file.getAbsolutePath());

            replicator.wait(1L, TimeUnit.MINUTES);

            Map<String, Object> kafkaConfiguration = new HashMap<>();

            kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.kafka.getURL());
            kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_REPLICATOR_IT_GROUP_ID);
            kafkaConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            ObjectMapper MAPPER = new ObjectMapper();

            try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaConfiguration, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                consumer.subscribe(Collections.singleton(com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_REPLICATOR_TOPIC_NAME));

                boolean consumed = false;
                boolean isMarkedRow = false;

                while (!consumed) {
                    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(1000L)) {

                        System.out.println(record.toString());
                        System.out.println("key => " + new String(record.key()));
                        System.out.println("value => " + new String(record.value()));

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
                }
            }
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
                com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.LOG.info("Executing query from " + scriptFilePath);
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
                        com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.LOG.info(query);
                    }
                }
                return true;
            } catch (Exception exception) {
                com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.LOG.warn(String.format("error executing query \"%s\": %s", scriptFilePath, exception.getMessage()));
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

            configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.zookeeper.getURL());
            configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.ZOOKEEPER_LEADERSHIP_PATH);

            configuration.put(WebServer.Configuration.TYPE, WebServer.ServerType.JETTY.name());

            configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.mysqlBinaryLogProvider.getHost()));
            configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.mysqlBinaryLogProvider.getPort()));
            configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_SCHEMA);
            configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_ROOT_USERNAME);
            configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.MYSQL_PASSWORD);

            configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.TRANSACTION_LIMIT));
            configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true);

            configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.kafka.getURL());
            configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), ByteArraySerializer.class);
            configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), KafkaAvroSerializer.class);
            configuration.put(KafkaApplier.Configuration.FORMAT, "json");

            configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.kafka.getURL());
            configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.GROUP_ID_CONFIG), com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_REPLICATOR_GROUP_ID);
            configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
            configuration.put(KafkaApplier.Configuration.TOPIC, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_REPLICATOR_TOPIC_NAME);

            configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());

            configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
            configuration.put(BinaryLogSupplier.Configuration.POSITION_TYPE, BinaryLogSupplier.PositionType.BINLOG);

            configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.BINLOG_METADATA.name());
            configuration.put(Seeker.Configuration.TYPE, Seeker.Type.KAFKA.name());

            configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.TABLE_NAME.name());

            configuration.put(Applier.Configuration.TYPE, Applier.Type.KAFKA.name());
            configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
            configuration.put(Replicator.Configuration.CHECKPOINT_PATH, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.ZOOKEEPER_CHECKPOINT_PATH);
            configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.CHECKPOINT_DEFAULT);
            configuration.put(Replicator.Configuration.REPLICATOR_THREADS, String.valueOf(com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_TOPIC_PARTITIONS));
            configuration.put(Replicator.Configuration.REPLICATOR_TASKS, String.valueOf(com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.KAFKA_TOPIC_PARTITIONS));

            return configuration;
        }

        @AfterClass
        public static void after() {
            com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.kafka.close();
            com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.mysqlBinaryLogProvider.close();
            com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.zookeeper.close();
            com.booking.replication.it.kafka.ReplicatorBinlogMetadataSchemaKafkaJSONTest.kafkaZk.close();
        }

    }
