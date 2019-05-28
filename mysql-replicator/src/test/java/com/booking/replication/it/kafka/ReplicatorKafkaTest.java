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
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.conf.MySQLConfiguration;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import com.booking.replication.controller.WebServer;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.coordinator.ZookeeperCoordinator;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;

import com.mysql.jdbc.Driver;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
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
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.*;
import java.sql.Connection;

import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ReplicatorKafkaTest {
    private static final Logger LOG = LogManager.getLogger(ReplicatorKafkaTest.class);

    private static final String ZOOKEEPER_LEADERSHIP_PATH = "/replicator/leadership";
    private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint";

    private static final String CHECKPOINT_DEFAULT = "{\"timestamp\": 0, \"serverId\": 1, \"gtid\": null, \"binlog\": {\"filename\": \"binlog.000001\", \"position\": 4}}";

    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String MYSQL_INIT_SCRIPT = "mysql.init.sql";
    private static final String MYSQL_TEST_SCRIPT = "mysql.binlog.test.sql";
    private static final String MYSQL_CONF_FILE = "my.cnf";
    private static final int TRANSACTION_LIMIT = 5;
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
    private static ServicesControl schemaRegistry;
    private static ServicesControl kafkaZk;

    @BeforeClass
    public static void before() {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        ReplicatorKafkaTest.zookeeper = servicesProvider.startZookeeper();

        MySQLConfiguration mySQLConfiguration = new MySQLConfiguration(
                ReplicatorKafkaTest.MYSQL_SCHEMA,
                ReplicatorKafkaTest.MYSQL_USERNAME,
                ReplicatorKafkaTest.MYSQL_PASSWORD,
                ReplicatorKafkaTest.MYSQL_CONF_FILE,
                Collections.singletonList(ReplicatorKafkaTest.MYSQL_INIT_SCRIPT),
                null,
                null
                );

        MySQLConfiguration mySQLActiveSchemaConfiguration = new MySQLConfiguration(
                ReplicatorKafkaTest.MYSQL_ACTIVE_SCHEMA,
                ReplicatorKafkaTest.MYSQL_USERNAME,
                ReplicatorKafkaTest.MYSQL_PASSWORD,
                ReplicatorKafkaTest.MYSQL_CONF_FILE,
                Collections.emptyList(),
                null,
                null
        );

        ReplicatorKafkaTest.mysqlBinaryLog = servicesProvider.startMySQL(mySQLConfiguration);
        ReplicatorKafkaTest.mysqlActiveSchema = servicesProvider.startMySQL(mySQLActiveSchemaConfiguration);
        Network network = Network.newNetwork();
        ReplicatorKafkaTest.kafkaZk = servicesProvider.startZookeeper(network, "kafkaZk");
        ReplicatorKafkaTest.kafka = servicesProvider.startKafka(network, ReplicatorKafkaTest.KAFKA_REPLICATOR_TOPIC_NAME, ReplicatorKafkaTest.KAFKA_TOPIC_PARTITIONS, ReplicatorKafkaTest.KAFKA_TOPIC_REPLICAS, "kafka");
        ReplicatorKafkaTest.schemaRegistry = servicesProvider.startSchemaRegistry(network);
    }

    @Test
    public void testReplicator() throws Exception {
        Replicator replicator = new Replicator(this.getConfiguration());

        replicator.start();

        File file = new File("src/test/resources/" + ReplicatorKafkaTest.MYSQL_TEST_SCRIPT);

        runMysqlScripts(this.getConfiguration(), file.getAbsolutePath());

        replicator.wait(1L, TimeUnit.MINUTES);

        Map<String, Object> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ReplicatorKafkaTest.kafka.getURL());
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, ReplicatorKafkaTest.KAFKA_REPLICATOR_IT_GROUP_ID);
        kafkaConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String schemaRegistryUrl = (String) this.getConfiguration().get(KafkaApplier.Configuration.SCHEMA_REGISTRY_URL);
        kafkaConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(client);

        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaConfiguration, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(Collections.singleton(ReplicatorKafkaTest.KAFKA_REPLICATOR_TOPIC_NAME));

            boolean consumed = false;

            while (!consumed) {

                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(1000L)) {
                    GenericRecord deserialize = (GenericRecord) kafkaAvroDeserializer.deserialize("", record.value());
                    System.out.println(deserialize.toString());
//                    AugmentedEvent augmentedEvent = AugmentedEvent.fromJSON(record.key(), record.value());

//                    ReplicatorKafkaTest.LOG.info(new String(augmentedEvent.toJSON()));

                    System.out.println(new String(record.key()));

                    consumed = true;
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
            ReplicatorKafkaTest.LOG.info("Executing query from " + scriptFilePath);
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
                    ReplicatorKafkaTest.LOG.info(query);
                }
            }
            return true;
        } catch (Exception exception) {
            ReplicatorKafkaTest.LOG.warn(String.format("error executing query \"%s\": %s", scriptFilePath, exception.getMessage()));
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

        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ReplicatorKafkaTest.zookeeper.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, ReplicatorKafkaTest.ZOOKEEPER_LEADERSHIP_PATH);

        configuration.put(WebServer.Configuration.TYPE, WebServer.ServerType.JETTY.name());

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(ReplicatorKafkaTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(ReplicatorKafkaTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, ReplicatorKafkaTest.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, ReplicatorKafkaTest.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, ReplicatorKafkaTest.MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, ReplicatorKafkaTest.mysqlActiveSchema.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(ReplicatorKafkaTest.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, ReplicatorKafkaTest.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, ReplicatorKafkaTest.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, ReplicatorKafkaTest.MYSQL_PASSWORD);

        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(ReplicatorKafkaTest.TRANSACTION_LIMIT));
        configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true);

        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), ReplicatorKafkaTest.kafka.getURL());
        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), ByteArraySerializer.class);
        configuration.put(String.format("%s%s", KafkaApplier.Configuration.PRODUCER_PREFIX, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), KafkaAvroSerializer.class);
        configuration.put(KafkaApplier.Configuration.SCHEMA_REGISTRY_URL, String.format("http://%s:%d", ReplicatorKafkaTest.schemaRegistry.getHost(), ReplicatorKafkaTest.schemaRegistry.getPort()));
        configuration.put(KafkaApplier.Configuration.FORMAT, "avro");

        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), ReplicatorKafkaTest.kafka.getURL());
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.GROUP_ID_CONFIG), ReplicatorKafkaTest.KAFKA_REPLICATOR_GROUP_ID);
        configuration.put(String.format("%s%s", KafkaSeeker.Configuration.CONSUMER_PREFIX, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        configuration.put(KafkaApplier.Configuration.TOPIC, ReplicatorKafkaTest.KAFKA_REPLICATOR_TOPIC_NAME);

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());

        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
        configuration.put(BinaryLogSupplier.Configuration.POSITION_TYPE, BinaryLogSupplier.PositionType.BINLOG);

        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.KAFKA.name());

        configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.TABLE_NAME.name());

        configuration.put(Applier.Configuration.TYPE, Applier.Type.KAFKA.name());
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, ReplicatorKafkaTest.ZOOKEEPER_CHECKPOINT_PATH);
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, ReplicatorKafkaTest.CHECKPOINT_DEFAULT);
        configuration.put(Replicator.Configuration.REPLICATOR_THREADS, String.valueOf(ReplicatorKafkaTest.KAFKA_TOPIC_PARTITIONS));
        configuration.put(Replicator.Configuration.REPLICATOR_TASKS, String.valueOf(ReplicatorKafkaTest.KAFKA_TOPIC_PARTITIONS));

        return configuration;
    }

    @AfterClass
    public static void after() {
        ReplicatorKafkaTest.kafka.close();
        ReplicatorKafkaTest.mysqlBinaryLog.close();
        ReplicatorKafkaTest.mysqlActiveSchema.close();
        ReplicatorKafkaTest.schemaRegistry.close();
        ReplicatorKafkaTest.zookeeper.close();
        ReplicatorKafkaTest.kafkaZk.close();
    }

    public static String avroToJson(byte[] avro, Schema schema) throws IOException {
        boolean pretty = false;
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
        Object datum = reader.read(null, decoder);
        writer.write(datum, encoder);
        encoder.flush();
        output.flush();
        return new String(output.toByteArray(), "UTF-8");
    }
}
