package com.booking.replication;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.Seeker;
import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.augmenter.ActiveSchemaManager;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterContext;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.coordinator.ZookeeperCoordinator;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;

import com.booking.replication.spec.BasicHBaseTransmitSpec;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.Driver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ReplicatorHBaseTest {
    private static final Logger LOG = Logger.getLogger(ReplicatorHBaseTest.class.getName());

    private static final String ZOOKEEPER_LEADERSHIP_PATH = "/replicator/leadership";
    private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint";

    private static final String CHECKPOINT_DEFAULT = "{\"timestamp\": 0, \"serverId\": 1, \"gtid\": null, \"binlog\": {\"filename\": \"binlog.000001\", \"position\": 4}}";

    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String MYSQL_INIT_SCRIPT = "mysql.init.sql";

    private static final String ACTIVE_SCHEMA_INIT_SCRIPT = "active_schema.init.sql";

    private static final int TRANSACTION_LIMIT = 100;

    private static final String HBASE_COLUMN_FAMILY_NAME = "d";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ServicesControl zookeeper;
    private static ServicesControl mysqlBinaryLog;
    private static ServicesControl mysqlActiveSchema;
    private static ServicesControl hbase;

    @BeforeClass
    public static void before() {

        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        Network network = Network.newNetwork();

        ReplicatorHBaseTest.zookeeper = servicesProvider.startZookeeper(network);
        ReplicatorHBaseTest.mysqlBinaryLog = servicesProvider.startMySQL(ReplicatorHBaseTest.MYSQL_SCHEMA, ReplicatorHBaseTest.MYSQL_USERNAME, ReplicatorHBaseTest.MYSQL_PASSWORD, ReplicatorHBaseTest.MYSQL_INIT_SCRIPT);
        ReplicatorHBaseTest.mysqlActiveSchema = servicesProvider.startMySQL(ReplicatorHBaseTest.MYSQL_ACTIVE_SCHEMA, ReplicatorHBaseTest.MYSQL_USERNAME, ReplicatorHBaseTest.MYSQL_PASSWORD, ReplicatorHBaseTest.ACTIVE_SCHEMA_INIT_SCRIPT);
        ReplicatorHBaseTest.hbase = servicesProvider.startHbase();
    }

    @Test
    public void testReplicator() throws Exception {

        LOG.info("waiting for containers to start...");
        // Active SchemaManager
        int counter = 60;
        while (counter > 0) {
            Thread.sleep(1000);
            if (activeSchemaIsReady()) {
                LOG.info("ActiveSchemaManager container is ready.");
                break;
            }
            counter--;
        }
        // HBase
        counter = 60;
        while (counter > 0) {
            Thread.sleep(1000);
            if (hbaseSanityCheck()) {
                LOG.info("HBase container is ready.");
                break;
            }
            counter--;
        }

        LOG.info("Starting the Replicator...");
        Replicator replicator = new Replicator(this.getConfiguration());

        replicator.start();

        replicator.wait(2L, TimeUnit.MINUTES);

        Thread.sleep(10);

        BasicHBaseTransmitSpec testSpec = new BasicHBaseTransmitSpec();

        int cnt = 100000000;
        while (--cnt > 0) {

            Thread.sleep(5000);
            HashMap<String, HashMap<String, HashMap<Long, String>>> retrieved =
                    testSpec.retrieveReplicatedDataFromHBase("organisms");

            String got = MAPPER.writeValueAsString(retrieved);

            LOG.info("got from hbase => " + got);
        }
        Thread.sleep(10000000);

        replicator.stop();

        org.junit.Assert.assertTrue(true); // TODO: port integration tests from v014/v015
    }

    private boolean activeSchemaIsReady() {

        Map<String, Object> configuration = getConfiguration();

        Object hostname = configuration.get(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME);

        Object port = configuration.getOrDefault(ActiveSchemaManager.Configuration.MYSQL_PORT, "3306");

        Object schema = configuration.get(ActiveSchemaManager.Configuration.MYSQL_SCHEMA);
        Object username = configuration.get(ActiveSchemaManager.Configuration.MYSQL_USERNAME);
        Object password = configuration.get(ActiveSchemaManager.Configuration.MYSQL_PASSWORD);

        final BasicDataSource dataSource;

        final String DEFAULT_MYSQL_DRIVER_CLASS = Driver.class.getName();
        Object driverClass = configuration.getOrDefault(ActiveSchemaManager.Configuration.MYSQL_DRIVER_CLASS,
                DEFAULT_MYSQL_DRIVER_CLASS);

        dataSource = getDataSource(driverClass.toString(), hostname.toString(), Integer.parseInt(port.toString()), schema.toString(), username.toString(), password.toString());

        try (java.sql.Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            ResultSet resultSet = statement.executeQuery("select @@server_id");
            if (resultSet.next()) {
                LOG.info("got server id: " + resultSet.getString(1));
                return true;
            }

        } catch (SQLException exception) {
            return false;
        }
        return false;
    }

    private BasicDataSource getDataSource(String driverClass, String hostname, int port, String schema, String username, String password) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(String.format("jdbc:mysql://%s:%d/%s", hostname, port, schema));
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    public boolean hbaseSanityCheck() {

        boolean passed = true;

        try {
            // instantiate Configuration class
            Configuration config = HBaseConfiguration.create();

            Connection connection = ConnectionFactory.createConnection(config);

            Admin admin = connection.getAdmin();

            String clusterStatus = admin.getClusterStatus().toString();
            // LOG.info("hbase cluster status => " + clusterStatus);

            TableName tableName = TableName.valueOf("test1");
            if (!admin.tableExists(tableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor cd = new HColumnDescriptor(HBASE_COLUMN_FAMILY_NAME);

                cd.setMaxVersions(1000);
                tableDescriptor.addFamily(cd);
                tableDescriptor.setCompactionEnabled(true);

                admin.createTable(tableDescriptor);
                // LOG.info("created table");

            }

            // write test data
            HashMap<String,HashMap<String, HashMap<Long, String>>> data = new HashMap<>();
            Table table = connection.getTable(TableName.valueOf(Bytes.toBytes("test1")));
            long timestamp = System.currentTimeMillis();

            for (int i = 0; i < 10; i++) {

                String rowKey = randString();

                data.put(rowKey, new HashMap<>());
                data.get(rowKey).put("c1", new HashMap<>());

                for (int v = 0; v < 10; v++) {

                    timestamp++;

                    Put put = new Put(Bytes.toBytes(rowKey));
                    String value = randString();

                    data.get(rowKey).get("c1").put(timestamp, value);

                    put.addColumn(
                            Bytes.toBytes(HBASE_COLUMN_FAMILY_NAME),
                            Bytes.toBytes("c1"),
                            timestamp,
                            Bytes.toBytes(value)
                    );
                    table.put(put);
                }
            }

            // read
            Scan scan = new Scan();
            scan.setMaxVersions(1000);
            ResultScanner scanner = table.getScanner(scan);
            for (Result row : scanner) {

                for (Cell version: row.getColumnCells(Bytes.toBytes(HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes("c1"))) {

                    String retrievedRowKey    = Bytes.toString(row.getRow());
                    String retrievedValue     = Bytes.toString(version.getValue());
                    Long   retrievedTimestamp = version.getTimestamp();

                    if (!data.get(retrievedRowKey).get("c1").containsKey(retrievedTimestamp)) {
                        passed = false;
                        break;
                    }
                    if (!data.get(retrievedRowKey).get("c1").get(retrievedTimestamp).equals(retrievedValue)) {
                        passed = false;
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return passed;
    }

    public static String randString() {

        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 3;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String generatedString = buffer.toString();

        return generatedString;
    }

    private Map<String, Object> getConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        // Coordinator Configuration
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, ReplicatorHBaseTest.ZOOKEEPER_CHECKPOINT_PATH);
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, ReplicatorHBaseTest.CHECKPOINT_DEFAULT);
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());
        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ReplicatorHBaseTest.zookeeper.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, ReplicatorHBaseTest.ZOOKEEPER_LEADERSHIP_PATH);

        // Supplier Configuration
        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(ReplicatorHBaseTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(ReplicatorHBaseTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, ReplicatorHBaseTest.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, ReplicatorHBaseTest.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, ReplicatorHBaseTest.MYSQL_PASSWORD);

        // SchemaManager Manager Configuration
        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, ReplicatorHBaseTest.mysqlActiveSchema.getHost());

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(ReplicatorHBaseTest.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, ReplicatorHBaseTest.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, ReplicatorHBaseTest.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, ReplicatorHBaseTest.MYSQL_PASSWORD);

        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(ReplicatorHBaseTest.TRANSACTION_LIMIT));

        // Applier Configuration
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.NONE.name());
        configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.XXID.name());
        configuration.put(Applier.Configuration.TYPE, Applier.Type.HBASE.name());

        // HBase Specifics
        configuration.put(HBaseApplier.Configuration.HBASE_ZOOKEEPER_QUORUM, "localhost:2181");
        configuration.put(HBaseApplier.Configuration.REPLICATED_SCHEMA_NAME, ReplicatorHBaseTest.MYSQL_SCHEMA);

        configuration.put(HBaseApplier.Configuration.TARGET_NAMESPACE,  "");
        configuration.put(HBaseApplier.Configuration.SCHEMA_HISTORY_NAMESPACE, "");

        configuration.put(HBaseApplier.Configuration.INITIAL_SNAPSHOT_MODE, false);
        configuration.put(HBaseApplier.Configuration.HBASE_USE_SNAPPY, false);
        configuration.put(HBaseApplier.Configuration.DRYRUN, false);

        return configuration;
    }

    @AfterClass
    public static void after() {
        ReplicatorHBaseTest.hbase.close();
        ReplicatorHBaseTest.mysqlBinaryLog.close();
        ReplicatorHBaseTest.mysqlActiveSchema.close();
        ReplicatorHBaseTest.zookeeper.close();
    }
}
