package com.booking.replication.it.hbase

import com.booking.replication.Replicator
import com.booking.replication.applier.Applier
import com.booking.replication.applier.Partitioner
import com.booking.replication.applier.Seeker
import com.booking.replication.applier.hbase.HBaseApplier
import com.booking.replication.applier.hbase.StorageConfig
import com.booking.replication.augmenter.ActiveSchemaManager
import com.booking.replication.augmenter.Augmenter
import com.booking.replication.augmenter.AugmenterContext
import com.booking.replication.augmenter.AugmenterFilter
import com.booking.replication.checkpoint.CheckpointApplier
import com.booking.replication.commons.conf.MySQLConfiguration
import com.booking.replication.commons.services.containers.hbase.HBaseContainer
import com.booking.replication.commons.services.containers.hbase.HBaseContainerProvider
import com.booking.replication.commons.services.containers.mysql.MySQLContainer
import com.booking.replication.commons.services.containers.mysql.MySQLContainerProvider
import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainer
import com.booking.replication.commons.services.containers.zookeeper.ZookeeperContainerProvider
import com.booking.replication.coordinator.Coordinator
import com.booking.replication.coordinator.ZookeeperCoordinator
import com.booking.replication.it.hbase.impl.TableWhiteListTest
import com.booking.replication.it.util.HBase
import com.booking.replication.supplier.Supplier
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier
import com.mysql.jdbc.Driver
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.testcontainers.containers.Network
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.TimeUnit

class ReplicatorHBasePipelineIntegrationTestRunner extends Specification {

    @Shared
    private static final Logger LOG = LogManager.getLogger(ReplicatorHBasePipelineIntegrationTestRunner.class)

    // TODO: add integration test for buffer size limit exceeded (rewind mode)
    @Shared
    private static final int AUGMENTER_TRANSACTION_BUFFER_SIZE_LIMIT = 100

    @Shared
    private static final int NUMBER_OF_THREADS = 3
    @Shared
    private static final int NUMBER_OF_TASKS = 3

    @Shared
    private static final String ZOOKEEPER_LEADERSHIP_PATH = "/replicator/leadership"
    @Shared
    private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint"

    @Shared
    private static final String CHECKPOINT_DEFAULT = "{\"timestamp\": 0, \"serverId\": 1, \"gtid\": null, \"binlog\": {\"filename\": \"binlog.000001\", \"position\": 4}}"

    @Shared
    private static final String MYSQL_SCHEMA = "replicator"
    @Shared
    private static final String MYSQL_ROOT_USERNAME = "root"
    @Shared
    private static final String MYSQL_USERNAME = "replicator"
    @Shared
    private static final String MYSQL_PASSWORD = "replicator"
    @Shared
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema"
    @Shared
    private static final String MYSQL_INIT_SCRIPT = "mysql.init.sql"
    @Shared
    private static final String MYSQL_CONF_FILE = "my.cnf"

    @Shared
    private static final String ACTIVE_SCHEMA_INIT_SCRIPT = "active_schema.init.sql"

    @Shared
    public static final String AUGMENTER_FILTER_TYPE = "TABLE_MERGE_PATTERN"
    @Shared
    public static final String AUGMENTER_FILTER_CONFIGURATION = "([_][12]\\d{3}(0[1-9]|1[0-2]))"

    @Shared
    private static final String HBASE_COLUMN_FAMILY_NAME = "d"
    @Shared
    public static final String HBASE_TEST_PAYLOAD_TABLE_NAME = "tbl_payload_context"

    // HBase/BigTable specific config
    @Shared
    public static final String HBASE_TARGET_NAMESPACE = getTargetNamespace()
    @Shared
    public static final String HBASE_SCHEMA_HISTORY_NAMESPACE = getSchemaNamespace()
    @Shared
    public static final String STORAGE_TYPE = getStorageType()
    @Shared
    public static final String BIGTABLE_PROJECT = getBigTableProject()
    @Shared
    public static final String BIGTABLE_INSTANCE = getBigTableInstance()

    @Shared
    private TESTS = [
            new TableWhiteListTest()
//            new TableNameMergeFilterTestImpl(),
//            new TransmitInsertsTestImpl(),
//            new MicrosecondValidationTestImpl(),
//            new LongTransactionTestImpl(),
//            new PayloadTableTestImpl(),
//            new SplitTransactionTestImpl()
    ]

    @Shared
    Network network = Network.newNetwork()

    @Shared
    ZookeeperContainer zookeeperContainer = ZookeeperContainerProvider
            .startWithNetwork(network, "replicatorZK")

    @Shared
    MySQLContainer mySQLBinaryLogContainer = MySQLContainerProvider.startWithMySQLConfiguration(
            new MySQLConfiguration(
                    MYSQL_SCHEMA,
                    MYSQL_USERNAME,
                    MYSQL_PASSWORD,
                    MYSQL_CONF_FILE,
                    Collections.singletonList(MYSQL_INIT_SCRIPT),
                    null,
                    null
            )
    )
    @Shared
    MySQLContainer mySQLActiveSchemaContainer = MySQLContainerProvider.startWithMySQLConfiguration(
            new MySQLConfiguration(
                    MYSQL_ACTIVE_SCHEMA,
                    MYSQL_USERNAME,
                    MYSQL_PASSWORD,
                    MYSQL_CONF_FILE,
                    Collections.singletonList(ACTIVE_SCHEMA_INIT_SCRIPT),
                    null,
                    null
            )
    )
    @Shared
    HBaseContainer hbaseContainer = HBaseContainerProvider.startWithNewNetworkAndZookeeper(zookeeperContainer)

    @Shared
    Replicator replicator

    static String getStorageType() {
        String storage = System.getProperty("sink")
        if (storage != null && storage == "bigtable") {
            return "BIGTABLE"
        } else {
            return "HBASE"
        }
    }

    static String getTargetNamespace() {
        String storage = System.getProperty("sink")
        if (storage != null && storage == "bigtable") {
            return ""
        } else {
            return "replicator_test"
        }
    }

    static String getSchemaNamespace() {
        String storage = System.getProperty("sink")
        if (storage != null && storage == "bigtable") {
            return ""
        } else {
            return "schema_history"
        }

    }

    static String getBigTableProject() {
        String projectID = System.getProperty("bigtable.projectID")
        if (projectID != null && projectID != "") {
            return projectID
        } else {
            return ""
        }
    }

    static String getBigTableInstance() {
        String instanceID = System.getProperty("bigtable.instanceID")
        if (instanceID != null && instanceID != "") {
            return instanceID
        } else {
            return ""
        }
    }

    void setupSpec() throws Exception {

        LOG.info("env: HBASE_TARGET_NAMESPACE => " + HBASE_TARGET_NAMESPACE)
        LOG.info("env: HBASE_SCHEMA_HISTORY_NAMESPACE => " + HBASE_SCHEMA_HISTORY_NAMESPACE)
        LOG.info("env: STORAGE_TYPE => " + STORAGE_TYPE)
        LOG.info("env: BIGTABLE_PROJECT => " + BIGTABLE_PROJECT)
        LOG.info("env: BIGTABLE_INSTANCE => " + BIGTABLE_INSTANCE)

        // start
        replicator = startReplicator()
    }

    def cleanupSpec() {

        LOG.info("tests done, shutting down replicator pipeline")

        // stop
        stopReplicator(replicator)

        hbaseContainer.stop()
        mySQLBinaryLogContainer.stop()
        mySQLActiveSchemaContainer.stop()
        zookeeperContainer.stop()

        LOG.info("pipeline stopped")

    }

    @Unroll
    def "#testName: { EXPECTED =>  #expected, RECEIVED => #received }"() {

        expect:
        received == expected

        where:
        testName << TESTS.collect({ test ->
            test.doAction(mySQLBinaryLogContainer)
            sleep(60000)
            ((HBaseApplier) replicator.getApplier()).forceFlushAll()
            test.testName()
        })
        expected << TESTS.collect({ test -> test.getExpectedState() })
        received << TESTS.collect({ test -> test.getActualState() })

    }

    private static stopReplicator(Replicator replicator) {
        replicator.stop()
    }

    private Replicator startReplicator() {

        LOG.info("waiting for containers setSink start...")

        // Active SchemaManager
        int counter = 60
        while (counter > 0) {
            Thread.sleep(1000)
            if (activeSchemaIsReady()) {
                LOG.info("ActiveSchemaManager container is ready.")
                break
            }
            counter--
        }

        // HBase
        counter = 60
        while (counter > 0) {
            Thread.sleep(1000)
            if (hbaseSanityCheck()) {
                LOG.info("HBase/BigTable is ready.")
                break
            }
            counter--
        }

        LOG.info("Starting the Replicator...")
        Replicator replicator = new Replicator(this.getConfiguration())

        replicator.start()

        replicator.wait(5L, TimeUnit.SECONDS)

        replicator
    }

    private boolean activeSchemaIsReady() {
        Map<String, Object> configuration = getConfiguration()
        Object hostname = configuration.get(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME)
        Object port = configuration.getOrDefault(ActiveSchemaManager.Configuration.MYSQL_PORT, "3306")
        Object schema = configuration.get(ActiveSchemaManager.Configuration.MYSQL_SCHEMA)
        Object username = configuration.get(ActiveSchemaManager.Configuration.MYSQL_USERNAME)
        Object password = configuration.get(ActiveSchemaManager.Configuration.MYSQL_PASSWORD)

        final BasicDataSource dataSource

        final String DEFAULT_MYSQL_DRIVER_CLASS = Driver.class.getName()
        Object driverClass = configuration.getOrDefault(ActiveSchemaManager.Configuration.MYSQL_DRIVER_CLASS,
                DEFAULT_MYSQL_DRIVER_CLASS)

        dataSource = getDataSource(driverClass.toString(), hostname.toString(), Integer.parseInt(port.toString()), schema.toString(), username.toString(), password.toString())

        try {
            java.sql.Connection connection = dataSource.getConnection()
            Statement statement = connection.createStatement()

            ResultSet resultSet = statement.executeQuery("select @@server_id")
            if (resultSet.next()) {
                LOG.info("got server id: " + resultSet.getString(1))
                return true
            }

        } catch (SQLException ignored) {
            return false
        }
        return false
    }

    private static BasicDataSource getDataSource(String driverClass, String hostname, int port, String schema, String username, String password) {
        BasicDataSource dataSource = new BasicDataSource()
        dataSource.setUrl(String.format("jdbc:mysql://%s:%d/%s", hostname, port, schema))
        dataSource.setUsername(username)
        dataSource.setPassword(password)

        return dataSource
    }

    boolean hbaseSanityCheck() {

        boolean passed = true

        String sanityCheckTableName = "sanity_check"

        try {
            // instantiate Configuration class

            StorageConfig storageConfig = StorageConfig.build(this.getConfiguration())
            Configuration config = storageConfig.getConfig()
            Connection connection = ConnectionFactory.createConnection(config)

            Admin admin = connection.getAdmin()

            if (STORAGE_TYPE == "HBASE") {

                LOG.info("storage type => " + STORAGE_TYPE)

                if (!HBASE_TARGET_NAMESPACE.empty) {
                    NamespaceDescriptor replicationNamespace =
                            NamespaceDescriptor.create(HBASE_TARGET_NAMESPACE).build()
                    admin.createNamespace(replicationNamespace)
                }
                if (!HBASE_SCHEMA_HISTORY_NAMESPACE.empty) {
                    NamespaceDescriptor schemaNamespace =
                            NamespaceDescriptor.create(HBASE_SCHEMA_HISTORY_NAMESPACE).build()
                    admin.createNamespace(schemaNamespace)
                }
                String clusterStatus = admin.getClusterStatus().toString()
                LOG.info("hbase cluster status => " + clusterStatus)

            }

            TableName tableName = TableName.valueOf(sanityCheckTableName)

            if (!admin.tableExists(tableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName)
                HColumnDescriptor cd = new HColumnDescriptor(HBASE_COLUMN_FAMILY_NAME)

                cd.setMaxVersions(1000)
                tableDescriptor.addFamily(cd)
                tableDescriptor.setCompactionEnabled(true)

                admin.createTable(tableDescriptor)

                LOG.info("Created table " + tableName)

            } else {
                LOG.info("table already exists, moving on...")
            }

            // write test data
            HashMap<String, HashMap<String, HashMap<Long, String>>> data = new HashMap<>()
            Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(sanityCheckTableName)))
            long timestamp = System.currentTimeMillis()

            for (int i = 0; i < 10; i++) {

                String rowKey = randString()

                data.put(rowKey, new HashMap<>())
                data.get(rowKey).put("c1", new HashMap<>())

                for (int v = 0; v < 10; v++) {

                    timestamp++

                    Put put = new Put(Bytes.toBytes(rowKey))
                    String value = randString()

                    data.get(rowKey).get("c1").put(timestamp, value)

                    put.addColumn(
                            Bytes.toBytes(HBASE_COLUMN_FAMILY_NAME),
                            Bytes.toBytes("c1"),
                            timestamp,
                            Bytes.toBytes(value)
                    )
                    table.put(put)
                }
            }

            // read
            Scan scan = new Scan()
            scan.setMaxVersions(1000)
            ResultScanner scanner = table.getScanner(scan)
            for (Result row : scanner) {
                for (Cell version : row.getColumnCells(Bytes.toBytes(HBASE_COLUMN_FAMILY_NAME), Bytes.toBytes("c1"))) {
                    String retrievedRowKey = Bytes.toString(row.getRow())
                    String retrievedValue = Bytes.toString(CellUtil.cloneValue(version))
                    Long retrievedTimestamp = version.getTimestamp()

                    if (!data.get(retrievedRowKey).get("c1").containsKey(retrievedTimestamp)) {
                        passed = false
                        break
                    }
                    if (data.get(retrievedRowKey).get("c1").get(retrievedTimestamp) != retrievedValue) {
                        passed = false
                        break
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace()
        }

        HBase.setConfiguration(this.getConfiguration())

        return passed
    }

    static String randString() {

        int leftLimit = 97   // letter 'a'
        int rightLimit = 122 // letter 'z'
        int targetStringLength = 3
        Random random = new Random()
        StringBuilder buffer = new StringBuilder(targetStringLength)
        for (int i = 0; i < targetStringLength; i++) {
            Number randomLimitedInt = leftLimit + ((Number)
                    (random.nextFloat() * (rightLimit - leftLimit + 1)))
            buffer.append(randomLimitedInt as char)
        }
        String generatedString = buffer.toString()

        return generatedString
    }

    private Map<String, Object> getConfiguration() {
        Map<String, Object> configuration = new HashMap<>()

        // Streams
        configuration.put(Replicator.Configuration.REPLICATOR_THREADS, String.valueOf(NUMBER_OF_THREADS))
        configuration.put(Replicator.Configuration.REPLICATOR_TASKS, String.valueOf(NUMBER_OF_TASKS))

        // Coordinator Configuration
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, ZOOKEEPER_CHECKPOINT_PATH)
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, CHECKPOINT_DEFAULT)
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name())
        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name())
        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, zookeeperContainer.getURL())
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, ZOOKEEPER_LEADERSHIP_PATH)

        // Supplier Configuration
        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name())
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(mySQLBinaryLogContainer.getHost()))
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(mySQLBinaryLogContainer.getPort()))
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, MYSQL_SCHEMA)
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, MYSQL_ROOT_USERNAME)
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, MYSQL_PASSWORD)

        // SchemaManager Manager Configuration
        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name())
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, mySQLActiveSchemaContainer.getHost())
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(mySQLActiveSchemaContainer.getPort()))
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, MYSQL_ACTIVE_SCHEMA)
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, MYSQL_ROOT_USERNAME)
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, MYSQL_PASSWORD)

        // Augmenter
        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(AUGMENTER_TRANSACTION_BUFFER_SIZE_LIMIT))
        configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true)

        configuration.put(AugmenterContext.Configuration.INCLUDE_TABLE, ['sometable_included'])

        configuration.put(AugmenterFilter.Configuration.FILTER_TYPE, AUGMENTER_FILTER_TYPE)
        configuration.put(AugmenterFilter.Configuration.FILTER_CONFIGURATION, AUGMENTER_FILTER_CONFIGURATION)

        // Applier Configuration
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.NONE.name())
        configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.TRID.name())
        configuration.put(Applier.Configuration.TYPE, Applier.Type.HBASE.name())

        // HBase Specifics
        configuration.put(HBaseApplier.Configuration.HBASE_ZOOKEEPER_QUORUM, "localhost:2181")
        configuration.put(HBaseApplier.Configuration.REPLICATED_SCHEMA_NAME, MYSQL_SCHEMA)

        configuration.put(HBaseApplier.Configuration.TARGET_NAMESPACE, HBASE_TARGET_NAMESPACE)
        configuration.put(HBaseApplier.Configuration.SCHEMA_HISTORY_NAMESPACE, HBASE_SCHEMA_HISTORY_NAMESPACE)

        configuration.put(HBaseApplier.Configuration.INITIAL_SNAPSHOT_MODE, false)
        configuration.put(HBaseApplier.Configuration.HBASE_USE_SNAPPY, false)
        configuration.put(HBaseApplier.Configuration.DRYRUN, false)

        configuration.put(HBaseApplier.Configuration.PAYLOAD_TABLE_NAME, HBASE_TEST_PAYLOAD_TABLE_NAME)

        configuration.put(StorageConfig.Configuration.TYPE, STORAGE_TYPE)
        configuration.put(StorageConfig.Configuration.BIGTABLE_INSTANCE_ID, BIGTABLE_INSTANCE)
        configuration.put(StorageConfig.Configuration.BIGTABLE_PROJECT_ID, BIGTABLE_PROJECT)

        return configuration
    }
}
