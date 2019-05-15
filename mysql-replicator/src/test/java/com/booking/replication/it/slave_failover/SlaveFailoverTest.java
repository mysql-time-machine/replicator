package com.booking.replication.it.slave_failover;

import avro.shaded.com.google.common.collect.Maps;
import com.booking.replication.Replicator;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.count.CountApplier;
import com.booking.replication.augmenter.ActiveSchemaManager;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterContext;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.conf.MySQLConfiguration;
import com.booking.replication.commons.services.ServicesControl;
import com.booking.replication.commons.services.ServicesProvider;
import com.booking.replication.controller.WebServer;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.it.util.MySQLRunner;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assume.assumeTrue;

public class SlaveFailoverTest {
    private static final Logger LOG = LogManager.getLogger(SlaveFailoverTest.class);

    private static String FILE_CHECKPOINT_PATH;

    private static final String CHECKPOINT_INITIALIZE = "{\"timestamp\": 0, "
            + "\"serverId\": 1,\"gtidSet\": \"%s\", \"gtid\": null, "
            + "\"binlog\": {\"filename\": \"\", \"position\": \"\"}}";

    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String MYSQL_MASTER_INIT_SCRIPT = "mysql.master.init.sql";
    private static final String MYSQL_SLAVE_INIT_SCRIPT = "mysql.slave.init.sql";
    private static final String MYSQL_CONF_FILE = "my.cnf";
    private static final String MYSQL_SLAVE_CONF_FILE = "myslave.cnf";
    private static final MySQLRunner MYSQL_RUNNER = new MySQLRunner();
    private static final String MYSQL_TEST_SCRIPT = "mysql.init.sql";
    private static final String MYSQL_SLAVE2_TEST_SCRIPT = "mysql.slave2.test.sql";
    private static final int TRANSACTION_LIMIT = 100;

    private static ServicesControl mysqlMaster;
    private static ServicesControl mysqlSlave1;
    private static ServicesControl mysqlSlave2;
    private static ServicesControl mysqlActiveSchema;

    private static MySQLConfiguration mySQLMasterConfiguration;
    private static MySQLConfiguration mySQLSlave1Configuration;
    private static MySQLConfiguration mySQLSlave2Configuration;
    private static MySQLConfiguration mySQLActiveSchemaConfiguration;

    private static Coordinator testCoordinator;

    private static final ImmutableMap<String, Long> expectedSlave1Counts = ImmutableMap.of(
            "QUERY", 2L,
            "DELETE_ROWS", 2L,
            "WRITE_ROWS", 14L,
            "UPDATE_ROWS", 1L
    );

    private static final ImmutableMap<String, Long> expectedSlave2Counts = ImmutableMap.of(
            "DELETE_ROWS", 2L,
            "WRITE_ROWS", 1L,
            "UPDATE_ROWS", 4L
    );


    @BeforeClass
    public static void before() throws InterruptedException {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        Network network = Network.newNetwork();

        LOG.info("Iniitializing mysql replication chain with 1 master and 2 slaves");

        mySQLMasterConfiguration = new MySQLConfiguration(
                SlaveFailoverTest.MYSQL_SCHEMA,
                SlaveFailoverTest.MYSQL_USERNAME,
                SlaveFailoverTest.MYSQL_PASSWORD,
                SlaveFailoverTest.MYSQL_CONF_FILE,
                Collections.singletonList(SlaveFailoverTest.MYSQL_MASTER_INIT_SCRIPT),
                network,
                "mysql-chain"
        );

        mySQLSlave1Configuration = new MySQLConfiguration(
                SlaveFailoverTest.MYSQL_SCHEMA,
                null,
                SlaveFailoverTest.MYSQL_PASSWORD,
                SlaveFailoverTest.MYSQL_SLAVE_CONF_FILE,
                Collections.emptyList(),
                network,
                "mysql-chain"
        );

        mySQLSlave2Configuration = new MySQLConfiguration(
                SlaveFailoverTest.MYSQL_SCHEMA,
                null,
                SlaveFailoverTest.MYSQL_PASSWORD,
                SlaveFailoverTest.MYSQL_SLAVE_CONF_FILE,
                Collections.emptyList(),
                network,
                "mysql-chain"
        );

        mySQLActiveSchemaConfiguration = new MySQLConfiguration(
                SlaveFailoverTest.MYSQL_ACTIVE_SCHEMA,
                SlaveFailoverTest.MYSQL_USERNAME,
                SlaveFailoverTest.MYSQL_PASSWORD,
                SlaveFailoverTest.MYSQL_CONF_FILE,
                Collections.emptyList(),
                network,
                "mysql-chain"
        );

        mysqlMaster = servicesProvider.startMySQL(mySQLMasterConfiguration);

        // Initialize replication on slave 1
        mysqlSlave1 = servicesProvider.startMySQL(mySQLSlave1Configuration);

        // Initialize replication on slave 2
        mysqlSlave2 = servicesProvider.startMySQL(mySQLSlave2Configuration);


        mysqlActiveSchema = servicesProvider.startMySQL(mySQLActiveSchemaConfiguration);

        boolean s1Initialized = initializeSlave(mysqlMaster, mysqlSlave1, mySQLSlave1Configuration);
        boolean s2Initialized = initializeSlave(mysqlMaster, mysqlSlave2, mySQLSlave2Configuration);
        assumeTrue("Mysql slaves failed to initialize",s1Initialized && s2Initialized );

        assumeTrue("File checkpoint failed to initialize",
                initializeGtidCheckpoint(mysqlSlave1, mySQLSlave1Configuration));


        LOG.info("Mysql replication chain initialized.. "
                + "Sleeping for 30 seconds to propagate changes");
        // Sleep for 30 seconds for master-slave mysql chain to initialize
        // Alternatively can poll slaves to check their state
        TimeUnit.SECONDS.sleep(30);
    }

    private static boolean initializeSlave(ServicesControl mysqlMaster,
                                            ServicesControl mysqlSlave,
                                            MySQLConfiguration configuration) {
        return MYSQL_RUNNER.runMysqlScript(mysqlSlave,
                configuration,
                new File("src/test/resources/" + SlaveFailoverTest.MYSQL_SLAVE_INIT_SCRIPT).getAbsolutePath(),
                ImmutableMap.of("MASTER_HOSTNAME", mysqlMaster.getContainer().getContainerInfo().getConfig().getHostName(),
                        "MASTER_USER", MYSQL_USERNAME,
                        "MASTER_PASSWORD", MYSQL_PASSWORD),
                true
        );
    }

    private static boolean initializeGtidCheckpoint(ServicesControl mysql,
                                                 MySQLConfiguration configuration) {
        List<HashMap<String,Object>> rs = MYSQL_RUNNER.runMysqlQuery(mysql,
                configuration,
                "SELECT @@GLOBAL.gtid_executed as gtid_executed;",
                true);

        String gtidSet = rs.stream()
                .map(row -> row.get("gtid_executed").toString().replace("\n",""))
                .collect(Collectors.joining( "," ));

        String cp = String.format(CHECKPOINT_INITIALIZE, gtidSet);

        try {
            File tempFile = File.createTempFile("replicator_checkpoint", null);
            FILE_CHECKPOINT_PATH = tempFile.getAbsolutePath();
            LOG.info("Created checkpoint file: " + FILE_CHECKPOINT_PATH);
            PrintWriter out = new PrintWriter(FILE_CHECKPOINT_PATH);
            out.println(cp);
            out.close();
        } catch (IOException e) {
            LOG.info("Error in creating tmp file for checkpoint :\n" + e.getMessage());
            return false;
        }
        return true;
    }

    @Test
    public void testReplicator() {
        // Initialize and start replicator with slave 1
        Replicator replicator = new Replicator(this.getConfiguration(mysqlSlave1));
        replicator.start();

        // Run some initial queries on master
        boolean execBinLog = MYSQL_RUNNER.runMysqlScript(
                mysqlMaster,
                mySQLMasterConfiguration,
                new File("src/test/resources/" + SlaveFailoverTest.MYSQL_TEST_SCRIPT).getAbsolutePath(),
                Collections.emptyMap(),
                false
        );
        Assert.assertTrue(execBinLog);

        // Wait for events to get captured by replicator
        replicator.wait(30, TimeUnit.SECONDS);

        // Validate if the event type counts from slave 1 queries match
        if (replicator.getApplier() instanceof  CountApplier) {
            Map<String, Long> eventCounts = ((CountApplier) replicator.getApplier()).getEventCounts();
            LOG.info("Event Counts (slave 1) - " + eventCounts.toString());
            Assert.assertTrue(Maps.difference(expectedSlave1Counts, eventCounts).areEqual());
        }

        replicator.stop();

        // Run some more queries on master
        boolean execBinLog2 = MYSQL_RUNNER.runMysqlScript(
                mysqlMaster,
                mySQLMasterConfiguration,
                new File("src/test/resources/" + SlaveFailoverTest.MYSQL_SLAVE2_TEST_SCRIPT).getAbsolutePath(),
                Collections.emptyMap(),
                false
        );
        Assert.assertTrue(execBinLog2);

        // Re-Initialize and start replicator with slave 2
        replicator = new Replicator(this.getConfiguration(mysqlSlave2));
        replicator.start();

        // Wait for events to get captured by replicator
        replicator.wait(30, TimeUnit.SECONDS);

        // Validate if the event type counts from slave 2 queries match
        if (replicator.getApplier() instanceof  CountApplier) {
            Map<String, Long> eventCounts = ((CountApplier) replicator.getApplier()).getEventCounts();
            LOG.info("Event Counts (slave 2) - " + eventCounts.toString());
            Assert.assertTrue(Maps.difference(expectedSlave2Counts, eventCounts).areEqual());
        }

        replicator.stop();
    }

    private Map<String, Object> getConfiguration(ServicesControl mysql) {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put(WebServer.Configuration.TYPE, WebServer.ServerType.JETTY.name());

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(mysql.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(mysql.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, mysqlActiveSchema.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, MYSQL_PASSWORD);

        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(TRANSACTION_LIMIT));
        configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true);

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.FILE.name());
        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());

        configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.TABLE_NAME.name());

        configuration.put(Applier.Configuration.TYPE, Applier.Type.COUNT.name());
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, FILE_CHECKPOINT_PATH);

        return configuration;
    }

    @AfterClass
    public static void after() {
        mysqlMaster.close();
        mysqlSlave1.close();
        mysqlSlave2.close();
        mysqlActiveSchema.close();
    }
}
