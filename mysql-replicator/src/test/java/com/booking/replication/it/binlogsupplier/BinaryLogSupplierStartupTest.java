package com.booking.replication.it.binlogsupplier;

import com.booking.replication.Replicator;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.Seeker;
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
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class BinaryLogSupplierStartupTest {
    private static String SERVER_UUID;
    private static final String INIT_SCRIPT = "./src/test/resources/binlogsupplier.init.sql";
    private static final String TEST_SCRIPT = "./src/test/resources/binlogsupplier.test.sql";
    private static final String CHECKPOINT_TMPL = "{\"timestamp\": 0, \"serverId\": 1, \"gtidSet\": {GTIDSET}, \"binlog\": {\"filename\": \"\", \"position\": null}}";
    private static final String EMPTY_CHECKPOINT = CHECKPOINT_TMPL.replace("{GTIDSET}","null");
    private static String INVALID_CHECKPOINT;

    private static final String PURGED_ERROR_MESSAGE = "The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires.";

    private static ServicesControl mysqlBinaryLog;
    private static MySQLConfiguration mysqlConfig = new MySQLConfiguration();
    private static final MySQLRunner MYSQL_RUNNER = new MySQLRunner();

    /*
     Tests:
        1) Startup and connect with invalid checkpoint with fallback set to false
            a) How to create said checkpoint?
                i) SELECT @@server_uuid
                ii) INVALID_CHECKPOINT.replace("{GTIDSET}",String(UUID + ":1-1");
            Expected:  should fail on connect with 'The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the
                       master has purged binary logs containing GTIDs that the slave requires.'
        2) Startup and connect without checkpoint with fallback set to true
            a) Add FALLBACK_TO_PURGED = true to the replicator config
            b) Set DEFAULT_CHECKPOINT to be empty, as invalid GTIDsets are *not* expected to connect properly.

            Expected:  should connect successfully, and being replicating from the earliest binlog (not current)
    */


    @BeforeClass
    public static void before() throws InterruptedException {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);
        mysqlBinaryLog = servicesProvider.startMySQL(BinaryLogSupplierStartupTest.mysqlConfig);

        try {
            Thread.sleep(20000L );
        }catch(Exception e){ throw new RuntimeException(e); }

        MYSQL_RUNNER.runMysqlScript( mysqlBinaryLog, mysqlConfig, INIT_SCRIPT, Collections.emptyMap(), true);

        List<HashMap<String,Object>> resultSet = MYSQL_RUNNER.runMysqlQuery( mysqlBinaryLog, mysqlConfig, "SELECT @@server_uuid", false);

        if ( resultSet.size() == 1 ) {
            for ( HashMap<String,Object> row : resultSet ) {
                SERVER_UUID = (String) row.get("@@server_uuid");
            }
        }

        if ( "".equals(SERVER_UUID) ) {
            throw new RuntimeException("We were unable to retrieve the UUID of the MySQL server, which is required to craft a (in)valid starting GTIDset");
        } else {
            INVALID_CHECKPOINT = CHECKPOINT_TMPL.replace("{GTIDSET}", "\"" + SERVER_UUID + ":1-1\"" );
        }

        MYSQL_RUNNER.runMysqlScript( mysqlBinaryLog, mysqlConfig, TEST_SCRIPT, Collections.emptyMap(), true);
    }

//    private String getServerUuid

    @Test
    public void testReplicator() throws Exception {
        Map<String,Object> configuration = this.getConfiguration();
        Replicator replicatorWithoutFallback = new Replicator(configuration);

        try {
            // First attempt contains an invalid checkpoint, proving that
            replicatorWithoutFallback.start();
            replicatorWithoutFallback.getCoordinator().getExecutor().awaitTermination( 3, TimeUnit.SECONDS );
            System.out.println( System.currentTimeMillis() );
        } catch ( Exception ex ) {
            assertTrue( ex.getMessage().contains(PURGED_ERROR_MESSAGE) );
        }

        assertTrue( !replicatorWithoutFallback.hasLeadership() );

        Replicator replicatorWithFallback    = new Replicator( replaceCheckpoint(configuration) );
        replicatorWithFallback.start();

        replicatorWithFallback.wait(25, TimeUnit.SECONDS );
        replicatorWithFallback.stop();

    }

    private Map<String,Object> replaceCheckpoint(Map<String,Object> config ) {

        config.replace(Replicator.Configuration.CHECKPOINT_DEFAULT,EMPTY_CHECKPOINT);
        return config;
    }

    private Map<String, Object> getConfiguration() {
        Map<String, Object> configuration = new HashMap<>();

        configuration.put(WebServer.Configuration.TYPE, WebServer.ServerType.NONE.name());

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(BinaryLogSupplierStartupTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(BinaryLogSupplierStartupTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, mysqlConfig.getSchema());
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, "root" ); //mysqlConfig.getUsername()
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, mysqlConfig.getPassword());

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, BinaryLogSupplierStartupTest.mysqlBinaryLog.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(BinaryLogSupplierStartupTest.mysqlBinaryLog.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_ACTIVE_SCHEMA, mysqlConfig.getSchema());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, "root"); // mysqlConfig.getUsername()
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, mysqlConfig.getPassword());

//        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(ActiveSchemaTest.TRANSACTION_LIMIT));
        configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true);
//        configuration.put(AugmenterContext.Configuration.INCLUDE_TABLE, ActiveSchemaTest.REPLICATOR_WHITELIST);


        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.FILE.name());

        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
        configuration.put(BinaryLogSupplier.Configuration.POSITION_TYPE, BinaryLogSupplier.PositionType.BINLOG);

        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.NONE.name());

        configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.TABLE_NAME.name());

        configuration.put(Applier.Configuration.TYPE, Applier.Type.CONSOLE.name());
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.NONE.name());

        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, "/tmp/checkpoint.txt");
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, INVALID_CHECKPOINT);

        configuration.put(Replicator.Configuration.REPLICATOR_THREADS, "1");
        configuration.put(Replicator.Configuration.REPLICATOR_TASKS, "1");

        return configuration;
    }


}
