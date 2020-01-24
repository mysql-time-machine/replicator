package com.booking.replication.it.activeschema;

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
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import com.mysql.jdbc.Driver;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ActiveSchemaTest {

    private static final Logger LOG = LogManager.getLogger(ActiveSchemaTest.class);

    private static final String ZOOKEEPER_CHECKPOINT_PATH = "/replicator/checkpoint";

    private static final String CHECKPOINT_DEFAULT = "{\"timestamp\": 0, \"serverId\": 1, \"gtid\": null, \"binlog\": {\"filename\": \"binlog.000001\", \"position\": 4}}";

    private static final String MYSQL_SCHEMA = "replicator";
    private static final String MYSQL_ROOT_USERNAME = "root";
    private static final String MYSQL_USERNAME = "replicator";
    private static final String MYSQL_PASSWORD = "replicator";
    private static final String MYSQL_ACTIVE_SCHEMA = "active_schema";
    private static final String MYSQL_TEST_SCRIPT = "activeschema.test.sql";
    private static final String MYSQL_CONF_FILE = "my.cnf";
    private static final String REPLICATOR_WHITELIST = "test1";
    private static final int TRANSACTION_LIMIT = 1000;
    private static final String CONNECTION_URL_FORMAT = "jdbc:mysql://%s:%d/%s";

    private static ServicesControl mysqlBinaryLog;

    @BeforeClass
    public static void before() throws InterruptedException {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        MySQLConfiguration mySQLConfiguration = new MySQLConfiguration(
                ActiveSchemaTest.MYSQL_SCHEMA,
                ActiveSchemaTest.MYSQL_USERNAME,
                ActiveSchemaTest.MYSQL_PASSWORD,
                ActiveSchemaTest.MYSQL_CONF_FILE,
                Collections.emptyList(),
                null,
                null
        );


        ActiveSchemaTest.mysqlBinaryLog = servicesProvider.startMySQL(mySQLConfiguration);

        try {
            Thread.sleep(15000L);
        }catch(Exception e){ throw new RuntimeException(e); }
    }

    @Test
    public void testReplicator() throws Exception {
        Replicator replicator = new Replicator(this.getConfiguration());

        replicator.start();

        File file = new File("src/test/resources/" + ActiveSchemaTest.MYSQL_TEST_SCRIPT);

        runMysqlScripts(this.getConfiguration(), file.getAbsolutePath());

        replicator.wait(1L, TimeUnit.MINUTES);

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
            ActiveSchemaTest.LOG.info("Executing query from " + scriptFilePath);
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
                    ActiveSchemaTest.LOG.info(query);
                }
            }
            return true;
        } catch (Exception exception) {
            ActiveSchemaTest.LOG.warn(String.format("error executing query \"%s\": %s", scriptFilePath, exception.getMessage()));
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

        configuration.put(WebServer.Configuration.TYPE, WebServer.ServerType.NONE.name());

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(ActiveSchemaTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(ActiveSchemaTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, ActiveSchemaTest.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, ActiveSchemaTest.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, ActiveSchemaTest.MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, ActiveSchemaTest.mysqlBinaryLog.getHost());
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(ActiveSchemaTest.mysqlBinaryLog.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_ACTIVE_SCHEMA, ActiveSchemaTest.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, ActiveSchemaTest.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, ActiveSchemaTest.MYSQL_PASSWORD);

        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(ActiveSchemaTest.TRANSACTION_LIMIT));
        configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true);
        configuration.put(AugmenterContext.Configuration.INCLUDE_TABLE, ActiveSchemaTest.REPLICATOR_WHITELIST);


        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.FILE.name());

        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());
        configuration.put(BinaryLogSupplier.Configuration.POSITION_TYPE, BinaryLogSupplier.PositionType.BINLOG);

        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.NONE.name());

        configuration.put(Partitioner.Configuration.TYPE, Partitioner.Type.TABLE_NAME.name());

        configuration.put(Applier.Configuration.TYPE, Applier.Type.CONSOLE.name());
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.NONE.name());
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, ActiveSchemaTest.ZOOKEEPER_CHECKPOINT_PATH);
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, ActiveSchemaTest.CHECKPOINT_DEFAULT);

        configuration.put(Replicator.Configuration.REPLICATOR_THREADS, "1");
        configuration.put(Replicator.Configuration.REPLICATOR_TASKS, "1");

        return configuration;
    }

    @AfterClass
    public static void after() {
        ActiveSchemaTest.mysqlBinaryLog.close();
    }

}