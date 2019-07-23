package com.booking.replication.it;

import com.booking.replication.Replicator;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.ReplicatorPartitioner;
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
import com.booking.replication.coordinator.ZookeeperCoordinator;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;

import com.mysql.jdbc.Driver;

import org.apache.commons.dbcp2.BasicDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.sql.Connection;

import java.sql.Statement;
import java.util.*;

public class ReplicatorConsoleTest {

    private static final Logger LOG = LogManager.getLogger(ReplicatorConsoleTest.class);

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
    private static final int TRANSACTION_LIMIT = 1000;
    private static final String CONNECTION_URL_FORMAT = "jdbc:mysql://%s:%d/%s";

    private static ServicesControl zookeeper;
    private static ServicesControl mysqlBinaryLog;
    private static ServicesControl mysqlActiveSchema;

    @BeforeClass
    public static void before() {
        ServicesProvider servicesProvider = ServicesProvider.build(ServicesProvider.Type.CONTAINERS);

        ReplicatorConsoleTest.zookeeper = servicesProvider.startZookeeper();

        MySQLConfiguration mySQLConfiguration = new MySQLConfiguration(
                ReplicatorConsoleTest.MYSQL_SCHEMA,
                ReplicatorConsoleTest.MYSQL_USERNAME,
                ReplicatorConsoleTest.MYSQL_PASSWORD,
                ReplicatorConsoleTest.MYSQL_CONF_FILE,
                Collections.singletonList(ReplicatorConsoleTest.MYSQL_INIT_SCRIPT),
                null,
                null
        );

        MySQLConfiguration mySQLActiveSchemaConfiguration = new MySQLConfiguration(
                ReplicatorConsoleTest.MYSQL_ACTIVE_SCHEMA,
                ReplicatorConsoleTest.MYSQL_USERNAME,
                ReplicatorConsoleTest.MYSQL_PASSWORD,
                ReplicatorConsoleTest.MYSQL_CONF_FILE,
                Collections.emptyList(),
                null,
                null
        );

        ReplicatorConsoleTest.mysqlBinaryLog = servicesProvider.startMySQL(mySQLConfiguration);
        ReplicatorConsoleTest.mysqlActiveSchema = servicesProvider.startMySQL(mySQLActiveSchemaConfiguration);

        try {
            // give some time for containers to start
            // TODO: make this nicer with a proper wait
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReplicator() throws Exception {

        Replicator replicator = new Replicator(this.getConfiguration());

        replicator.start();

        File file = new File("src/test/resources/" + ReplicatorConsoleTest.MYSQL_TEST_SCRIPT);

        runMysqlScripts(this.getConfiguration(), file.getAbsolutePath());

        Thread.sleep(1000000);

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
            ReplicatorConsoleTest.LOG.info("Executing query from " + scriptFilePath);
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
                    ReplicatorConsoleTest.LOG.info(query);
                }
            }
            return true;
        } catch (Exception exception) {
            ReplicatorConsoleTest.LOG.warn(String.format("error executing query \"%s\": %s", scriptFilePath, exception.getMessage()));
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

        configuration.put(ZookeeperCoordinator.Configuration.CONNECTION_STRING, ReplicatorConsoleTest.zookeeper.getURL());
        configuration.put(ZookeeperCoordinator.Configuration.LEADERSHIP_PATH, ReplicatorConsoleTest.ZOOKEEPER_LEADERSHIP_PATH);

        configuration.put(WebServer.Configuration.TYPE, WebServer.ServerType.JETTY.name());

        configuration.put(BinaryLogSupplier.Configuration.MYSQL_HOSTNAME, Collections.singletonList(ReplicatorConsoleTest.mysqlBinaryLog.getHost()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PORT, String.valueOf(ReplicatorConsoleTest.mysqlBinaryLog.getPort()));
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_SCHEMA, ReplicatorConsoleTest.MYSQL_SCHEMA);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_USERNAME, ReplicatorConsoleTest.MYSQL_ROOT_USERNAME);
        configuration.put(BinaryLogSupplier.Configuration.MYSQL_PASSWORD, ReplicatorConsoleTest.MYSQL_PASSWORD);

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_HOSTNAME, ReplicatorConsoleTest.mysqlActiveSchema.getHost());

        System.out.println("PORT => " + ReplicatorConsoleTest.mysqlActiveSchema.getPort());

        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PORT, String.valueOf(ReplicatorConsoleTest.mysqlActiveSchema.getPort()));
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_SCHEMA, ReplicatorConsoleTest.MYSQL_ACTIVE_SCHEMA);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_USERNAME, ReplicatorConsoleTest.MYSQL_ROOT_USERNAME);
        configuration.put(ActiveSchemaManager.Configuration.MYSQL_PASSWORD, ReplicatorConsoleTest.MYSQL_PASSWORD);

        configuration.put(AugmenterContext.Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(ReplicatorConsoleTest.TRANSACTION_LIMIT));
        configuration.put(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, true);

        configuration.put(Coordinator.Configuration.TYPE, Coordinator.Type.ZOOKEEPER.name());

        configuration.put(Supplier.Configuration.TYPE, Supplier.Type.BINLOG.name());

        configuration.put(BinaryLogSupplier.Configuration.POSITION_TYPE, BinaryLogSupplier.PositionType.BINLOG);

        configuration.put(Augmenter.Configuration.SCHEMA_TYPE, Augmenter.SchemaType.ACTIVE.name());
        configuration.put(Seeker.Configuration.TYPE, Seeker.Type.NONE.name());

        configuration.put(ReplicatorPartitioner.Configuration.TYPE, ReplicatorPartitioner.Type.TABLE_NAME.name());

        configuration.put(Applier.Configuration.TYPE, Applier.Type.CONSOLE);
        configuration.put(CheckpointApplier.Configuration.TYPE, CheckpointApplier.Type.COORDINATOR.name());
        configuration.put(Replicator.Configuration.CHECKPOINT_PATH, ReplicatorConsoleTest.ZOOKEEPER_CHECKPOINT_PATH);
        configuration.put(Replicator.Configuration.CHECKPOINT_DEFAULT, ReplicatorConsoleTest.CHECKPOINT_DEFAULT);

        return configuration;
    }

    @AfterClass
    public static void after() {
        ReplicatorConsoleTest.mysqlBinaryLog.close();
        ReplicatorConsoleTest.mysqlActiveSchema.close();
        ReplicatorConsoleTest.zookeeper.close();
    }
}
