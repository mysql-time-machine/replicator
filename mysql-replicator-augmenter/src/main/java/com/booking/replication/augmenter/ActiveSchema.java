package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.AugmentedEventColumn;
import com.mysql.jdbc.Driver;
import org.apache.commons.dbcp2.BasicDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveSchema implements Schema {
    public interface Configuration {
        String MYSQL_DRIVER_CLASS = "augmenter.schema.active.mysql.driver.class";
        String MYSQL_HOSTNAME = "augmenter.schema.active.mysql.hostname";
        String MYSQL_PORT = "augmenter.schema.active.mysql.port";
        String MYSQL_SCHEMA = "augmenter.schema.active.mysql.schema";
        String MYSQL_USERNAME = "augmenter.schema.active.mysql.username";
        String MYSQL_PASSWORD = "augmenter.schema.active.mysql.password";
    }

    private static final Logger LOG = Logger.getLogger(ActiveSchema.class.getName());

    private static final String DEFAULT_MYSQL_DRIVER_CLASS = Driver.class.getName();

    private static final String CONNECTION_URL_FORMAT = "jdbc:mysql://%s:%d/%s";
    private static final String LIST_COLUMNS_SQL = "DESC %s";
    private static final String SHOW_CREATE_TABLE_SQL = "SHOW CREATE TABLE %s";

    private final BasicDataSource dataSource;
    private final Map<String, List<AugmentedEventColumn>> cache;

    public ActiveSchema(Map<String, Object> configuration) {
        Object driverClass = configuration.getOrDefault(Configuration.MYSQL_DRIVER_CLASS, ActiveSchema.DEFAULT_MYSQL_DRIVER_CLASS);
        Object hostname = configuration.get(Configuration.MYSQL_HOSTNAME);
        Object port = configuration.getOrDefault(Configuration.MYSQL_PORT, "3306");
        Object schema = configuration.get(Configuration.MYSQL_SCHEMA);
        Object username = configuration.get(Configuration.MYSQL_USERNAME);
        Object password = configuration.get(Configuration.MYSQL_PASSWORD);

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", Configuration.MYSQL_PASSWORD));

        this.dataSource = this.getDataSource(driverClass.toString(), hostname.toString(), Integer.parseInt(port.toString()), schema.toString(), username.toString(), password.toString());
        this.cache = new ConcurrentHashMap<>();
    }

    private BasicDataSource getDataSource(String driverClass, String hostname, int port, String schema, String username, String password) {
        BasicDataSource dataSource = new BasicDataSource();

        dataSource.setDriverClassName(driverClass);
        dataSource.setUrl(String.format(ActiveSchema.CONNECTION_URL_FORMAT, hostname, port, schema));
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    @Override
    public boolean execute(String tableName, String query) {
        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            if (tableName != null) {
                this.cache.remove(tableName);
            }

            return statement.execute(query);
        } catch (SQLException exception) {
            ActiveSchema.LOG.log(Level.WARNING, String.format("error executing query \"%s\": %s", query, exception.getMessage()));
            return false;
        }
    }

    @Override
    public List<AugmentedEventColumn> listColumns(String tableName) {
        return this.cache.computeIfAbsent(tableName, key -> {
            try (Connection connection = this.dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                 List<AugmentedEventColumn> columnList = new ArrayList<>();

                 try (ResultSet resultSet = statement.executeQuery(String.format(ActiveSchema.LIST_COLUMNS_SQL, tableName))) {
                    while (resultSet.next()) {
                        columnList.add(new AugmentedEventColumn(
                                resultSet.getString(1),
                                resultSet.getString(2),
                                resultSet.getBoolean(3),
                                resultSet.getString(4),
                                resultSet.getString(5),
                                resultSet.getString(6)
                        ));
                    }
                 }

                return columnList;
            } catch (SQLException exception) {
                ActiveSchema.LOG.log(Level.WARNING, String.format("error listing columns from table \"%s\": %s", tableName, exception.getMessage()));
                return null;
            }
        });
    }

    @Override
    public String getCreateTable(String tableName) {
        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(String.format(ActiveSchema.SHOW_CREATE_TABLE_SQL, tableName))) {
            if (resultSet.next()) {
                return resultSet.getString(2);
            } else  {
                return null;
            }
        } catch (SQLException exception) {
            ActiveSchema.LOG.log(Level.WARNING, String.format("error getting create table from table \"%s\"", tableName, exception.getMessage()));
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            this.dataSource.close();
        } catch (SQLException exception) {
            throw new IOException("error closing active schema loader", exception);
        }
    }
}
