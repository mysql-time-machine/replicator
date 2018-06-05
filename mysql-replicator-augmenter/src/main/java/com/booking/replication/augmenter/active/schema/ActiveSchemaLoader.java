package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventColumn;
import com.booking.replication.augmenter.model.AugmentedEventTable;
import com.mysql.jdbc.Driver;
import org.apache.commons.dbcp2.BasicDataSource;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ActiveSchemaLoader implements Closeable {
    public interface Configuration {
        String MYSQL_DRIVER_CLASS = "augmenter.active.schema.mysql.driver.class";
        String MYSQL_HOSTNAME = "augmenter.active.schema.mysql.hostname";
        String MYSQL_PORT = "augmenter.active.schema.mysql.port";
        String MYSQL_SCHEMA = "augmenter.active.schema.mysql.schema";
        String MYSQL_USERNAME = "augmenter.active.schema.mysql.username";
        String MYSQL_PASSWORD = "augmenter.active.schema.mysql.password";
    }

    private static final String DEFAULT_MYSQL_DRIVER_CLASS = Driver.class.getName();

    private static final String CONNECTION_URL_FORMAT = "jdbc:mysql://%s:%d/%s";
    private static final String LIST_TABLES_SQL = "SHOW TABLES";
    private static final String LIST_COLUMNS_SQL = "DESC ?";
    private static final String SHOW_CREATE_TABLE_SQL = "SHOW CREATE TABLE ?";

    private final String schema;
    private final BasicDataSource dataSource;

    public ActiveSchemaLoader(Map<String, String> configuration) {
        String driverClass = configuration.getOrDefault(Configuration.MYSQL_DRIVER_CLASS, ActiveSchemaLoader.DEFAULT_MYSQL_DRIVER_CLASS);
        String hostname = configuration.get(Configuration.MYSQL_HOSTNAME);
        String port = configuration.getOrDefault(Configuration.MYSQL_PORT, "3306");
        String schema = configuration.get(Configuration.MYSQL_SCHEMA);
        String username = configuration.get(Configuration.MYSQL_USERNAME);
        String password = configuration.get(Configuration.MYSQL_PASSWORD);

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", Configuration.MYSQL_PASSWORD));

        this.schema = schema;
        this.dataSource = this.getDataSource(driverClass, hostname, Integer.parseInt(port), schema, username, password);
    }

    private BasicDataSource getDataSource(String driverClass, String hostname, int port, String schema, String username, String password) {
        BasicDataSource dataSource = new BasicDataSource();

        dataSource.setDriverClassName(driverClass);
        dataSource.setUrl(String.format(ActiveSchemaLoader.CONNECTION_URL_FORMAT, hostname, port, schema));
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    public boolean execute(String query) {
        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            return statement.execute(query);
        } catch (SQLException exception) {
            throw new RuntimeException("error executing query", exception);
        }
    }

    public List<AugmentedEventTable> listTables() {
        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(ActiveSchemaLoader.LIST_TABLES_SQL)) {
            List<AugmentedEventTable> tableList = new ArrayList<>();

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    tableList.add(new AugmentedEventTable(
                            this.schema,
                            resultSet.getString(0)
                    ));
                }
            }

            return tableList;
        } catch (SQLException exception) {
            throw new RuntimeException("error listing tables", exception);
        }
    }

    public List<AugmentedEventColumn> listColumns(String tableName) {
        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(ActiveSchemaLoader.LIST_COLUMNS_SQL)) {
            List<AugmentedEventColumn> columnList = new ArrayList<>();

            preparedStatement.setString(1, tableName);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
            throw new RuntimeException("error listing columns", exception);
        }
    }

    public String getCreateTable(String tableName) {
        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(ActiveSchemaLoader.SHOW_CREATE_TABLE_SQL)) {
            preparedStatement.setString(1, tableName);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                return resultSet.getString(2);
            }
        } catch (SQLException exception) {
            throw new RuntimeException("error getting create table", exception);
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
