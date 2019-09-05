package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.schema.TableSchema;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SchemaHelpers {

    public static TableSchema computeTableSchema(String tableName, BasicDataSource dataSource, DataSource binlogDataSource) {

        try (Connection connection = dataSource.getConnection()) {
            try (Statement statementListColumns = connection.createStatement();
                 Statement statementShowCreateTable = connection.createStatement()) {

                //  connection.getSchema() returns null for MySQL, so we do this ugly hack
                // TODO: find nicer way
                String[] terms = connection.getMetaData().getURL().split("/");
                String schemaName = terms[terms.length - 1];

                List<ColumnSchema> columnList = new ArrayList<>();
                String tableCreateStatement;

                // TODO: get this info from information_schema instead of 'show full columns from'
                //       to avoid dealing with columns indexes
                ResultSet resultSet;
                SchemaHelpers.createTableIfNotExists(tableName, connection, binlogDataSource);

                resultSet = statementListColumns.executeQuery(
                    String.format(ActiveSchemaManager.LIST_COLUMNS_SQL, tableName)
                );
                while (resultSet.next()) {

                    String collation = resultSet.getString(3);

                    boolean nullable = (!resultSet.getString(4).equals("NO"));

                    columnList.add(new ColumnSchema(
                        resultSet.getString(1),
                        resultSet.getString(2),
                        collation,
                        nullable,
                        resultSet.getString(5),
                        resultSet.getString(6),
                        resultSet.getString(7)
                    ));
                }

                ResultSet showCreateTableResultSet = statementShowCreateTable.executeQuery(
                    String.format(ActiveSchemaManager.SHOW_CREATE_TABLE_SQL, tableName)
                );
                ResultSetMetaData showCreateTableResultSetMetadata = showCreateTableResultSet.getMetaData();
                tableCreateStatement = SchemaHelpers.getCreateTableStatement(tableName, showCreateTableResultSet, showCreateTableResultSetMetadata);

                return new TableSchema(new FullTableName(schemaName, tableName), columnList, tableCreateStatement);
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Could not get table schema: ", exception);
        }
    }

    private static void createTableIfNotExists(String tableName, Connection connection, DataSource binlogDataSource) throws SQLException {
        try (PreparedStatement stmtShowTables = connection.prepareStatement("show tables like ?")) {
            stmtShowTables.setString(1, tableName);
            ResultSet resultSet = stmtShowTables.executeQuery();
            if (resultSet.next()) {
                return;
            } else {
                //get from orignal table
                try (Connection binlogDbConn = binlogDataSource.getConnection()) {
                    try (PreparedStatement preparedStatement = binlogDbConn.prepareStatement("show create table " + tableName);
                         ResultSet showCreateTableResultSet = preparedStatement.executeQuery()) {
                        ResultSetMetaData showCreateTableResultSetMetadata = showCreateTableResultSet.getMetaData();
                        String createTableStatement = SchemaHelpers.getCreateTableStatement(tableName, showCreateTableResultSet, showCreateTableResultSetMetadata);
                        try (Statement statement = connection.createStatement()) {
                            statement.execute(createTableStatement);
                        }
                    }
                }
            }
        }
    }

    private static String getCreateTableStatement(String tableName, ResultSet showCreateTableResultSet, ResultSetMetaData showCreateTableResultSetMetadata) throws SQLException {
        String tableCreateStatement = null;
        while (showCreateTableResultSet.next()) {
            if (showCreateTableResultSetMetadata.getColumnCount() != 2) {
                throw new SQLException("SHOW CREATE TABLE should return 2 columns.");
            }
            String returnedTableName = showCreateTableResultSet.getString(1);
            if (!returnedTableName.equalsIgnoreCase(tableName)) {
                throw new SQLException("We asked for '" + tableName + "' and got '" + returnedTableName + "'");
            }
            tableCreateStatement = showCreateTableResultSet.getString(2);
        }
        return tableCreateStatement;
    }

}
