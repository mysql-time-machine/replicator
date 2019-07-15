package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.DataType;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.schema.TableSchema;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

public class SchemaHelpers {

    public static TableSchema computeTableSchema(String schema, String tableName, BasicDataSource dataSource, DataSource binlogDataSource) {

        try (Connection connection = dataSource.getConnection()) {
            Statement statementListColumns      = connection.createStatement();
            Statement statementShowCreateTable  = connection.createStatement();

            //  connection.getSchema() returns null for MySQL, so we do this ugly hack
            // TODO: find nicer way
            String[] terms = connection.getMetaData().getURL().split("/");
            String schemaName = terms[terms.length - 1];

            List<ColumnSchema> columnList = new ArrayList<>();
            String tableCreateStatement;

            ResultSet resultSet;
            SchemaHelpers.createTableIfNotExists(tableName, connection, binlogDataSource);

            resultSet = statementListColumns.executeQuery(
                    String.format(ActiveSchemaManager.LIST_COLUMNS_SQL, schema, tableName)
            );

            while (resultSet.next()) {

                String collation = resultSet.getString("COLLATION_NAME");

                boolean nullable = (resultSet.getString("IS_NULLABLE").equals("NO") ? false : true);

                DataType dataType = DataType.byCode(resultSet.getString("DATA_TYPE"));

                columnList.add(new ColumnSchema(
                        resultSet.getString("COLUMN_NAME"),
                        dataType,
                        resultSet.getString("COLUMN_TYPE"),
                        collation,
                        nullable,
                        resultSet.getString("COLUMN_KEY"),
                        resultSet.getString("COLUMN_DEFAULT"),
                        resultSet.getString("EXTRA")
                ));
            }

            ResultSet showCreateTableResultSet = statementShowCreateTable.executeQuery(
                    String.format(ActiveSchemaManager.SHOW_CREATE_TABLE_SQL, tableName)
            );
            ResultSetMetaData showCreateTableResultSetMetadata = showCreateTableResultSet.getMetaData();
            tableCreateStatement = SchemaHelpers.getCreateTableStatement(tableName, showCreateTableResultSet, showCreateTableResultSetMetadata);


            return new TableSchema(new FullTableName(schemaName, tableName),
                    columnList,
                    tableCreateStatement);

        } catch (SQLException exception) {
            throw new IllegalStateException("Could not get table schema: ", exception);
        }
    }

    private static void createTableIfNotExists(String tableName, Connection connection, DataSource binlogDataSource) throws SQLException {
        PreparedStatement stmtShowTables = connection.prepareStatement("show tables like ?");
        stmtShowTables.setString(1, tableName);
        ResultSet resultSet = stmtShowTables.executeQuery();
        if (resultSet.next()) {
            return;
        } else {
            //get from orignal table
            try (Connection binlogDbConn = binlogDataSource.getConnection()) {
                PreparedStatement preparedStatement = binlogDbConn.prepareStatement("show create table " + tableName);
                ResultSet showCreateTableResultSet = preparedStatement.executeQuery();
                ResultSetMetaData showCreateTableResultSetMetadata = showCreateTableResultSet.getMetaData();
                String createTableStatement = SchemaHelpers.getCreateTableStatement(tableName, showCreateTableResultSet, showCreateTableResultSetMetadata);
                boolean executed = connection.createStatement().execute(createTableStatement);
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

    ;
}
