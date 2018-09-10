package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.TableSchema;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SchemaHelpers {

    public static Function<String, TableSchema> fnComputeTableSchema;

    public static BiFunction<String, DataSource, Optional<TableSchema>> computeTableSchema =

            (tableName, dataSource) ->  {

                try (Connection connection = dataSource.getConnection();
                     Statement statementListColumns = connection.createStatement();
                     Statement statementShowCreateTable = connection.createStatement()) {

                    List<ColumnSchema> columnList = new ArrayList<>();
                    String tableCreateStatement = null;

                    // TODO: get this info from information_schema instead of 'show full columns from'
                    //       to avoid dealing with columns indexes
                    try (ResultSet resultSet = statementListColumns.executeQuery(
                            String.format(ActiveSchemaManager.LIST_COLUMNS_SQL, tableName)
                    )) {
                        while (resultSet.next()) {

                            String collation = resultSet.getString(3);

                            boolean nullable = (resultSet.getString(4).equals("NO") ? false : true);

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
                    }

                    try(ResultSet showCreateTableResultSet = statementShowCreateTable.executeQuery(
                            String.format(ActiveSchemaManager.SHOW_CREATE_TABLE_SQL, tableName)
                    )) {
                        ResultSetMetaData showCreateTableResultSetMetadata = showCreateTableResultSet.getMetaData();

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
                    }

                    return Optional.of(new TableSchema(columnList, tableCreateStatement));

                } catch (SQLException exception) {
                    return Optional.empty(); // TODO: replace Optional with Either
                }
            };
}
