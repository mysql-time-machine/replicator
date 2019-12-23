package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.DataType;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.schema.TableSchema;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

public class ActiveSchemaHelpers {

    private static final Logger LOG = LogManager.getLogger(ActiveSchemaHelpers.class);

    public static TableSchema computeTableSchema(
            String schemaName,
            String tableName,
            BasicDataSource activeSchemaDataSource,
            BasicDataSource replicantDataSource,
            Boolean fallbackToReplicant) {

        try (Connection activeSchemaConnection = activeSchemaDataSource.getConnection()) {

            Statement statementActiveSchemaListColumns      = activeSchemaConnection.createStatement();
            Statement statementActiveSchemaShowCreateTable  = activeSchemaConnection.createStatement();
            Boolean fallbackUsed = false;

            List<ColumnSchema> columnList = new ArrayList<>();

            ResultSet resultSet;

            resultSet = statementActiveSchemaListColumns.executeQuery(
                 String.format(
                         ActiveSchemaManager.LIST_COLUMNS_SQL,
                         schemaName,
                         tableName
                 )
            );

            Statement statementReplicantListColumns     = null;
            Statement statementReplicantShowCreateTable = null;
            Connection replicantConnection = null;

            if ( !resultSet.next() ) {
                if ( fallbackToReplicant ) {
                    try {
                        resultSet.close();
                        replicantConnection = replicantDataSource.getConnection();
                        statementReplicantListColumns = replicantConnection.createStatement();
                        statementReplicantShowCreateTable = replicantConnection.createStatement();

                        resultSet = statementReplicantListColumns.executeQuery( String.format(ActiveSchemaManager.LIST_COLUMNS_SQL, schemaName, tableName) );
                        fallbackUsed = true;
                    } catch (Exception e) {
                        throw new RuntimeException("Error initiating connection to replicantDataSource: " + e.getMessage());
                    }
                } else {
                    throw new RuntimeException("Unable to retrieve column list from ActiveSchema for table " + tableName );
                }
            } else {
                resultSet.previous();
            }

            while (resultSet.next()) {

                boolean isNullable = (resultSet.getString("IS_NULLABLE").equals("NO") ? false : true);

                DataType dataType = DataType.byCode(resultSet.getString("DATA_TYPE"));

                ColumnSchema columnSchema = new ColumnSchema(
                        resultSet.getString("COLUMN_NAME"),
                        dataType,
                        resultSet.getString("COLUMN_TYPE"),
                        isNullable,
                        resultSet.getString("COLUMN_KEY"),
                        resultSet.getString("EXTRA")
                );

                columnSchema
                        .setCollation(resultSet.getString("COLLATION_NAME"))
                        .setDefaultValue(resultSet.getString("COLUMN_DEFAULT"))
                        .setDateTimePrecision(resultSet.getInt("DATETIME_PRECISION"))
                        .setCharMaxLength(resultSet.getInt("CHARACTER_MAXIMUM_LENGTH"))
                        .setCharOctetLength(resultSet.getInt("CHARACTER_OCTET_LENGTH"))
                        .setNumericPrecision(resultSet.getInt("NUMERIC_PRECISION"))
                        .setNumericScale(resultSet.getInt("NUMERIC_SCALE"));

                columnList.add(columnSchema);
            }

            DatabaseMetaData dbm = fallbackUsed ? replicantConnection.getMetaData() : activeSchemaConnection.getMetaData();
            boolean tableExists = false;
            ResultSet tables = dbm.getTables(schemaName, null, tableName, null);
            if (tables.next()) {
                tableExists = true; // DLL statement was not table DROP
            }

            String tableCreateStatement = "";
            if (tableExists) {
                Statement createTableStatement = fallbackUsed ? statementReplicantShowCreateTable : statementActiveSchemaShowCreateTable;
                ResultSet showCreateTableResultSet = createTableStatement.executeQuery(
                        String.format(ActiveSchemaManager.SHOW_CREATE_TABLE_SQL, tableName)
                );
                ResultSetMetaData showCreateTableResultSetMetadata = showCreateTableResultSet.getMetaData();
                tableCreateStatement = ActiveSchemaHelpers.getCreateTableStatement(tableName, showCreateTableResultSet, showCreateTableResultSetMetadata);
            }

            return new TableSchema(
                    new FullTableName(schemaName, tableName),
                    columnList,
                    tableCreateStatement
            );

        } catch (SQLException exception) {
            throw new IllegalStateException("Could not get table schema: ", exception);
        }
    }


    public static String getCreateTableStatement(String tableName, ResultSet showCreateTableResultSet, ResultSetMetaData showCreateTableResultSetMetadata) throws SQLException {
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

    /**
     * Mangle name of the active schema before applying DDL statements.
     *
     * @param query             Query string
     * @param replicantDbName   Database name
     * @return                  Rewritten query
     */
    public static String rewriteActiveSchemaName(String query, String replicantDbName) {

        LOG.info("Rewriting active schema name");

        String dbNamePattern =
                "( " + replicantDbName + "\\.)" +
                        "|" +
                        "( `" + replicantDbName + "`\\.)";
        String rewritenQuery = query.replaceAll(dbNamePattern, " ");

        LOG.info("Rewritten => { in => " + query + ", out => " + rewritenQuery + " }");

        return rewritenQuery;
    }

}
