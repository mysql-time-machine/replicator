package com.booking.replication.schema;

import com.booking.replication.Configuration;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.column.types.EnumColumnSchema;
import com.booking.replication.schema.column.types.SetColumnSchema;
import com.booking.replication.schema.table.TableSchema;
import com.booking.replication.util.JsonBuilder;

import org.apache.commons.dbcp2.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * ActiveSchemaVersion refers to the schema that corresponds
 * to the current position in the binlog.
 * This class abstracts work with database in which
 * this data is maintained.
 */
public class ActiveSchemaVersion {

    private static final String SHOW_TABLES_SQL        = "SHOW TABLES";
    private static final String SHOW_CREATE_TABLE_SQL  = "SHOW CREATE TABLE ";
    private static final String INFORMATION_SCHEMA_SQL =
            "SELECT * FROM `information_schema`.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

    private final HashMap<String,String> activeSchemaCreateStatements = new HashMap<>();
    private final HashMap<String,TableSchema> activeSchemaTables      = new HashMap<>();

    private final Configuration configuration;

    // TODO: refactor this so that datasource is passed in constructor
    //       so that same pool is shared for different objects
    private final BasicDataSource activeSchemaDataSource;
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveSchemaVersion.class);

    public ActiveSchemaVersion(Configuration replicatorConfiguration) throws URISyntaxException, SQLException {

        activeSchemaDataSource = new BasicDataSource();

        activeSchemaDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        activeSchemaDataSource.setUrl(replicatorConfiguration.getActiveSchemaDSN());

        activeSchemaDataSource.addConnectionProperty("useUnicode", "true");
        activeSchemaDataSource.addConnectionProperty("characterEncoding", "UTF-8");
        activeSchemaDataSource.setUsername(replicatorConfiguration.getActiveSchemaUserName());
        activeSchemaDataSource.setPassword(replicatorConfiguration.getActiveSchemaPassword());

        configuration = replicatorConfiguration;

        loadActiveSchema();

        LOGGER.info("Successfully loaded ActiveSchemaVersion");
    }

    public void loadActiveSchema() throws SQLException {
        Connection con = null;

        try {
            con = activeSchemaDataSource.getConnection();

            // 1. Get list of tables in active schema
            Statement         showTablesStatement         = con.createStatement();
            ResultSet         showTablesResultSet         = showTablesStatement.executeQuery(SHOW_TABLES_SQL);
            ResultSetMetaData showTablesResultSetMetaData = showTablesResultSet.getMetaData();

            List<String> tableNames = new ArrayList<>();
            while (showTablesResultSet.next()) {
                int columnCount = showTablesResultSetMetaData.getColumnCount();
                if (columnCount != 1) {
                    throw new SQLException("SHOW TABLES result set should have only one column!");
                }
                String tableName = showTablesResultSet.getString(1);
                tableNames.add(tableName);
            }
            showTablesResultSet.close();
            showTablesStatement.close();

            // 2. For each table:
            //       a. getValue and cache its create statement
            //       b. create and initialize TableSchema object
            //       b. create and initialize TableSchema object
            for (String tableName : tableNames) {

                // a. getValue and cache table's create statement
                PreparedStatement showCreateTableStatement         = con.prepareStatement(SHOW_CREATE_TABLE_SQL + tableName);
                ResultSet         showCreateTableResultSet         = showCreateTableStatement.executeQuery();
                ResultSetMetaData showCreateTableResultSetMetadata = showCreateTableResultSet.getMetaData();

                while (showCreateTableResultSet.next()) {

                    if (showCreateTableResultSetMetadata.getColumnCount() != 2) {
                        throw new SQLException("SHOW CREATE TABLE should return 2 columns.");
                    }

                    String returnedTableName = showCreateTableResultSet.getString(1);
                    if (!returnedTableName.equalsIgnoreCase(tableName)) {
                        throw new SQLException("We asked for '" + tableName + "' and got '" + returnedTableName + "'");
                    }
                    String returnedCreateStatement = showCreateTableResultSet.getString(2);

                    this.activeSchemaCreateStatements.put(tableName,returnedCreateStatement);
                }
                showCreateTableResultSet.close();
                showCreateTableStatement.close();

                // b. create and initialize TableSchema object
                this.activeSchemaTables.put(tableName, new TableSchema());

                PreparedStatement getTableInfoStatement =
                        con.prepareStatement(INFORMATION_SCHEMA_SQL);
                getTableInfoStatement.setString(1, this.configuration.getActiveSchemaDB());
                getTableInfoStatement.setString(2, tableName);

                ResultSet getTableInfoResultSet = getTableInfoStatement.executeQuery();

                while (getTableInfoResultSet.next()) {

                    ColumnSchema columnSchema;

                    if (getTableInfoResultSet.getString("DATA_TYPE").equals("enum")) {
                        columnSchema = new EnumColumnSchema(getTableInfoResultSet);
                    } else if (getTableInfoResultSet.getString("DATA_TYPE").equals("set")) {
                        columnSchema = new SetColumnSchema(getTableInfoResultSet);
                    } else {
                        columnSchema = new ColumnSchema(getTableInfoResultSet);
                    }

                    this.activeSchemaTables.get(tableName).addColumn(columnSchema);
                }
                getTableInfoResultSet.close();
                getTableInfoStatement.close();
            }
            con.close();
        } finally {
            try {
                // 3. release connection
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public String schemaTablesToJson() {
        return  JsonBuilder.schemaVersionTablesToJson(activeSchemaTables);
    }

    public String schemaCreateStatementsToJson() {
        return  JsonBuilder.schemaCreateStatementsToJson(activeSchemaCreateStatements);
    }

    public String toJson() {
        return JsonBuilder.schemaVersionToJson(this);
    }

    public HashMap<String, TableSchema> getActiveSchemaTables() {
        return activeSchemaTables;
    }

    public HashMap<String, String> getActiveSchemaCreateStatements() {
        return activeSchemaCreateStatements;
    }

    /**
     * applyDDL
     *
     * Changes the active schema by executing ddl on active schema db
     * and then reloading the activeSchema objects
     *
     * @param sequence Sequence of DDL statements for schema transition
     * @return ActiveSchemaVersion
     */
    public ActiveSchemaVersion applyDDL(HashMap<String,String> sequence) {

        LOGGER.info("GOT DDL => " + sequence.get("ddl"));

        Connection con = null;

        try {
            // applyAugmentedRowsEvent DDL
            con = activeSchemaDataSource.getConnection();

            if (sequence.containsKey("timezonePre")) {
                Statement timezonePre = con.createStatement();
                timezonePre.execute(sequence.get("timezonePre"));
                timezonePre.close();
            }

            Statement ddlStatement = con.createStatement();
            ddlStatement.execute(sequence.get("ddl"));
            ddlStatement.close();

            if (sequence.containsKey("timezonePost")) {
                Statement timezonePost = con.createStatement();
                timezonePost.execute(sequence.get("timezonePost"));
                timezonePost.close();
            }

            con.close();
            LOGGER.info("Successfully altered active schema");

            // load new schema
            this.loadActiveSchema();
            LOGGER.info("Successfully loaded new active schema version");

        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.error("FATAL: failed to execute DDL statement on active schema.", e);
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return this;
    }
}
