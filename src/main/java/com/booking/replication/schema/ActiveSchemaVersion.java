package com.booking.replication.schema;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.sql.*;
import java.util.List;

import com.booking.replication.Configuration;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.column.types.EnumColumnSchema;
import com.booking.replication.schema.column.types.SetColumnSchema;
import com.booking.replication.schema.table.TableSchema;
import com.booking.replication.util.JSONBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.dbcp2.*;

/**
 * ActiveSchemaVersion refers to the schema that corresponds
 * to the current position in the binlog.
 * This class abstracts work with database in which
 * this data is maintained.
 */
public class ActiveSchemaVersion {

    private final String SHOW_TABLES_SQL        = "SHOW TABLES";
    private final String SHOW_CREATE_TABLE_SQL  = "SHOW CREATE TABLE ";
    private final String INFORMATION_SCHEMA_SQL =
            "SELECT * FROM `information_schema`.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

    private final HashMap<String,String> activeSchemaCreateStatements = new HashMap<String,String>();
    private final HashMap<String,TableSchema> activeSchemaTables      = new HashMap<String, TableSchema>();

    private final Configuration configuration;

    // TODO: refactor this so that datasource is passed in constructor
    //       so that same pool is shared for different objects
    private BasicDataSource activeSchemaDataSource;
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveSchemaVersion.class);

    public ActiveSchemaVersion(Configuration replicatorConfiguration) throws SQLException, URISyntaxException {

        activeSchemaDataSource = new BasicDataSource();

        activeSchemaDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        activeSchemaDataSource.setUrl(replicatorConfiguration.getActiveSchemaDSN());

        activeSchemaDataSource.addConnectionProperty("useUnicode", "true");
        activeSchemaDataSource.addConnectionProperty("characterEncoding", "UTF-8");
        activeSchemaDataSource.setUsername(replicatorConfiguration.getActiveSchemaUserName());
        activeSchemaDataSource.setPassword(replicatorConfiguration.getActiveSchemaPassword());

        this.configuration = replicatorConfiguration;

        this.loadActiveSchema();

        LOGGER.info("Successfully loaded ActiveSchemaVersion");
    }

    public void loadActiveSchema()
            throws SQLException, URISyntaxException {

        Connection con = null;

        try {
            con = activeSchemaDataSource.getConnection();

            // 1. Get list of tables in active schema
            Statement         showTables_Statement         = con.createStatement();
            ResultSet         showTables_ResultSet         = showTables_Statement.executeQuery(SHOW_TABLES_SQL);
            ResultSetMetaData showTables_ResultSetMetaData = showTables_ResultSet.getMetaData();

            List<String> tableNames = new ArrayList<String>();
            while(showTables_ResultSet.next()){

                int columnCount = showTables_ResultSetMetaData.getColumnCount();
                if (columnCount != 1) {
                    throw new SQLException("SHOW TABLES result set should have only one column!");
                }
                String tableName = showTables_ResultSet.getString(1);
                tableNames.add(tableName);
            }
            showTables_ResultSet.close();
            showTables_Statement.close();

            // 2. For each table:
            //       a. getValue and cache its create statement
            //       b. create and initialize TableSchema object
            //       b. create and initialize TableSchema object
            for (String tableName : tableNames) {

                // a. getValue and cache table's create statement
                PreparedStatement showCreateTable_Statement         = con.prepareStatement(SHOW_CREATE_TABLE_SQL + tableName);
                ResultSet         showCreateTable_ResultSet         = showCreateTable_Statement.executeQuery();
                ResultSetMetaData showCreateTable_ResultSetMetadata = showCreateTable_ResultSet.getMetaData();

                while (showCreateTable_ResultSet.next()) {

                    int showCreateTable_ResultSet_ColumnCount = showCreateTable_ResultSetMetadata.getColumnCount();
                    if (showCreateTable_ResultSet_ColumnCount != 2) {
                        throw new SQLException("SHOW CREATE TABLE should return 2 columns.");
                    }

                    String returnedTableName = showCreateTable_ResultSet.getString(1);
                    if (!returnedTableName.equalsIgnoreCase(tableName)) {
                        throw new SQLException("We asked for '" + tableName + "' and got '" + returnedTableName + "'");
                    }
                    String returnedCreateStatement = showCreateTable_ResultSet.getString(2);

                    this.activeSchemaCreateStatements.put(tableName,returnedCreateStatement);
                }
                showCreateTable_ResultSet.close();
                showCreateTable_Statement.close();

                // b. create and initialize TableSchema object
                this.activeSchemaTables.put(tableName, new TableSchema());

                PreparedStatement getTableInfo_Statement =
                        con.prepareStatement(INFORMATION_SCHEMA_SQL);
                getTableInfo_Statement.setString(1, this.configuration.getActiveSchemaDB());
                getTableInfo_Statement.setString(2, tableName);

                ResultSet getTableInfo_ResultSet =
                        getTableInfo_Statement.executeQuery();
                ResultSetMetaData getTableInfo_ResultSetMetadata =
                        getTableInfo_ResultSet.getMetaData();

                while (getTableInfo_ResultSet.next()) {

                    ColumnSchema c;

                    if (getTableInfo_ResultSet.getString("DATA_TYPE").equals("enum")) {
                        c = new EnumColumnSchema(getTableInfo_ResultSet);
                    }
                    else if (getTableInfo_ResultSet.getString("DATA_TYPE").equals("set")) {
                        c = new SetColumnSchema(getTableInfo_ResultSet);
                    }
                    else {
                        c = new ColumnSchema(getTableInfo_ResultSet);
                    }

                    this.activeSchemaTables.get(tableName).addColumn(c);
                }
                getTableInfo_ResultSet.close();
                getTableInfo_Statement.close();
            }
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally{
            try {
                // 3. release connection
                if(con != null) con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public String schemaTablesToJSON() {
        return  JSONBuilder.schemaVersionTablesToJSON(activeSchemaTables);
    }

    public String schemaCreateStatementsToJSON() {
        return  JSONBuilder.schemaCreateStatementsToJSON(activeSchemaCreateStatements);
    }

    public String toJSON() {
        return JSONBuilder.schemaVersionToJSON(this);
    }

    public HashMap<String, TableSchema> getActiveSchemaTables() {
        return activeSchemaTables;
    }

    public HashMap<String, String> getActiveSchemaCreateStatements() {
        return this.activeSchemaCreateStatements;
    }

    /**
     * applyDDL
     *
     * Changes the active schema by executing ddl on active schema db
     * and then reloading the activeSchema objects
     *
     * @param sequence
     * @return
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
        } catch (URISyntaxException e) {
            e.printStackTrace();
            LOGGER.error("FATAL: failed to execute DDL statement on active schema.", e);
        } finally{
            try {
                if(con != null) con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return this;
    }
}
