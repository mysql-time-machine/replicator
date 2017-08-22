package com.booking.replication.schema;

import com.booking.replication.Configuration;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.column.types.EnumColumnSchema;
import com.booking.replication.schema.column.types.SetColumnSchema;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.schema.table.TableSchemaVersion;
import com.booking.replication.util.JsonBuilder;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * ActiveSchemaVersion refers to the schema that corresponds
 * to the current position in the binlog.
 * This class abstracts work with database in which
 * this data is maintained.
 */
public class MysqlActiveSchemaVersion implements ActiveSchemaVersion {

    private static final String SHOW_TABLES_SQL        = "SHOW TABLES";
    private static final String SHOW_CREATE_TABLE_SQL  = "SHOW CREATE TABLE ";
    private static final String INFORMATION_SCHEMA_SQL =
            "SELECT * FROM `information_schema`.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

    private final HashMap<String,String> activeSchemaCreateStatements = new HashMap<>();
    private final HashMap<String,TableSchemaVersion> activeSchemaTables      = new HashMap<>();

    private String lastReceivedDDL = null;

    private final Configuration configuration;

    // TODO: refactor this so that datasource is passed in constructor
    //       so that same pool is shared for different objects
    private final BasicDataSource activeSchemaDataSource;
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlActiveSchemaVersion.class);

    public MysqlActiveSchemaVersion(Configuration replicatorConfiguration) throws URISyntaxException, SQLException {

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

    @Override
    public void loadActiveSchema() throws SQLException {
        Connection con = null;

        try {
            con = activeSchemaDataSource.getConnection();

            // 1 .Get list of tables in active schema
            List<String> tableNames = getTableList(con);

            // 2. For each table check if needed to:
            //  - get and cache its create statement
            //  - create and initialize TableSchemaVersion object
            for (String tableName : tableNames) {
                if (lastReceivedDDL == null) {
                    // replicator is starting
                    loadAndCacheTableSchemaInfo(con, tableName);
                } else {
                    if (!skipLoadTable(tableName, lastReceivedDDL)) {
                        loadAndCacheTableSchemaInfo(con, tableName);
                    }
                }
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

    private List<String> getTableList(Connection con) throws SQLException {
        Statement showTablesStatement         = con.createStatement();
        ResultSet showTablesResultSet         = showTablesStatement.executeQuery(SHOW_TABLES_SQL);
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
        return tableNames;
    }

    private void loadAndCacheTableSchemaInfo(Connection con, String tableName) throws SQLException {

        // get and cache table's create statement
        PreparedStatement showCreateTableStatement = con.prepareStatement(SHOW_CREATE_TABLE_SQL + tableName);
        ResultSet showCreateTableResultSet = showCreateTableStatement.executeQuery();
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

            // TODO: improve this to contian schema versions per table name
            this.activeSchemaCreateStatements.put(tableName, returnedCreateStatement);
        }
        showCreateTableResultSet.close();
        showCreateTableStatement.close();

        // create and initialize TableSchemaVersion object
        this.activeSchemaTables.put(tableName, new TableSchemaVersion());

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

    @Override
    public String schemaTablesToJson() {
        return  JsonBuilder.schemaVersionTablesToJson(activeSchemaTables);
    }

    @Override
    public String schemaCreateStatementsToJson() {
        return  JsonBuilder.schemaCreateStatementsToJson(activeSchemaCreateStatements);
    }

    @Override
    public String toJson() {
        return JsonBuilder.schemaVersionToJson(this);
    }

    @Override
    public HashMap<String, TableSchemaVersion> getActiveSchemaTables() {
        return activeSchemaTables;
    }

    @Override
    public HashMap<String, String> getActiveSchemaCreateStatements() {
        return activeSchemaCreateStatements;
    }

    /**
     * Transitions active schema to a new state that corresponds
     * to the current binlog position.
     *
     * <p>Steps performed are:
     *
     *       1. make snapshot of active schema before change
     *       2. transition to the new schema
     *       3. snapshot schema after change
     *       4. create augmentedSchemaChangeEvent
     *       5. return augmentedSchemaChangeEvent
     * </p>
     */
    @Override
    public AugmentedSchemaChangeEvent transitionSchemaToNextVersion(HashMap<String, String> schemaTransitionSequence, Long timestamp)
            throws SchemaTransitionException {

        // 1. make snapshot of active schema before change
        final SchemaVersionSnapshot schemaVersionSnapshotBeforeTransition =
                new SchemaVersionSnapshot(this);

        // 2. transition to the new schema
        if (schemaTransitionSequence == null) {
            throw new SchemaTransitionException("DDL statement can not be null!");
        }

        try {
            LOGGER.info("Try DDL");
            applyDDL(schemaTransitionSequence);
        } catch (Exception e) {
            String activeSchemaTransitionDDL = schemaTransitionSequence.get("ddl");
            throw new SchemaTransitionException(String.format(
                    "Failed to calculateAndPropagateChanges with DDL statement: %s",
                    activeSchemaTransitionDDL),
                    e);
        }

        // 3. snapshot schema after change
        final SchemaVersionSnapshot schemaVersionSnapshotAfterTransition =
                new SchemaVersionSnapshot(this);

        // 4. create & return augmentedSchemaChangeEvent
        return new AugmentedSchemaChangeEvent(
                schemaVersionSnapshotBeforeTransition,
                schemaTransitionSequence,
                schemaVersionSnapshotAfterTransition,
                timestamp
        );
    }

    /**
     * Apply DDL statements.
     *
     * <p>Changes the active schema by executing ddl on active schema db
     * and then reloading the activeSchema objects</p>
     *
     * @param sequence Sequence of DDL statements for schema transition
     * @return ActiveSchemaVersion
     */
    @Override
    public void applyDDL(HashMap<String, String> sequence)
            throws SchemaTransitionException, SQLException {

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
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    private boolean skipLoadTable(String tableName, String ddlStatement) {
        // Skip if:
        //  1. table is allready cached and it is not mentioned in DDL statement
        if  (!isMentionedInDDLStatement(tableName, ddlStatement) && activeSchemaTables.containsKey(tableName)) {
            return true;
        } else {
            return false;
        }

    }

    private boolean isMentionedInDDLStatement(String tableName, String ddlStatement) {

        Pattern pattern = Pattern.compile(tableName, Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(ddlStatement);

        return matcher.find();
    }

}
