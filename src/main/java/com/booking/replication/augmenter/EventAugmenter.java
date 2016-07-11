package com.booking.replication.augmenter;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.SchemaVersionSnapshot;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.column.types.Converter;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.schema.table.TableSchema;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.StatusVariable;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.binlog.impl.variable.status.QTimeZoneCode;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * EventAugmenter
 *
 * <p>This class contains the logic that tracks the schema
 * that corresponds to current binlog position. It also
 * handles schema transition management when DDL statement
 * is encountered. In addition it maintains a tableMapEvent
 * cache (that is needed to getValue tableName from tableID) and
 * provides utility method for mapping raw binlog event to
 * currently active schema.</p>
 */
public class EventAugmenter {

    // public CurrentTransactionMetadata currentTransactionMetadata;

    private ActiveSchemaVersion activeSchemaVersion;

    private static final Logger LOGGER = LoggerFactory.getLogger(EventAugmenter.class);

    /**
     * Event Augmenter constructor.
     *
     * @param  replicatorConfiguration Replicator Configuration object
     */
    public EventAugmenter(Configuration replicatorConfiguration) throws SQLException, URISyntaxException {
        activeSchemaVersion = new ActiveSchemaVersion(replicatorConfiguration);
    }

    /**
     * Get active schema version.
     *
     * @return ActiveSchemaVersion
     */
    public ActiveSchemaVersion getActiveSchemaVersion() {
        return activeSchemaVersion;
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
    public AugmentedSchemaChangeEvent transitionSchemaToNextVersion(BinlogEventV4 event)
            throws SchemaTransitionException {

        ActiveSchemaVersion futureSchemaVersion;

        // 1. make snapshot of active schema before change
        SchemaVersionSnapshot schemaVersionSnapshotBeforeTransition =
                new SchemaVersionSnapshot(activeSchemaVersion);

        // 2. transition to the new schema
        HashMap<String, String> schemaTransitionSequence = getDDLFromEvent(event);
        if (schemaTransitionSequence != null) {
            // since active schema has a postfix, we need to make sure that queires that
            // specify schema explictly are rewriten so they work properly on active schema
            String ddl = schemaTransitionSequence.get("ddl"); // TODO: FIX sometimes null!!!

            String replicatedSchema = schemaTransitionSequence.get("databaseName");
            String activeSchemaTransitionDDL = rewriteActiveSchemaName(ddl, replicatedSchema);

            schemaTransitionSequence.put("ddl", activeSchemaTransitionDDL);
            futureSchemaVersion = activeSchemaVersion.applyDDL(schemaTransitionSequence);
            if (futureSchemaVersion != null) {
                activeSchemaVersion = futureSchemaVersion;
            } else {
                throw new SchemaTransitionException(String.format(
                        "Failed to calculateAndPropagateChanges with DDL statement: %s",
                        activeSchemaTransitionDDL));
            }
        } else {
            throw new SchemaTransitionException("DDL statement can not be null!");
        }

        // 3. snapshot schema after change
        SchemaVersionSnapshot schemaVersionSnapshotAfterTransition =
                new SchemaVersionSnapshot(activeSchemaVersion);

        // 4. create & return augmentedSchemaChangeEvent
        Long   timestamp = event.getHeader().getTimestamp();

        return new AugmentedSchemaChangeEvent(
                schemaVersionSnapshotBeforeTransition,
                schemaTransitionSequence,
                schemaVersionSnapshotAfterTransition,
                timestamp
        );
    }

    private HashMap<String,String> getDDLFromEvent(BinlogEventV4 event) throws SchemaTransitionException {

        if (event instanceof QueryEvent) {
            String ddl = ((QueryEvent) event).getSql().toString();

            // query
            HashMap<String, String> sqlCommands = new HashMap<>();
            sqlCommands.put("ddl", ddl);

            // status variables
            for (StatusVariable av : ((QueryEvent) event).getStatusVariables()) {

                // handle timezone overrides during schema changes
                if (av instanceof QTimeZoneCode) {
                    QTimeZoneCode tzCode = (QTimeZoneCode) av;

                    LOGGER.info("This DDL query has specified timezone override: " + tzCode.getTimeZone());
                    String timezone = tzCode.getTimeZone().toString();
                    String timezoneSetCommand = "SET @@session.time_zone='" + timezone + "'";
                    String timezoneSetBackToSystem = "SET @@session.time_zone='SYSTEM'";

                    sqlCommands.put("timezonePre", timezoneSetCommand);
                    sqlCommands.put("timezonePost", timezoneSetBackToSystem);
                }
            }
            return sqlCommands;
        } else {
            throw new SchemaTransitionException("Not a valid query event!");
        }
    }

    /**
     * Mangle name of the active schema before applying DDL statements.
     *
     * @param query             Query string
     * @param replicantDbName   Database name
     * @return                  Rewritten query
     */
    public String rewriteActiveSchemaName(String query, String replicantDbName) {
        String dbNamePattern = "( " + replicantDbName + ".)|(`" + replicantDbName + "`.)";
        query = query.replaceAll(dbNamePattern, " ");

        return query;
    }

    /**
     * Map data event to Schema.
     *
     * <p>Maps raw binlog event to column names and types</p>
     *
     * @param  event               AbstractRowEvent
     * @return augmentedDataEvent  AugmentedRow
     */
    public AugmentedRowsEvent mapDataEventToSchema(AbstractRowEvent event, PipelineOrchestrator caller) throws TableMapException {

        AugmentedRowsEvent au;

        switch (event.getHeader().getEventType()) {

            case MySQLConstants.UPDATE_ROWS_EVENT:
                UpdateRowsEvent updateRowsEvent = ((UpdateRowsEvent) event);
                au = augmentUpdateRowsEvent(updateRowsEvent, caller);
                break;
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                UpdateRowsEventV2 updateRowsEventV2 = ((UpdateRowsEventV2) event);
                au = augmentUpdateRowsEventV2(updateRowsEventV2, caller);
                break;
            case MySQLConstants.WRITE_ROWS_EVENT:
                WriteRowsEvent writeRowsEvent = ((WriteRowsEvent) event);
                au = augmentWriteRowsEvent(writeRowsEvent, caller);
                break;
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
                WriteRowsEventV2 writeRowsEventV2 = ((WriteRowsEventV2) event);
                au = augmentWriteRowsEventV2(writeRowsEventV2, caller);
                break;
            case MySQLConstants.DELETE_ROWS_EVENT:
                DeleteRowsEvent deleteRowsEvent = ((DeleteRowsEvent) event);
                au = augmentDeleteRowsEvent(deleteRowsEvent, caller);
                break;
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                DeleteRowsEventV2 deleteRowsEventV2 = ((DeleteRowsEventV2) event);
                au = augmentDeleteRowsEventV2(deleteRowsEventV2, caller);
                break;
            default:
                throw new TableMapException("RBR event type expected! Received type: " + event.getHeader().getEventType(), event);
        }

        if (au == null) {
            throw  new TableMapException("Augmented event ended up as null - something went wrong!", event);
        }

        return au;
    }

    private AugmentedRowsEvent augmentWriteRowsEvent(WriteRowsEvent writeRowsEvent, PipelineOrchestrator caller) throws TableMapException {

        // table name
        String tableName =  caller.currentTransactionMetadata.getTableNameFromID(writeRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchema tableSchema = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        if (tableSchema == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", writeRowsEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(writeRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = writeRowsEvent.getColumnCount().intValue();

        // In write event there is only a List<Row> from getRows. No before after naturally.
        for (Row row : writeRowsEvent.getRows()) {

            AugmentedRow augEvent = new AugmentedRow();
            augEvent.setTableName(tableName);
            augEvent.setTableSchema(tableSchema);
            augEvent.setEventType("INSERT");
            augEvent.setEventV4Header(writeRowsEvent.getHeader());

            tableMetrics.inserted.inc();
            tableMetrics.processed.inc();

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchema.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchema.getColumnSchemaByColumnName(columnName);

                String value = Converter.orTypeToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(columnName, value);
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }

    // TODO: refactor these functions since they are mostly the same. Also move to a different class.
    // Same as for V1 write event. There is some extra data in V2, but not sure if we can use it.
    private AugmentedRowsEvent augmentWriteRowsEventV2(
            WriteRowsEventV2 writeRowsEvent,
            PipelineOrchestrator caller) throws TableMapException {

        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(writeRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchema tableSchema = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchema == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", writeRowsEvent);
        }

        int numberOfColumns = writeRowsEvent.getColumnCount().intValue();

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(writeRowsEvent);
        augEventGroup.setMysqlTableName(tableName);
        
        for (Row row : writeRowsEvent.getRows()) {

            tableMetrics.inserted.inc();
            tableMetrics.processed.inc();

            AugmentedRow augEvent = new AugmentedRow();

            augEvent.setTableName(tableName);
            augEvent.setTableSchema(tableSchema);
            augEvent.setEventType("INSERT");
            augEvent.setEventV4Header(writeRowsEvent.getHeader());

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // getValue column name from indexToNameMap
                String columnName = tableSchema.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchema.getColumnSchemaByColumnName(columnName);

                // type cast
                String value = Converter.orTypeToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(columnName, value);
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }

    private AugmentedRowsEvent augmentDeleteRowsEvent(DeleteRowsEvent deleteRowsEvent, PipelineOrchestrator pipeline)
            throws TableMapException {

        // table name
        String tableName = pipeline.currentTransactionMetadata.getTableNameFromID(deleteRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchema tableSchema = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchema == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", deleteRowsEvent);
        }
        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(deleteRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = deleteRowsEvent.getColumnCount().intValue();

        for (Row row : deleteRowsEvent.getRows()) {

            tableMetrics.processed.inc();
            tableMetrics.deleted.inc();

            AugmentedRow augEvent = new AugmentedRow();
            augEvent.setTableName(tableName);
            augEvent.setTableSchema(tableSchema);
            augEvent.setEventType("DELETE");
            augEvent.setEventV4Header(deleteRowsEvent.getHeader());

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchema.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchema.getColumnSchemaByColumnName(columnName);

                String value = Converter.orTypeToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(columnName, value);
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }

    // For now this is the same as for V1 event.
    private AugmentedRowsEvent augmentDeleteRowsEventV2(
            DeleteRowsEventV2 deleteRowsEvent,
            PipelineOrchestrator caller) throws TableMapException {
        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(deleteRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchema tableSchema = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchema == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", deleteRowsEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(deleteRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = deleteRowsEvent.getColumnCount().intValue();

        for (Row row : deleteRowsEvent.getRows()) {

            tableMetrics.deleted.inc();
            tableMetrics.processed.inc();

            AugmentedRow augEvent = new AugmentedRow();
            augEvent.setTableName(tableName);
            augEvent.setTableSchema(tableSchema);
            augEvent.setEventType("DELETE");
            augEvent.setEventV4Header(deleteRowsEvent.getHeader());

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchema.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchema.getColumnSchemaByColumnName(columnName);

                String value = Converter.orTypeToString(columnValue, columnSchema);

                // TODO: delete has same content as insert, but add a differently named method for clarity
                augEvent.addColumnDataForInsert(columnName, value);
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }

    private AugmentedRowsEvent augmentUpdateRowsEvent(UpdateRowsEvent upEvent, PipelineOrchestrator caller) throws TableMapException {

        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(upEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchema tableSchema = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchema == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", upEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(upEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = upEvent.getColumnCount().intValue();

        // rowPair is pair <rowBeforeChange, rowAfterChange>
        for (Pair<Row> rowPair : upEvent.getRows()) {

            tableMetrics.processed.inc();
            tableMetrics.updated.inc();

            AugmentedRow augEvent = new AugmentedRow();
            augEvent.setTableName(tableName);
            augEvent.setTableSchema(tableSchema); // <- We can do this since in data event schema is unchanged
            augEvent.setEventType("UPDATE");
            augEvent.setEventV4Header(upEvent.getHeader());

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchema.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValueBefore = rowPair.getBefore().getColumns().get(columnIndex - 1);
                Column columnValueAfter = rowPair.getAfter().getColumns().get(columnIndex - 1);

                // We need schema for proper type casting; Since this is RowChange event, schema
                // is the same for both before and after states
                ColumnSchema columnSchema = tableSchema.getColumnSchemaByColumnName(columnName);

                String valueBefore = Converter.orTypeToString(columnValueBefore, columnSchema);
                String valueAfter  = Converter.orTypeToString(columnValueAfter, columnSchema);

                augEvent.addColumnDataForUpdate(columnName, valueBefore, valueAfter);
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }

    // For now this is the same as V1. Not sure if the extra info in V2 can be of use to us.
    private AugmentedRowsEvent augmentUpdateRowsEventV2(UpdateRowsEventV2 upEvent, PipelineOrchestrator caller) throws TableMapException {

        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(upEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchema tableSchema = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchema == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", upEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(upEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = upEvent.getColumnCount().intValue();

        // rowPair is pair <rowBeforeChange, rowAfterChange>
        for (Pair<Row> rowPair : upEvent.getRows()) {

            tableMetrics.processed.inc();
            tableMetrics.updated.inc();

            AugmentedRow augEvent = new AugmentedRow();
            augEvent.setTableName(tableName);
            augEvent.setTableSchema(tableSchema); // <- We can do this since in data event schema is unchanged
            augEvent.setEventType("UPDATE");
            augEvent.setEventV4Header(upEvent.getHeader());

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchema.getColumnIndexToNameMap().get(columnIndex);

                if (columnName == null) {
                    LOGGER.error("null columnName for { columnIndex => " + columnIndex + ", tableName => " + tableName + " }" );
                    throw new TableMapException("columnName cant be null", upEvent);
                }

                // but here index goes from 0..
                Column columnValueBefore = rowPair.getBefore().getColumns().get(columnIndex - 1);
                Column columnValueAfter = rowPair.getAfter().getColumns().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchema.getColumnSchemaByColumnName(columnName);

                try {
                    String valueBefore = Converter.orTypeToString(columnValueBefore, columnSchema);
                    String valueAfter = Converter.orTypeToString(columnValueAfter, columnSchema);

                    augEvent.addColumnDataForUpdate(columnName, valueBefore, valueAfter);
                } catch (TableMapException e) {
                    throw new TableMapException(e.getMessage(), upEvent);
                }
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }

    private static class PerTableMetrics {
        private static String prefix = "mysql";
        private static HashMap<String, PerTableMetrics> tableMetricsHash = new HashMap<>();

        static PerTableMetrics get(String tableName) {
            if (! tableMetricsHash.containsKey(tableName)) {
                tableMetricsHash.put(tableName, new PerTableMetrics(tableName));
            }
            return tableMetricsHash.get(tableName);
        }

        final Counter inserted;
        final Counter processed;
        final Counter deleted;
        final Counter updated;
        final Counter committed;

        PerTableMetrics(String tableName) {
            inserted    = Metrics.registry.counter(name(prefix, tableName, "inserted"));
            processed   = Metrics.registry.counter(name(prefix, tableName, "processed"));
            deleted     = Metrics.registry.counter(name(prefix, tableName, "deleted"));
            updated     = Metrics.registry.counter(name(prefix, tableName, "updated"));
            committed   = Metrics.registry.counter(name(prefix, tableName, "committed"));
        }
    }

}
