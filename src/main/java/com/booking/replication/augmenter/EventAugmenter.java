package com.booking.replication.augmenter;

import com.booking.replication.Metrics;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.column.types.Converter;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.schema.table.TableSchemaVersion;
import com.booking.replication.util.CaseInsensitiveMap;
import com.codahale.metrics.Counter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.StatusVariable;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.variable.status.QTimeZoneCode;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

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

    public final static String UUID_FIELD_NAME = "_replicator_uuid";
    public final static String XID_FIELD_NAME = "_replicator_xid";

    private ActiveSchemaVersion activeSchemaVersion;
    private final boolean applyUuid;
    private final boolean applyXid;

    private static final Logger LOGGER = LoggerFactory.getLogger(EventAugmenter.class);

    /**
     * Event Augmenter constructor.
     *
     * @param asv Active schema version
     */
    public EventAugmenter(ActiveSchemaVersion asv, boolean applyUuid, boolean applyXid) throws SQLException, URISyntaxException {
        activeSchemaVersion = asv;
        this.applyUuid = applyUuid;
        this.applyXid = applyXid;
    }

    /**
     * Get active schema version.
     *
     * @return ActiveSchemaVersion
     */
    public ActiveSchemaVersion getActiveSchemaVersion() {
        return activeSchemaVersion;
    }

    public HashMap<String, String> getSchemaTransitionSequence(BinlogEventV4 event) throws SchemaTransitionException {

        if (event instanceof QueryEvent) {
            String ddl = ((QueryEvent) event).getSql().toString();

            // query
            HashMap<String, String> sqlCommands = new HashMap<>();
            sqlCommands.put("databaseName", ((QueryEvent) event).getDatabaseName().toString());
            sqlCommands.put("originalDDL", ddl);

            // since active schema has a postfix, we need to make sure that queires that
            // specify schema explictly are rewriten so they work properly on active schema
            sqlCommands.put("ddl", rewriteActiveSchemaName(ddl, ((QueryEvent) event).getDatabaseName().toString()));

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
     * @param currentTransaction   CurrentTransaction
     * @return AugmentedRowsEvent  AugmentedRow
     */
    public AugmentedRowsEvent mapDataEventToSchema(AbstractRowEvent event, CurrentTransaction currentTransaction) throws TableMapException {

        AugmentedRowsEvent au;

        switch (event.getHeader().getEventType()) {

            case MySQLConstants.UPDATE_ROWS_EVENT:
                UpdateRowsEvent updateRowsEvent = ((UpdateRowsEvent) event);
                au = augmentUpdateRowsEvent(updateRowsEvent, currentTransaction);
                break;
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                UpdateRowsEventV2 updateRowsEventV2 = ((UpdateRowsEventV2) event);
                au = augmentUpdateRowsEventV2(updateRowsEventV2, currentTransaction);
                break;
            case MySQLConstants.WRITE_ROWS_EVENT:
                WriteRowsEvent writeRowsEvent = ((WriteRowsEvent) event);
                au = augmentWriteRowsEvent(writeRowsEvent, currentTransaction);
                break;
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
                WriteRowsEventV2 writeRowsEventV2 = ((WriteRowsEventV2) event);
                au = augmentWriteRowsEventV2(writeRowsEventV2, currentTransaction);
                break;
            case MySQLConstants.DELETE_ROWS_EVENT:
                DeleteRowsEvent deleteRowsEvent = ((DeleteRowsEvent) event);
                au = augmentDeleteRowsEvent(deleteRowsEvent, currentTransaction);
                break;
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                DeleteRowsEventV2 deleteRowsEventV2 = ((DeleteRowsEventV2) event);
                au = augmentDeleteRowsEventV2(deleteRowsEventV2, currentTransaction);
                break;
            default:
                throw new TableMapException("RBR event type expected! Received type: " + event.getHeader().getEventType(), event);
        }

        if (au == null) {
            throw  new TableMapException("Augmented event ended up as null - something went wrong!", event);
        }

        return au;
    }

    private AugmentedRowsEvent augmentWriteRowsEvent(WriteRowsEvent writeRowsEvent, CurrentTransaction currentTransaction) throws TableMapException {

        // table name
        String tableName =  currentTransaction.getTableNameFromID(writeRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getTableSchemaVersion(tableName);

        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", writeRowsEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(writeRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = writeRowsEvent.getColumnCount().intValue();

        // In write event there is only a List<Row> from getRows. No before after naturally.

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (Row row : writeRowsEvent.getRows()) {

            String evType = "INSERT";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    writeRowsEvent.getHeader(),
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid
            );

            // add transaction uuid and xid
            if (applyUuid) {
                augEvent.addColumnDataForInsert(UUID_FIELD_NAME, currentTransaction.getUuid().toString(), "VARCHAR");
            }
            if (applyXid) {
                augEvent.addColumnDataForInsert(XID_FIELD_NAME, String.valueOf(currentTransaction.getXid()), "BIGINT");
            }

            tableMetrics.inserted.inc();
            tableMetrics.processed.inc();

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnIndex(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                String value = Converter.orTypeToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(columnSchema.getColumnName(), value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }

    // TODO: refactor these functions since they are mostly the same. Also move to a different class.
    // Same as for V1 write event. There is some extra data in V2, but not sure if we can use it.
    private AugmentedRowsEvent augmentWriteRowsEventV2(
            WriteRowsEventV2 writeRowsEvent,
            CurrentTransaction currentTransaction) throws TableMapException {

        // table name
        String tableName = currentTransaction.getTableNameFromID(writeRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getTableSchemaVersion(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", writeRowsEvent);
        }

        int numberOfColumns = writeRowsEvent.getColumnCount().intValue();

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(writeRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (Row row : writeRowsEvent.getRows()) {

            String evType = "INSERT";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    writeRowsEvent.getHeader(),
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid
            );

            // add transaction uuid and xid
            if (applyUuid) {
                augEvent.addColumnDataForInsert(UUID_FIELD_NAME, currentTransaction.getUuid().toString(), "VARCHAR");
            }
            if (applyXid) {
                augEvent.addColumnDataForInsert(XID_FIELD_NAME, String.valueOf(currentTransaction.getXid()), "BIGINT");
            }

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnIndex(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                // type cast
                String value = Converter.orTypeToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(columnSchema.getColumnName(), value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.inserted.inc();
            tableMetrics.processed.inc();
        }

        return augEventGroup;
    }

    private AugmentedRowsEvent augmentDeleteRowsEvent(DeleteRowsEvent deleteRowsEvent, CurrentTransaction currentTransaction)
            throws TableMapException {

        // table name
        String tableName = currentTransaction.getTableNameFromID(deleteRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getTableSchemaVersion(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", deleteRowsEvent);
        }
        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(deleteRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = deleteRowsEvent.getColumnCount().intValue();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (Row row : deleteRowsEvent.getRows()) {

            String evType =  "DELETE";
            rowBinlogEventOrdinal++;
            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    deleteRowsEvent.getHeader(),
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid
            );

            // add transaction uuid and xid
            if (applyUuid) {
                augEvent.addColumnDataForInsert(UUID_FIELD_NAME, currentTransaction.getUuid().toString(), "VARCHAR");
            }
            if (applyXid) {
                augEvent.addColumnDataForInsert(XID_FIELD_NAME, String.valueOf(currentTransaction.getXid()), "BIGINT");
            }

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnIndex(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                String value = Converter.orTypeToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(columnSchema.getColumnName(), value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.processed.inc();
            tableMetrics.deleted.inc();
        }

        return augEventGroup;
    }

    // For now this is the same as for V1 event.
    private AugmentedRowsEvent augmentDeleteRowsEventV2(
            DeleteRowsEventV2 deleteRowsEvent,
            CurrentTransaction currentTransaction) throws TableMapException {
        // table name
        String tableName = currentTransaction.getTableNameFromID(deleteRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getTableSchemaVersion(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", deleteRowsEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(deleteRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = deleteRowsEvent.getColumnCount().intValue();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (Row row : deleteRowsEvent.getRows()) {

            String evType = "DELETE";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    deleteRowsEvent.getHeader(),
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid
            );

            // add transaction uuid and xid
            if (applyUuid) {
                augEvent.addColumnDataForInsert(UUID_FIELD_NAME, currentTransaction.getUuid().toString(), "VARCHAR");
            }
            if (applyXid) {
                augEvent.addColumnDataForInsert(XID_FIELD_NAME, String.valueOf(currentTransaction.getXid()), "BIGINT");
            }

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnIndex(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                String value = Converter.orTypeToString(columnValue, columnSchema);

                // TODO: delete has same content as insert, but add a differently named method for clarity
                augEvent.addColumnDataForInsert(columnSchema.getColumnName(), value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.deleted.inc();
            tableMetrics.processed.inc();
        }

        return augEventGroup;
    }

    private AugmentedRowsEvent augmentUpdateRowsEvent(UpdateRowsEvent upEvent, CurrentTransaction currentTransaction) throws TableMapException {

        // table name
        String tableName = currentTransaction.getTableNameFromID(upEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getTableSchemaVersion(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", upEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(upEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = upEvent.getColumnCount().intValue();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event

        // rowPair is pair <rowBeforeChange, rowAfterChange>
        for (Pair<Row> rowPair : upEvent.getRows()) {

            String evType = "UPDATE";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    upEvent.getHeader(),
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid
            );

            // add transaction uuid and xid
            if (applyUuid) {
                augEvent.addColumnDataForUpdate(UUID_FIELD_NAME, null, currentTransaction.getUuid().toString(), "VARCHAR");
            }
            if (applyXid) {
                augEvent.addColumnDataForUpdate(XID_FIELD_NAME, null, String.valueOf(currentTransaction.getXid()), "BIGINT");
            }

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // We need schema for proper type casting; Since this is RowChange event, schema
                // is the same for both before and after states
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnIndex(columnIndex);

                // but here index goes from 0..
                Column columnValueBefore = rowPair.getBefore().getColumns().get(columnIndex - 1);
                Column columnValueAfter = rowPair.getAfter().getColumns().get(columnIndex - 1);

                String valueBefore = Converter.orTypeToString(columnValueBefore, columnSchema);
                String valueAfter  = Converter.orTypeToString(columnValueAfter, columnSchema);

                String columnType  = columnSchema.getColumnType();

                augEvent.addColumnDataForUpdate(columnSchema.getColumnName(), valueBefore, valueAfter, columnType);
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.processed.inc();
            tableMetrics.updated.inc();
        }

        return augEventGroup;
    }

    // For now this is the same as V1. Not sure if the extra info in V2 can be of use to us.
    private AugmentedRowsEvent augmentUpdateRowsEventV2(UpdateRowsEventV2 upEvent, CurrentTransaction currentTransaction) throws TableMapException {

        // table name
        String tableName = currentTransaction.getTableNameFromID(upEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getTableSchemaVersion(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", upEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(upEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = upEvent.getColumnCount().intValue();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event

        // rowPair is pair <rowBeforeChange, rowAfterChange>
        for (Pair<Row> rowPair : upEvent.getRows()) {

            String evType = "UPDATE";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    upEvent.getHeader(),
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid
            );

            // add transaction uuid and xid
            if (applyUuid) {
                augEvent.addColumnDataForUpdate(UUID_FIELD_NAME, null, currentTransaction.getUuid().toString(), "VARCHAR");
            }
            if (applyXid) {
                augEvent.addColumnDataForUpdate(XID_FIELD_NAME, null, String.valueOf(currentTransaction.getXid()), "BIGINT");
            }

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnIndex(columnIndex);

                if (columnSchema == null) {
                    LOGGER.error("null columnScheme for { columnIndex => " + columnIndex + ", tableName => " + tableName + " }" );
                    throw new TableMapException("columnName cant be null", upEvent);
                }

                // but here index goes from 0..
                Column columnValueBefore = rowPair.getBefore().getColumns().get(columnIndex - 1);
                Column columnValueAfter = rowPair.getAfter().getColumns().get(columnIndex - 1);

                try {
                    String valueBefore = Converter.orTypeToString(columnValueBefore, columnSchema);
                    String valueAfter  = Converter.orTypeToString(columnValueAfter, columnSchema);
                    String columnType  = columnSchema.getColumnType();

                    augEvent.addColumnDataForUpdate(columnSchema.getColumnName(), valueBefore, valueAfter, columnType);
                } catch (TableMapException e) {
                    TableMapException rethrow = new TableMapException(e.getMessage(), upEvent);
                    rethrow.setStackTrace(e.getStackTrace());
                    throw rethrow;
                }
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.processed.inc();
            tableMetrics.updated.inc();
        }
        return augEventGroup;
    }

    private static class PerTableMetrics {
        private static String prefix = "mysql";
        private static Map<String, PerTableMetrics> tableMetricsHash = new CaseInsensitiveMap<>();

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
