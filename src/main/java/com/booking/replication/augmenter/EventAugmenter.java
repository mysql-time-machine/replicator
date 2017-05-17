package com.booking.replication.augmenter;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.event.*;
import com.codahale.metrics.Counter;

import com.booking.replication.Metrics;

import com.booking.replication.pipeline.CurrentTransaction;

import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.column.types.Converter;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.schema.table.TableSchemaVersion;
import com.booking.replication.util.CaseInsensitiveMap;
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

        if (asv == null) {
            throw new SQLException("Active Schema can't be null!");
        }

        this.activeSchemaVersion = asv;
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

    public HashMap<String, String> getSchemaTransitionSequence(RawBinlogEvent event) throws SchemaTransitionException {

        if (event.isQuery()) {

            String ddl = ((RawBinlogEventQuery) event).getSql();

            // query
            HashMap<String, String> sqlCommands = new HashMap<>();
            sqlCommands.put("databaseName", ((RawBinlogEventQuery) event).getDatabaseName());
            sqlCommands.put("originalDDL", ddl);

            sqlCommands.put(
                    "ddl",
                    rewriteActiveSchemaName( // since active schema has a postfix, we need to make sure that queires that
                            ddl,             // specify schema explicitly are rewritten so they work properly on active schema
                            ((RawBinlogEventQuery) event).getDatabaseName().toString()
                    ));

                // handle timezone overrides during schema changes
                if (((RawBinlogEventQuery) event).hasTimezoneOverride()) {

                    HashMap<String,String> timezoneOverrideCommands = ((RawBinlogEventQuery) event).getTimezoneOverrideCommands();

                    if (timezoneOverrideCommands.containsKey("timezonePre")) {
                        sqlCommands.put("timezonePre", timezoneOverrideCommands.get("timezonePre"));
                    }
                    if (timezoneOverrideCommands.containsKey("timezonePost")) {
                        sqlCommands.put("timezonePost",  timezoneOverrideCommands.get("timezonePost"));
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
    public AugmentedRowsEvent mapDataEventToSchema(RawBinlogEventRows event, CurrentTransaction currentTransaction) throws TableMapException {

        AugmentedRowsEvent au;

        switch (event.getEventType()) {
            case UPDATE_ROWS_EVENT:
                RawBinlogEventUpdateRows updateRowsEvent = ((RawBinlogEventUpdateRows) event);
                au = augmentUpdateRowsEvent(updateRowsEvent, currentTransaction);
                break;

            case WRITE_ROWS_EVENT:
                RawBinlogEventWriteRows writeRowsEvent = ((RawBinlogEventWriteRows) event);
                au = augmentWriteRowsEvent(writeRowsEvent, currentTransaction);
                break;

            case DELETE_ROWS_EVENT:
                RawBinlogEventDeleteRows deleteRowsEvent = ((RawBinlogEventDeleteRows) event);
                au = augmentDeleteRowsEvent(deleteRowsEvent, currentTransaction);
                break;

            default:
                throw new TableMapException("RBR event type expected! Received type: " +
                        event.getEventType().toString(), event
                );
        }

        if (au == null) {
            throw  new TableMapException("Augmented event ended up as null - something went wrong!", event);
        }

        return au;
    }

    private AugmentedRowsEvent augmentWriteRowsEvent(RawBinlogEventWriteRows writeRowsEvent, CurrentTransaction currentTransaction) throws TableMapException {

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

        int numberOfColumns = writeRowsEvent.getColumnCount();

        // In write event there is only a List<ParsedRow> from getRows. No before after naturally.

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (com.booking.replication.binlog.common.Row  row : writeRowsEvent.getExtractedRows()) {

            String evType = "INSERT";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid,
                    writeRowsEvent.getPosition(),
                    writeRowsEvent.getTimestamp()
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
                Cell columnValue = row.getRowCells().get(columnIndex - 1);

                String value = Converter.cellValueToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(
                        columnSchema.getColumnName(),
                        value,
                        columnSchema.getColumnType()
                );
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }


    private AugmentedRowsEvent augmentDeleteRowsEvent(RawBinlogEventDeleteRows deleteRowsEvent, CurrentTransaction currentTransaction)
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

        int numberOfColumns = deleteRowsEvent.getColumnCount();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (com.booking.replication.binlog.common.Row row : deleteRowsEvent.getExtractedRows()) {

            String evType =  "DELETE";
            rowBinlogEventOrdinal++;
            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid,
                    deleteRowsEvent.getPosition(),
                    deleteRowsEvent.getTimestamp()
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
                Cell cellValue = row.getRowCells().get(columnIndex - 1);

                String value = Converter.cellValueToString(cellValue, columnSchema);

                augEvent.addColumnDataForInsert(columnSchema.getColumnName(), value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.processed.inc();
            tableMetrics.deleted.inc();
        }

        return augEventGroup;
    }

    private AugmentedRowsEvent augmentUpdateRowsEvent(RawBinlogEventUpdateRows upEvent, CurrentTransaction currentTransaction) throws TableMapException {

        // table name
        String tableName = currentTransaction.getTableNameFromID(upEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getTableSchemaVersion(tableName);

        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", upEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(upEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = upEvent.getColumnCount();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event

        // rowPair is pair <rowBeforeChange, rowAfterChange>
        for (com.booking.replication.binlog.common.RowPair rowPair : upEvent.getExtractedRows()) {

            String evType = "UPDATE";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    currentTransaction.getUuid(),
                    currentTransaction.getXid(),
                    applyUuid,
                    applyXid,
                    upEvent.getPosition(),
                    upEvent.getTimestamp()
            );

            // add transaction uuid and xid
            if (applyUuid) {
                augEvent.addColumnDataForUpdate(UUID_FIELD_NAME, null, currentTransaction.getUuid().toString(), "VARCHAR");
            }
            if (applyXid) {
                augEvent.addColumnDataForUpdate(XID_FIELD_NAME, null, String.valueOf(currentTransaction.getXid()), "BIGINT");
            }

            //column index counting starts with 1 for name tracking
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // We need schema for proper type casting; Since this is RowChange event, schema
                // is the same for both before and after states
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnIndex(columnIndex);

                // but here index goes from 0..
                Cell cellValueBefore = rowPair.getBefore().getRowCells().get(columnIndex - 1);
                Cell cellValueAfter = rowPair.getAfter().getRowCells().get(columnIndex - 1);

                String valueBefore = Converter.cellValueToString(cellValueBefore, columnSchema);
                String valueAfter  = Converter.cellValueToString(cellValueAfter, columnSchema);

                String columnType  = columnSchema.getColumnType();

                augEvent.addColumnDataForUpdate(
                        columnSchema.getColumnName(),
                        valueBefore,
                        valueAfter,
                        columnType
                );
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
