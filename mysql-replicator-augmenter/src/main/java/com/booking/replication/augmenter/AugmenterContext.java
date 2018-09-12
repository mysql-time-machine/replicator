package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.*;
import com.booking.replication.augmenter.model.event.AugmentedEventUpdatedRow;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventDataOperationType;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventDataType;

import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.GTID;
import com.booking.replication.commons.checkpoint.GTIDType;

import com.booking.replication.supplier.model.GTIDRawEventData;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.RotateRawEventData;
import com.booking.replication.supplier.model.TableIdRawEventData;
import com.booking.replication.supplier.model.TableMapRawEventData;
import com.booking.replication.supplier.model.XIDRawEventData;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class AugmenterContext implements Closeable {

    public interface Configuration {
        String TRANSACTION_BUFFER_CLASS = "augmenter.context.transaction.buffer.class";
        String TRANSACTION_BUFFER_LIMIT = "augmenter.context.transaction.buffer.limit";
        String GTID_TYPE = "augmenter.context.gtid.type";
        String BEGIN_PATTERN = "augmenter.context.pattern.begin";
        String COMMIT_PATTERN = "augmenter.context.pattern.commit";
        String DDL_DEFINER_PATTERN = "augmenter.context.pattern.ddl.definer";
        String DDL_TABLE_PATTERN = "augmenter.context.pattern.ddl.table";
        String DDL_TEMPORARY_TABLE_PATTERN = "augmenter.context.pattern.ddl.temporary.table";
        String DDL_VIEW_PATTERN = "augmenter.context.pattern.ddl.view";
        String DDL_ANALYZE_PATTERN = "augmenter.context.pattern.ddl.analyze";
        String PSEUDO_GTID_PATTERN = "augmenter.context.pattern.pseudo.gtid";
        String ENUM_PATTERN = "augmenter.context.pattern.enum";
        String SET_PATTERN = "augmenter.context.pattern.set";
        String EXCLUDE_TABLE = "augmenter.context.exclude.table";
    }

    private static final Logger LOG = Logger.getLogger(AugmenterContext.class.getName());

    private static final int DEFAULT_TRANSACTION_LIMIT = 1000;
    private static final String DEFAULT_GTID_TYPE = GTIDType.REAL.name();
    private static final String DEFAULT_BEGIN_PATTERN = "^(/\\*.*?\\*/\\s*)?(begin)";
    private static final String DEFAULT_COMMIT_PATTERN = "^(/\\*.*?\\*/\\s*)?(commit)";
    private static final String DEFAULT_DDL_DEFINER_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(definer)\\s*=";
    private static final String DEFAULT_DDL_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(table)\\s+(\\S+)";

    private static final String DEFAULT_DDL_TEMPORARY_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(temporary)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_DDL_VIEW_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(view)\\s+(\\S+)";
    private static final String DEFAULT_DDL_ANALYZE_PATTERN = "^(/\\*.*?\\*/\\s*)?(analyze)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_PSEUDO_GTID_PATTERN = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";
    private static final String DEFAULT_ENUM_PATTERN = "(?<=enum\\()(.*?)(?=\\))";
    private static final String DEFAULT_SET_PATTERN = "(?<=set\\()(.*?)(?=\\))";

    private final SchemaManager schemaManager;
    private final CurrentTransaction transaction;

    private final Pattern beginPattern;
    private final Pattern commitPattern;
    private final Pattern ddlDefinerPattern;
    private final Pattern ddlTablePattern;
    private final Pattern ddlTemporaryTablePattern;
    private final Pattern ddlViewPattern;
    private final Pattern ddlAnalyzePattern;
    private final Pattern pseudoGTIDPattern;
    private final Pattern enumPattern;
    private final Pattern setPattern;

    private final List<String> excludeTableList;

    private final AtomicLong timestamp;
    private final AtomicLong serverId;
    private final AtomicLong nextPosition;

    private final AtomicBoolean continueFlag;
    private final AtomicReference<QueryAugmentedEventDataType> queryType;
    private final AtomicReference<QueryAugmentedEventDataOperationType> queryOperationType;
    private final AtomicReference<FullTableName> eventTable;

    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;

    private final GTIDType gtidType;
    private final AtomicReference<String> gtidValue;
    private final AtomicReference<Byte> gtidFlags;

    private final AtomicReference<Collection<ColumnSchema>> columnsBefore;
    private final AtomicReference<String> createTableBefore;
    private final AtomicReference<Collection<ColumnSchema>> columnsAfter;
    private final AtomicReference<String> createTableAfter;
    private final AtomicReference<SchemaAtPositionCache> schemaAtPositionCache;

    private final AtomicBoolean isAtDDL;
    private final AtomicReference<SchemaSnapshot> schemaSnapshot;

    public AugmenterContext(SchemaManager schemaManager, Map<String, Object> configuration) {

        this.schemaManager = schemaManager;

        this.schemaAtPositionCache = new AtomicReference<>();
        this.schemaAtPositionCache.set(new SchemaAtPositionCache());

        this.schemaSnapshot = new AtomicReference<>();

        this.transaction = new CurrentTransaction(
                configuration.getOrDefault(Configuration.TRANSACTION_BUFFER_CLASS, ConcurrentLinkedQueue.class.getName()).toString(),
                Integer.parseInt(configuration.getOrDefault(Configuration.TRANSACTION_BUFFER_LIMIT, String.valueOf(AugmenterContext.DEFAULT_TRANSACTION_LIMIT)).toString())
        );
        this.beginPattern = this.getPattern(configuration, Configuration.BEGIN_PATTERN, AugmenterContext.DEFAULT_BEGIN_PATTERN);
        this.commitPattern = this.getPattern(configuration, Configuration.COMMIT_PATTERN, AugmenterContext.DEFAULT_COMMIT_PATTERN);
        this.ddlDefinerPattern = this.getPattern(configuration, Configuration.DDL_DEFINER_PATTERN, AugmenterContext.DEFAULT_DDL_DEFINER_PATTERN);
        this.ddlTablePattern = this.getPattern(configuration, Configuration.DDL_TABLE_PATTERN, AugmenterContext.DEFAULT_DDL_TABLE_PATTERN);
        this.ddlTemporaryTablePattern = this.getPattern(configuration, Configuration.DDL_TEMPORARY_TABLE_PATTERN, AugmenterContext.DEFAULT_DDL_TEMPORARY_TABLE_PATTERN);
        this.ddlViewPattern = this.getPattern(configuration, Configuration.DDL_VIEW_PATTERN, AugmenterContext.DEFAULT_DDL_VIEW_PATTERN);
        this.ddlAnalyzePattern = this.getPattern(configuration, Configuration.DDL_ANALYZE_PATTERN, AugmenterContext.DEFAULT_DDL_ANALYZE_PATTERN);
        this.pseudoGTIDPattern = this.getPattern(configuration, Configuration.PSEUDO_GTID_PATTERN, AugmenterContext.DEFAULT_PSEUDO_GTID_PATTERN);
        this.enumPattern = this.getPattern(configuration, Configuration.ENUM_PATTERN, AugmenterContext.DEFAULT_ENUM_PATTERN);
        this.setPattern = this.getPattern(configuration, Configuration.SET_PATTERN, AugmenterContext.DEFAULT_SET_PATTERN);
        this.excludeTableList = this.getList(configuration.get(Configuration.EXCLUDE_TABLE));

        this.timestamp = new AtomicLong();
        this.serverId = new AtomicLong();
        this.nextPosition = new AtomicLong();

        this.continueFlag = new AtomicBoolean();
        this.queryType = new AtomicReference<>();
        this.queryOperationType = new AtomicReference<>();
        this.eventTable = new AtomicReference<>();

        this.binlogFilename = new AtomicReference<>();
        this.binlogPosition = new AtomicLong();

        this.gtidType = GTIDType.valueOf(configuration.getOrDefault(Configuration.GTID_TYPE, AugmenterContext.DEFAULT_GTID_TYPE).toString());
        this.gtidValue = new AtomicReference<>();
        this.gtidFlags = new AtomicReference<>();

        this.columnsBefore = new AtomicReference<>();
        this.createTableBefore = new AtomicReference<>();
        this.columnsAfter = new AtomicReference<>();
        this.createTableAfter = new AtomicReference<>();

        this.isAtDDL = new AtomicBoolean();
        this.isAtDDL.set(false);
    }

    private Pattern getPattern(Map<String, Object> configuration, String configurationPath, String configurationDefault) {
        return Pattern.compile(
                configuration.getOrDefault(
                        configurationPath,
                        configurationDefault
                ).toString(),
                Pattern.CASE_INSENSITIVE
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> getList(Object object) {
        if (object != null) {
            if (List.class.isInstance(object)) {
                return (List<String>) object;
            } else {
                return Collections.singletonList(object.toString());
            }
        } else {
            return Collections.emptyList();
        }
    }

    public void updateContext(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        this.updateHeader(
                eventHeader.getTimestamp(),
                eventHeader.getServerId(),
                eventHeader.getNextPosition()
        );

        isAtDDL.set(false);

        switch (eventHeader.getEventType()) {

            case ROTATE:
                this.updateCommons(
                        false,
                        null,
                        null,
                        null,
                        null
                );

                RotateRawEventData rotateRawEventData = RotateRawEventData.class.cast(eventData);

                this.updateBinlog(
                        rotateRawEventData.getBinlogFilename(),
                        rotateRawEventData.getBinlogPosition()
                );
                break;

            case QUERY:
                QueryRawEventData queryRawEventData = QueryRawEventData.class.cast(eventData);
                String query = queryRawEventData.getSQL();
                Matcher matcher;

                // begin
                if (this.beginPattern.matcher(query).find()) {
                    this.updateCommons(
                            false,
                            QueryAugmentedEventDataType.BEGIN,
                            null,
                            queryRawEventData.getDatabase(),
                            null
                    );

                    if (!this.transaction.begin()) {
                        AugmenterContext.LOG.log(Level.WARNING, "transaction already started");
                    }

                }
                // commit
                else if (this.commitPattern.matcher(query).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.COMMIT,
                            null,
                            queryRawEventData.getDatabase(),
                            null
                    );

                    if (!this.transaction.commit(eventHeader.getTimestamp())) {
                        AugmenterContext.LOG.log(Level.WARNING, "transaction already markedForCommit");
                    }
                }
                // ddl definer
                else if ((matcher = this.ddlDefinerPattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_DEFINER,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            queryRawEventData.getDatabase(),
                            null
                    );
                }
                // ddl table
                else if ((matcher = this.ddlTablePattern.matcher(query)).find()) {

                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_TABLE,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            queryRawEventData.getDatabase(),
                            matcher.group(4)
                    );

                    isAtDDL.set(true);

                    long schemaChangeTimestamp = eventHeader.getTimestamp();
                    this.updateSchema(query, schemaChangeTimestamp);

                }
                // ddl temp table
                else if ((matcher = this.ddlTemporaryTablePattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            queryRawEventData.getDatabase(),
                            matcher.group(4)
                    );
                }
                // ddl view
                else if ((matcher = this.ddlViewPattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_VIEW,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            queryRawEventData.getDatabase(),
                            null
                    );
                } else if ((matcher = this.ddlAnalyzePattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_ANALYZE,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            queryRawEventData.getDatabase(),
                            null
                    );
                }

                // pseudoGTID
                else if ((matcher = this.pseudoGTIDPattern.matcher(query)).find()) {
                    this.updateCommons(
                            false,
                            QueryAugmentedEventDataType.PSEUDO_GTID,
                            null,
                            queryRawEventData.getDatabase(),
                            null
                    );

                    this.updateGTID(
                            GTIDType.PSEUDO,
                            matcher.group(0),
                            (byte) 0,
                            0
                    );
                } else {

                    this.updateCommons(
                            false,
                            null,
                            null,
                            queryRawEventData.getDatabase(),
                            null
                    );
                }

                break;

            case XID:
                XIDRawEventData xidRawEventData = XIDRawEventData.class.cast(eventData);

                this.updateCommons(
                        true,
                        QueryAugmentedEventDataType.COMMIT,
                        null,
                        null,
                        null
                );

                if (!this.transaction.commit(xidRawEventData.getXID(), eventHeader.getTimestamp())) {
                    AugmenterContext.LOG.log(Level.WARNING, "transaction already markedForCommit");
                }
                break;

            case GTID:
                GTIDRawEventData gtidRawEventData = GTIDRawEventData.class.cast(eventData);

                this.updateCommons(
                        false,
                        QueryAugmentedEventDataType.GTID,
                        null,
                        null,
                        null
                );

                this.updateGTID(
                        GTIDType.REAL,
                        gtidRawEventData.getGTID(),
                        gtidRawEventData.getFlags(),
                        0
                );
                break;

            case TABLE_MAP:
                this.updateCommons(
                        false,
                        QueryAugmentedEventDataType.COMMIT,
                        null,
                        null,
                        null
                );

                TableMapRawEventData tableMapRawEventData = TableMapRawEventData.class.cast(eventData);

                this.schemaAtPositionCache.get().getTableIdToTableNameMap().put(
                        tableMapRawEventData.getTableId(),
                        new FullTableName(
                                tableMapRawEventData.getDatabase(),
                                tableMapRawEventData.getTable()
                        )
                );

                break;
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                TableIdRawEventData tableIdRawEventData = TableIdRawEventData.class.cast(eventData);
                FullTableName eventTable = this.getEventTable(tableIdRawEventData.getTableId());

                this.updateCommons(
                        (eventTable == null) || (!this.excludeTable(eventTable.getName())),
                        null,
                        null,
                        null,
                        null
                );

                break;
            default:
                this.updateCommons(
                        false,
                        null,
                        null,
                        null,
                        null
                );

                break;
        }
    }

    private void updateHeader(long timestamp, long serverId, long nextPosition) {
        this.timestamp.set(timestamp);
        this.serverId.set(serverId);
        this.nextPosition.set(nextPosition);
    }

    private void updateCommons(
            boolean continueFlag,
            QueryAugmentedEventDataType queryType,
            QueryAugmentedEventDataOperationType queryOperationType,
            String database,
            String table
        ) {
        this.continueFlag.set(continueFlag);
        this.queryType.set(queryType);
        this.queryOperationType.set(queryOperationType);

        if (table != null) {
            this.eventTable.set(new FullTableName(database, table));
        }
    }

    private void updateBinlog(String filename, long position) {
        this.binlogFilename.set(filename);
        this.binlogPosition.set(0);
        this.nextPosition.set(position);
    }

    private void updateGTID(GTIDType type, String value, byte flags, int index) {
        if (this.gtidType == type) {
            this.gtidValue.set(value);
            this.gtidFlags.set(flags);
        }
    }

    /**
     *  update || create have value_after
     *  drop || update have value_before
     * */
    private void updateSchema(String query, long schemaChangeTimestamp) {

        if (query != null) {

            if (this.eventTable.get() != null) {

                SchemaAtPositionCache schemaPositionCacheBefore = schemaAtPositionCache.get().deepCopy();

                String tableName = this.eventTable.get().getName();

                if (isDDLAndIsNot(QueryAugmentedEventDataOperationType.CREATE)) {
                    this.columnsBefore.set(this.schemaManager.listColumns(tableName));
                    this.createTableBefore.set(this.schemaManager.getCreateTable(tableName));
                } else {
                    this.columnsBefore.set(null);
                    this.createTableBefore.set(null);
                }

                this.schemaManager.execute(tableName, query);
                this.schemaAtPositionCache.get().reloadTableSchema(
                        tableName,
                        SchemaHelpers.fnComputeTableSchema
                );

                if (isDDLAndIsNot(QueryAugmentedEventDataOperationType.DROP)) {
                    // Not drop, so create/alter, set after?
                    this.columnsAfter.set(this.schemaManager.listColumns(tableName));
                    this.createTableAfter.set(this.schemaManager.getCreateTable(tableName));
                } else {
                    //
                    this.columnsAfter.set(null);
                    this.createTableAfter.set(null);
                }

                SchemaAtPositionCache schemaPositionCacheAfter = schemaAtPositionCache.get().deepCopy();
                SchemaTransitionSequence schemaTransitionSequence = new SchemaTransitionSequence(
                        eventTable,
                        columnsBefore,
                        createTableBefore,
                        columnsAfter,
                        createTableAfter,
                        query,
                        schemaChangeTimestamp
                );
                SchemaSnapshot newSchemaSnapshot = new SchemaSnapshot(
                        schemaTransitionSequence,
                        schemaPositionCacheBefore,
                        schemaPositionCacheAfter
                );
                schemaSnapshot.set(newSchemaSnapshot);

            } else {
                LOG.info("Unrecognised query type: " + query);

                this.columnsBefore.set(null);
                this.createTableBefore.set(null);

                this.schemaManager.execute(null, query);

                this.columnsAfter.set(null);
                this.createTableAfter.set(null);
            }
        } else {
            throw new RuntimeException("Query cannot be null");
        }
    }

    private boolean isDDLAndIsNot(QueryAugmentedEventDataOperationType ddlOpType) {
        return ((this.queryType.get() == QueryAugmentedEventDataType.DDL_TABLE
                ||
                this.queryType.get() == QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE
               )
               &&
               this.getQueryOperationType() != ddlOpType);
    }

    private boolean excludeTable(String tableName) {
        return this.excludeTableList.contains(tableName);
    }

    public void updatePosition() {
        if (this.nextPosition.get() > 0) {
            this.binlogPosition.set(this.nextPosition.get());
        }
    }

    public CurrentTransaction getTransaction() {
        return this.transaction;
    }

    public boolean shouldProcess() {
        return this.continueFlag.get();
    }

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType.get();
    }

    public QueryAugmentedEventDataOperationType getQueryOperationType() {
        return this.queryOperationType.get();
    }

    public FullTableName getEventTable() {
        return this.eventTable.get();
    }

    public Checkpoint getCheckpoint() {
        return new Checkpoint(
                this.timestamp.get(),
                this.serverId.get(),
                this.getGTID(),
                this.getBinlog()
        );
    }

    private GTID getGTID() {
        if (this.gtidValue.get() != null) {
            return new GTID(
                    this.gtidType,
                    this.gtidValue.get(),
                    this.gtidFlags.get()
            );
        } else {
            return null;
        }
    }

    private Binlog getBinlog() {
        if (this.binlogFilename.get() != null) {
            return new Binlog(
                    this.binlogFilename.get(),
                    this.binlogPosition.get()
            );
        } else {
            return null;
        }
    }

    public TableSchema getSchemaBefore() {
        if (this.columnsBefore.get() != null && this.createTableBefore != null) {
            return new TableSchema(
                    this.columnsBefore.get(),
                    this.createTableBefore.get()
            );
        } else {
            return null;
        }
    }

    public TableSchema getSchemaAfter() {
        if (this.columnsAfter.get() != null && this.createTableAfter != null) {
            return new TableSchema(
                    this.columnsAfter.get(),
                    this.createTableAfter.get()
            );
        } else {
            return null;
        }
    }

    public FullTableName getEventTable(long tableId) {
        return this.schemaAtPositionCache.get().getTableIdToTableNameMap().get(tableId);
    }

    public Collection<Boolean> getIncludedColumns(BitSet includedColumns) {
        Collection<Boolean> includedColumnList = new ArrayList<>(includedColumns.length());

        for (int index = 0; index < includedColumns.length(); index++) {
            includedColumnList.add(includedColumns.get(index));
        }

        return includedColumnList;
    }

    public Collection<ColumnSchema> getColumns(long tableId) {
        FullTableName eventTable = this.getEventTable(tableId);

        if (eventTable != null) {
            return this.schemaManager.listColumns(eventTable.getName());
        } else {
            return null;
        }
    }

    public Collection<Map<String, Object>> getRows(long tableId, BitSet includedColumns, List<Serializable[]> rows) {
        FullTableName eventTable = this.getEventTable(tableId);

        if (eventTable != null) {
            Collection<Map<String, Object>> rowList = new ArrayList<>();
            List<ColumnSchema> columns = this.schemaManager.listColumns(eventTable.getName());
            Map<String, String[]> cache = this.getCache(columns);

            for (Serializable[] row : rows) {
                rowList.add(this.getRow(columns, includedColumns, row, cache));
            }

            return rowList;
        } else {
            return null;
        }
    }
    public Collection<AugmentedRow> getAugmentedRows(
            String eventType,
            AtomicLong commitTimestamp,
            UUID transactionUUID,
            Long xxid,
            long tableId,
            BitSet includedColumns,
            List<Serializable[]> rows
    ) {

        FullTableName eventTable = this.getEventTable(tableId);

        if (eventTable != null) {

            Collection<AugmentedRow> augmentedRows = new ArrayList<>();
            List<ColumnSchema> columns = this.schemaManager.listColumns(eventTable.getName());
            Map<String, String[]> cache = this.getCache(columns);

            for (Serializable[] row : rows) {
                augmentedRows.add(
                        this.getAugmentedRow(
                                eventType,
                                commitTimestamp.get(),
                                transactionUUID,
                                xxid,
                                columns,
                                includedColumns,
                                row,
                                cache
                        )
                );
            }
            return augmentedRows;
        } else {
            return null;
        }
    }

    public Collection<AugmentedEventUpdatedRow> getUpdatedRows(long tableId, BitSet includedColumns, List<Map.Entry<Serializable[], Serializable[]>> rows) {
        FullTableName eventTable = this.getEventTable(tableId);

        if (eventTable != null) {
            List<AugmentedEventUpdatedRow> rowList = new ArrayList<>();
            List<ColumnSchema> columns = this.schemaManager.listColumns(eventTable.getName());
            Map<String, String[]> cache = this.getCache(columns);

            for (Map.Entry<Serializable[], Serializable[]> row : rows) {
                rowList.add(new AugmentedEventUpdatedRow(
                        this.getRow(columns, includedColumns, row.getKey(), cache),
                        this.getRow(columns, includedColumns, row.getValue(), cache)
                ));
            }

            return rowList;
        } else {
            return null;
        }
    }

    // TODO: deprecate getRow in favour of getAugmentedRow
    private AugmentedRow getAugmentedRow(
            String eventType,
            Long commitTimestamp,
            UUID transactionUUID,
            Long xxid,
            List<ColumnSchema> columns,
            BitSet includedColumns,
            Serializable[] row,
            Map<String, String[]> cache
    ) {
        Map<String, Map<String, String>> rowColumns = new HashMap<>();

        // TODO: encapsulate in Stringifier class
        if (columns != null) {
            for (int columnIndex = 0, rowIndex = 0; columnIndex < columns.size() && rowIndex < row.length; columnIndex++) {
                if (includedColumns.get(columnIndex)) {

                    ColumnSchema column = columns.get(columnIndex);

                    String columnName = column.getName();
                    String columnType = column.getType().toLowerCase();
                    Serializable cellValue = row[rowIndex++];
                    String collation = column.getCollation();

                    String stringifiedCellValue = null;

                    if(collation != null && (cellValue instanceof byte[])){
                        byte[] bytes = (byte[])cellValue;
                        if(collation.contains("latin1")){
                            stringifiedCellValue = new String(bytes,StandardCharsets.ISO_8859_1);
                        }else{
                            // Currently handle all the other character set as UTF8, extend this to handle specific character sets
                            stringifiedCellValue = new String(bytes, StandardCharsets.UTF_8);
                        }
                    }

                    if(cellValue instanceof  BitSet){
                        final BitSet data = (BitSet)cellValue;
                        final StringBuilder buffer = new StringBuilder(data.length());
                        IntStream.range(0, data.length()).mapToObj( i -> data.get(i) ? '1': '0').forEach(buffer::append);
                        stringifiedCellValue = buffer.reverse().toString();
                    }

                    if(columnType.contains("unsigned") && cellValue != null ){
                        if(columnType.contains("tiny")){
                            stringifiedCellValue = String.valueOf(Byte.toUnsignedInt(((Integer)cellValue).byteValue()));
                        }else if(columnType.contains("small")){
                            stringifiedCellValue = String.valueOf(((Integer)cellValue) & 0xffff);
                        }else if(columnType.contains("medium")){
                            stringifiedCellValue = String.valueOf(((Integer)cellValue) & 0xffffff);
                        }else if(columnType.contains("bigint")){
                            long i = (Long)cellValue;
                            int upper = (int) (i >>> 32);
                            int lower = (int) i;

                            stringifiedCellValue = String.valueOf(
                                    BigInteger.valueOf(
                                            Integer.toUnsignedLong(upper)
                                    )
                                            .shiftLeft(32)
                                            .add(
                                                    BigInteger.valueOf(Integer.toUnsignedLong(lower))
                                            )
                            );

                        } else if(columnType.contains("int")){
                            stringifiedCellValue = String.valueOf(Long.valueOf(((Integer)cellValue)) & 0x00000000FFFFFFFFl);
                        }
                    }

                    if (cache.containsKey(columnType)) {
                        if (columnType.startsWith("enum")) {
                            int index = Number.class.cast(cellValue).intValue();

                            if (index > 0) {
                                stringifiedCellValue = String.valueOf(cache.get(columnType)[index - 1]);
                            } else {
                                stringifiedCellValue = null;
                            }
                        } else if (columnType.startsWith("set")) {
                            long bits = Number.class.cast(cellValue).longValue();

                            if (bits > 0) {
                                String[] members = cache.get(columnType);
                                List<String> items = new ArrayList<>();

                                for (int index = 0; index < members.length; index++) {
                                    if (((bits >> index) & 1) == 1) {
                                        items.add(members[index]);
                                    }
                                }

                                stringifiedCellValue = String.valueOf(items.toArray(new String[0]));
                            } else {
                                stringifiedCellValue = null;
                            }
                        }
                    }

                    if (eventType.equals("INSERT")) {
                        rowColumns.put(columnName, new HashMap<>());
                        rowColumns.get(columnName).put("before", stringifiedCellValue);
                    } else {
                        // TODO: implement update/delete
                    }
                }
            }
        } else {
            throw new RuntimeException("Invalid data. Columns list cannot be null!");
        }

        // =============================================================

        TableSchema tableSchema = new TableSchema(
                (Collection<ColumnSchema>) rowColumns,
                null // TODO: ? not really needed for AugmentedRow
        );

        AugmentedRow augmentedRow = new AugmentedRow(
                tableSchema,
                null,        // TODO
                null, // TODO
                transactionUUID,
                xxid,
                commitTimestamp,
                null, // TODO
                eventType,
                rowColumns
        );

        return augmentedRow;
    }

    private Map<String, Object> getRow(List<ColumnSchema> columns, BitSet includedColumns, Serializable[] row, Map<String, String[]> cache) {
        Map<String, Object> rowMap = new LinkedHashMap<>();

        if (columns != null) {
            for (int columnIndex = 0, rowIndex = 0; columnIndex < columns.size() && rowIndex < row.length; columnIndex++) {
                if (includedColumns.get(columnIndex)) {
                    ColumnSchema column = columns.get(columnIndex);
                    String columnName = column.getName();
                    String columnType = column.getType().toLowerCase();
                    Serializable cellValue = row[rowIndex++];
                    String collation = column.getCollation();

                    if(collation != null && (cellValue instanceof byte[])){
                        byte[] bytes = (byte[])cellValue;
                        if(collation.contains("latin1")){
                            cellValue = new String(bytes,StandardCharsets.ISO_8859_1);
                        }else{
                            // Currently handle all the other character set as UTF8, extend this to handle specific character sets
                            cellValue = new String(bytes, StandardCharsets.UTF_8);
                        }
                    }

                    if(cellValue instanceof  BitSet){
                        final BitSet data = (BitSet)cellValue;
                        final StringBuilder buffer = new StringBuilder(data.length());
                        IntStream.range(0, data.length()).mapToObj( i -> data.get(i) ? '1': '0').forEach(buffer::append);
                        cellValue = buffer.reverse().toString();
                    }

                    if(columnType.contains("unsigned") && cellValue != null ){
                        if(columnType.contains("tiny")){
                            cellValue = Byte.toUnsignedInt(((Integer)cellValue).byteValue());
                        } else if(columnType.contains("small")){
                            cellValue = ((Integer)cellValue) & 0xffff;
                        } else if(columnType.contains("medium")){
                            cellValue =  ((Integer)cellValue) & 0xffffff;
                        } else if(columnType.contains("bigint")){
                            long i = (Long)cellValue;
                            int upper = (int) (i >>> 32);
                            int lower = (int) i;

                            cellValue = (BigInteger.valueOf(Integer.toUnsignedLong(upper))).shiftLeft(32).
                                    add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));

                        } else if(columnType.contains("int")){
                            cellValue =  Long.valueOf(((Integer)cellValue)) & 0x00000000FFFFFFFFl;
                        }
                    }

                    if (cache.containsKey(columnType)) {
                        if (columnType.startsWith("enum")) {
                            int index = Number.class.cast(cellValue).intValue();

                            if (index > 0) {
                                cellValue = cache.get(columnType)[index - 1];
                            } else {
                                cellValue = null;
                            }
                        } else if (columnType.startsWith("set")) {
                            long bits = Number.class.cast(cellValue).longValue();

                            if (bits > 0) {
                                String[] members = cache.get(columnType);
                                List<String> items = new ArrayList<>();

                                for (int index = 0; index < members.length; index++) {
                                    if (((bits >> index) & 1) == 1) {
                                        items.add(members[index]);
                                    }
                                }
                                cellValue = items.toArray(new String[0]);
                            } else {
                                cellValue = null;
                            }
                        }
                    }
                    rowMap.put(columnName, cellValue);
                }
            }
        } else {
            for (Serializable cell : row) {
                rowMap.put("unknown", cell);
            }
        }
        return rowMap;
    }

    private Map<String, String[]> getCache(List<ColumnSchema> columns) {
        Matcher matcher;
        Map<String, String[]> cache = new HashMap<>();

        for (ColumnSchema column : columns) {
            String columnType = column.getType().toLowerCase();

            if (((matcher = this.enumPattern.matcher(columnType)).find() && matcher.groupCount() > 0) || ((matcher = this.setPattern.matcher(columnType)).find() && matcher.groupCount() > 0)) {
                String[] members = matcher.group(0).split(",");

                if (members.length > 0) {
                    for (int index = 0; index < members.length; index++) {
                        if (members[index].startsWith("'") && members[index].endsWith("'")) {
                            members[index] = members[index].substring(1, members[index].length() - 1);
                        }
                    }
                    cache.put(columnType, members);
                }
            }
        }

        return cache;
    }

    @Override
    public void close() throws IOException {
        this.schemaManager.close();
    }

    public Boolean isAtDdl() {
        return isAtDDL.get();
    }

    public SchemaSnapshot getSchemaSnapshot() {
        return schemaSnapshot.get();
    }
}
