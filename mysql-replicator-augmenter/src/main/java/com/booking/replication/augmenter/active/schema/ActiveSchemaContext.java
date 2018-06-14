package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventColumn;
import com.booking.replication.augmenter.model.AugmentedEventUpdatedRow;
import com.booking.replication.augmenter.model.AugmentedEventTable;
import com.booking.replication.augmenter.model.QueryAugmentedEventDataOperationType;
import com.booking.replication.augmenter.model.QueryAugmentedEventDataType;
import com.booking.replication.commons.checkpoint.Checkpoint;
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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ActiveSchemaContext implements Closeable {
    public interface Configuration {
        String TRANSACTION_LIMIT = "augmenter.context.transaction.limit";
        String BEGIN_PATTERN = "augmenter.context.pattern.begin";
        String COMMIT_PATTERN = "augmenter.context.pattern.commit";
        String DDL_DEFINER_PATTERN = "augmenter.context.pattern.ddl.definer";
        String DDL_TABLE_PATTERN = "augmenter.context.pattern.ddl.table";
        String DDL_TEMPORARY_TABLE_PATTERN = "augmenter.context.pattern.ddl.temporary.table";
        String DDL_VIEW_PATTERN = "augmenter.context.pattern.ddl.view";
        String DDL_ANALYZE_PATTERN = "augmenter.context.pattern.ddl.analyze";
        String PSEUDO_GTID_PATTERN = "augmenter.context.pattern.pseudogtid";
        String EXCLUDE_TABLE = "augmenter.context.exclude.table";
    }

    private static final Logger LOG = Logger.getLogger(ActiveSchemaContext.class.getName());

    private static final int DEFAULT_TRANSACTION_LIMIT = 1000;
    private static final String DEFAULT_BEGIN_PATTERN = "^(/\\*.*?\\*/\\s*)?(begin)";
    private static final String DEFAULT_COMMIT_PATTERN = "^(/\\*.*?\\*/\\s*)?(commit)";
    private static final String DEFAULT_DDL_DEFINER_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(definer)\\s*=";
    private static final String DEFAULT_DDL_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_DDL_TEMPORARY_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(temporary)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_DDL_VIEW_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(view)\\s+(\\S+)";
    private static final String DEFAULT_DDL_ANALYZE_PATTERN = "^(/\\*.*?\\*/\\s*)?(analyze)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_PSEUDO_GTID_PATTERN = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";

    private final CurrentTransaction transaction;
    private final ActiveSchemaManager manager;

    private final Pattern beginPattern;
    private final Pattern commitPattern;
    private final Pattern ddlDefinerPattern;
    private final Pattern ddlTablePattern;
    private final Pattern ddlTemporaryTablePattern;
    private final Pattern ddlViewPattern;
    private final Pattern ddlAnalyzePattern;
    private final Pattern pseudoGTIDPattern;

    private final List<String> excludeTableList;

    private final AtomicLong serverId;

    private final AtomicBoolean continueFlag;
    private final AtomicReference<QueryAugmentedEventDataType> queryType;
    private final AtomicReference<QueryAugmentedEventDataOperationType> queryOperationType;
    private final AtomicReference<AugmentedEventTable> eventTable;

    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;

    private final AtomicReference<String> pseudoGTIDValue;
    private final AtomicInteger pseudoGTIDIndex;

    private final AtomicReference<String> createTableBefore;
    private final AtomicReference<String> createTableAfter;

    private final Map<Long, AugmentedEventTable> tableIdEventTableMap;

    public ActiveSchemaContext(Map<String, Object> configuration) {
        this.transaction = new CurrentTransaction(Integer.parseInt(configuration.getOrDefault(Configuration.TRANSACTION_LIMIT, String.valueOf(ActiveSchemaContext.DEFAULT_TRANSACTION_LIMIT)).toString()));
        this.manager = new ActiveSchemaManager(configuration);
        this.beginPattern = this.getPattern(configuration, Configuration.BEGIN_PATTERN, ActiveSchemaContext.DEFAULT_BEGIN_PATTERN);
        this.commitPattern = this.getPattern(configuration, Configuration.COMMIT_PATTERN, ActiveSchemaContext.DEFAULT_COMMIT_PATTERN);
        this.ddlDefinerPattern = this.getPattern(configuration, Configuration.DDL_DEFINER_PATTERN, ActiveSchemaContext.DEFAULT_DDL_DEFINER_PATTERN);
        this.ddlTablePattern = this.getPattern(configuration, Configuration.DDL_TABLE_PATTERN, ActiveSchemaContext.DEFAULT_DDL_TABLE_PATTERN);
        this.ddlTemporaryTablePattern = this.getPattern(configuration, Configuration.DDL_TEMPORARY_TABLE_PATTERN, ActiveSchemaContext.DEFAULT_DDL_TEMPORARY_TABLE_PATTERN);
        this.ddlViewPattern = this.getPattern(configuration, Configuration.DDL_VIEW_PATTERN, ActiveSchemaContext.DEFAULT_DDL_VIEW_PATTERN);
        this.ddlAnalyzePattern = this.getPattern(configuration, Configuration.DDL_ANALYZE_PATTERN, ActiveSchemaContext.DEFAULT_DDL_ANALYZE_PATTERN);
        this.pseudoGTIDPattern = this.getPattern(configuration, Configuration.PSEUDO_GTID_PATTERN, ActiveSchemaContext.DEFAULT_PSEUDO_GTID_PATTERN);
        this.excludeTableList = this.getList(configuration.get(Configuration.EXCLUDE_TABLE));

        this.serverId = new AtomicLong();

        this.continueFlag = new AtomicBoolean();
        this.queryType = new AtomicReference<>();
        this.queryOperationType = new AtomicReference<>();
        this.eventTable = new AtomicReference<>();

        this.binlogFilename = new AtomicReference<>();
        this.binlogPosition = new AtomicLong();

        this.pseudoGTIDValue = new AtomicReference<>();
        this.pseudoGTIDIndex = new AtomicInteger();

        this.createTableBefore = new AtomicReference<>();
        this.createTableAfter = new AtomicReference<>();

        this.tableIdEventTableMap = new ConcurrentHashMap<>();
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

    private Pattern getPattern(Map<String, Object> configuration, String configurationPath, String configurationDefault) {
        return Pattern.compile(
                configuration.getOrDefault(
                        configurationPath,
                        configurationDefault
                ).toString(),
                Pattern.CASE_INSENSITIVE
        );
    }

    private void updateServer(long id) {
        this.serverId.set(id);
    }

    private void updateCommons(boolean continueFlag, QueryAugmentedEventDataType queryType, QueryAugmentedEventDataOperationType queryOperationType, String database, String table) {
        this.continueFlag.set(continueFlag);
        this.queryType.set(queryType);
        this.queryOperationType.set(queryOperationType);

        if (table != null) {
            this.eventTable.set(new AugmentedEventTable(database, table));
        }
    }

    private void updateBinlog(String filename, long position) {
        this.binlogFilename.set(filename);
        this.binlogPosition.set(position);
    }

    private void updatePseudoGTID(String value, int index) {
        this.pseudoGTIDValue.set(value);
        this.pseudoGTIDIndex.set(index);
    }

    private void updateSchema(String query) {
        if (query != null) {
            if (this.eventTable.get() != null) {
                String table = this.eventTable.get().getName();

                if ((this.queryType.get() == QueryAugmentedEventDataType.DDL_TABLE ||
                     this.queryType.get() == QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE) &&
                     this.getQueryOperationType() != QueryAugmentedEventDataOperationType.CREATE) {
                    this.createTableBefore.set(this.manager.getCreateTable(table));
                } else {
                    this.createTableBefore.set(null);
                }

                this.manager.execute(table, query);

                if ((this.queryType.get() == QueryAugmentedEventDataType.DDL_TABLE ||
                     this.queryType.get() == QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE) &&
                     this.getQueryOperationType() != QueryAugmentedEventDataOperationType.DROP) {
                    this.createTableAfter.set(this.manager.getCreateTable(table));
                } else {
                    this.createTableAfter.set(null);
                }
            } else {
                this.manager.execute(null, query);
            }
        }
    }

    private boolean excludeTable(String tableName) {
        return this.excludeTableList.contains(tableName);
    }

    public void updateContext(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        this.updateServer(eventHeader.getServerId());

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

                if (this.beginPattern.matcher(query).find()) {
                    this.updateCommons(
                            false,
                            QueryAugmentedEventDataType.BEGIN,
                            null,
                            queryRawEventData.getDatabase(),
                            null
                    );

                    if (!this.transaction.begin()) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already started");
                    }
                } else if (this.commitPattern.matcher(query).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.COMMIT,
                            null,
                            queryRawEventData.getDatabase(),
                            null
                    );

                    if (!this.transaction.commit(eventHeader.getTimestamp())) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already committed");
                    }
                }
                else if ((matcher = this.ddlDefinerPattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_DEFINER,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            queryRawEventData.getDatabase(),
                            null
                    );
                } else if ((matcher = this.ddlTablePattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_TABLE,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            queryRawEventData.getDatabase(),
                            matcher.group(4)
                    );
                } else if ((matcher = this.ddlTemporaryTablePattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            queryRawEventData.getDatabase(),
                            matcher.group(4)
                    );
                } else if ((matcher = this.ddlViewPattern.matcher(query)).find()) {
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
                } else if ((matcher = this.pseudoGTIDPattern.matcher(query)).find()) {
                    this.updateCommons(
                            false,
                            QueryAugmentedEventDataType.PSEUDO_GTID,
                            null,
                            queryRawEventData.getDatabase(),
                            null
                    );

                    this.updatePseudoGTID(matcher.group(0), 0);
                } else {
                    query = null;

                    this.updateCommons(
                            false,
                            null,
                            null,
                            queryRawEventData.getDatabase(),
                            null
                    );
                }

                this.updateSchema(query);

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
                    ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already committed");
                }

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

                this.tableIdEventTableMap.put(
                        tableMapRawEventData.getTableId(),
                        new AugmentedEventTable(
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
                AugmentedEventTable eventTable = this.getEventTable(tableIdRawEventData.getTableId());

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

    public CurrentTransaction getTransaction() {
        return this.transaction;
    }

    public boolean process() {
        return this.continueFlag.get();
    }

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType.get();
    }

    public QueryAugmentedEventDataOperationType getQueryOperationType() {
        return this.queryOperationType.get();
    }

    public AugmentedEventTable getEventTable() {
        return this.eventTable.get();
    }

    public Checkpoint getCheckpoint() {
        return new Checkpoint(
                this.serverId.get(),
                this.binlogFilename.get(),
                this.binlogPosition.get(),
                this.pseudoGTIDValue.get(),
                this.pseudoGTIDIndex.getAndIncrement()
        );
    }

    public String getCreateTableBefore() {
        return this.createTableBefore.get();
    }

    public String getCreateTableAfter() {
        return this.createTableAfter.get();
    }

    public AugmentedEventTable getEventTable(long tableId) {
        return this.tableIdEventTableMap.get(tableId);
    }

    public List<AugmentedEventColumn> getColumns(long tableId, BitSet includedColumns) {
        AugmentedEventTable eventTable = this.getEventTable(tableId);

        if (eventTable != null) {
            List<AugmentedEventColumn> columnList = this.manager.listColumns(eventTable.getName());

            if (columnList != null) {
                List<AugmentedEventColumn> includedColumnList = new LinkedList<>();

                for (int columnIndex = 0; columnIndex < columnList.size(); columnIndex++) {
                    if (includedColumns.get(columnIndex)) {
                        includedColumnList.add(columnList.get(columnIndex));
                    }
                }

                return includedColumnList;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public List<AugmentedEventUpdatedRow> getUpdatedRows(List<Map.Entry<Serializable[], Serializable[]>> rawRows) {
        List<AugmentedEventUpdatedRow> rows = new ArrayList<>();

        for (Map.Entry<Serializable[], Serializable[]> rawRow : rawRows) {
            rows.add(new AugmentedEventUpdatedRow(
                    rawRow.getKey(),
                    rawRow.getValue()
            ));
        }

        return rows;
    }

    @Override
    public void close() throws IOException {
        this.manager.close();
    }
}
