package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventColumn;
import com.booking.replication.augmenter.model.AugmentedEventTable;
import com.booking.replication.augmenter.model.QueryAugmentedEventDataOperationType;
import com.booking.replication.augmenter.model.QueryAugmentedEventDataType;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.RotateRawEventData;
import com.booking.replication.supplier.model.TableMapRawEventData;
import com.booking.replication.supplier.model.XIDRawEventData;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
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
        String MYSQL_TRANSACTION_LIMIT = "agumenter.context.transaction.limit";
        String MYSQL_BEGIN_PATTERN = "augmenter.context.pattern.begin";
        String MYSQL_COMMIT_PATTERN = "augmenter.context.pattern.commit";
        String MYSQL_DDL_DEFINER_PATTERN = "augmenter.context.pattern.ddl.definer";
        String MYSQL_DDL_TABLE_PATTERN = "augmenter.context.pattern.ddl.table";
        String MYSQL_DDL_TEMPORARY_TABLE_PATTERN = "augmenter.context.pattern.ddl.temporary.table";
        String MYSQL_DDL_VIEW_PATTERN = "augmenter.context.pattern.ddl.view";
        String MYSQL_DDL_ANALYZE_PATTERN = "augmenter.context.pattern.ddl.analyze";
        String MYSQL_PSEUDO_GTID_PATTERN = "augmenter.context.pattern.pseudogtid";
    }

    private static final Logger LOG = Logger.getLogger(ActiveSchemaContext.class.getName());

    private static final int DEFAULT_MYSQL_TRANSACTION_LIMIT = 1000;
    private static final String DEFAULT_MYSQL_BEGIN_PATTERN = "^(/\\*.*?\\*/\\s*)?(begin)";
    private static final String DEFAULT_MYSQL_COMMIT_PATTERN = "^(/\\*.*?\\*/\\s*)?(commit)";
    private static final String DEFAULT_MYSQL_DDL_DEFINER_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(definer)\\s*=";
    private static final String DEFAULT_MYSQL_DDL_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_MYSQL_DDL_TEMPORARY_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(temporary)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_MYSQL_DDL_VIEW_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(view)\\s+(\\S+)";
    private static final String DEFAULT_MYSQL_DDL_ANALYZE_PATTERN = "^(/\\*.*?\\*/\\s*)?(analyze)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_MYSQL_PSEUDO_GTID_PATTERN = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";

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

    private final AtomicLong serverId;

    private final AtomicBoolean dataFlag;
    private final AtomicReference<QueryAugmentedEventDataType> queryType;
    private final AtomicReference<QueryAugmentedEventDataOperationType> queryOperationType;
    private final AtomicReference<AugmentedEventTable> table;

    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;

    private final AtomicReference<String> pseudoGTIDValue;
    private final AtomicInteger pseudoGTIDIndex;

    private final AtomicReference<String> createTableBefore;
    private final AtomicReference<String> createTableAfter;

    private final Map<Long, AugmentedEventTable> tableIdTableMap;

    public ActiveSchemaContext(Map<String, String> configuration) {
        this.transaction = new CurrentTransaction(Integer.parseInt(configuration.getOrDefault(Configuration.MYSQL_TRANSACTION_LIMIT, String.valueOf(ActiveSchemaContext.DEFAULT_MYSQL_TRANSACTION_LIMIT))));
        this.manager = new ActiveSchemaManager(configuration);
        this.beginPattern = this.getPattern(configuration, Configuration.MYSQL_BEGIN_PATTERN, ActiveSchemaContext.DEFAULT_MYSQL_BEGIN_PATTERN);
        this.commitPattern = this.getPattern(configuration, Configuration.MYSQL_COMMIT_PATTERN, ActiveSchemaContext.DEFAULT_MYSQL_COMMIT_PATTERN);
        this.ddlDefinerPattern = this.getPattern(configuration, Configuration.MYSQL_DDL_DEFINER_PATTERN, ActiveSchemaContext.DEFAULT_MYSQL_DDL_DEFINER_PATTERN);
        this.ddlTablePattern = this.getPattern(configuration, Configuration.MYSQL_DDL_TABLE_PATTERN, ActiveSchemaContext.DEFAULT_MYSQL_DDL_TABLE_PATTERN);
        this.ddlTemporaryTablePattern = this.getPattern(configuration, Configuration.MYSQL_DDL_TEMPORARY_TABLE_PATTERN, ActiveSchemaContext.DEFAULT_MYSQL_DDL_TEMPORARY_TABLE_PATTERN);
        this.ddlViewPattern = this.getPattern(configuration, Configuration.MYSQL_DDL_VIEW_PATTERN, ActiveSchemaContext.DEFAULT_MYSQL_DDL_VIEW_PATTERN);
        this.ddlAnalyzePattern = this.getPattern(configuration, Configuration.MYSQL_DDL_ANALYZE_PATTERN, ActiveSchemaContext.DEFAULT_MYSQL_DDL_ANALYZE_PATTERN);
        this.pseudoGTIDPattern = this.getPattern(configuration, Configuration.MYSQL_PSEUDO_GTID_PATTERN, ActiveSchemaContext.DEFAULT_MYSQL_PSEUDO_GTID_PATTERN);

        this.serverId = new AtomicLong();

        this.dataFlag = new AtomicBoolean();
        this.queryType = new AtomicReference<>(QueryAugmentedEventDataType.UNKNOWN);
        this.queryOperationType = new AtomicReference<>(QueryAugmentedEventDataOperationType.UNKNOWN);
        this.table = new AtomicReference<>();

        this.binlogFilename = new AtomicReference<>();
        this.binlogPosition = new AtomicLong();

        this.pseudoGTIDValue = new AtomicReference<>();
        this.pseudoGTIDIndex = new AtomicInteger();

        this.createTableBefore = new AtomicReference<>();
        this.createTableAfter = new AtomicReference<>();

        this.tableIdTableMap = new ConcurrentHashMap<>();
    }

    private Pattern getPattern(Map<String, String> configuration, String configurationPath, String configurationDefault) {
        return Pattern.compile(
                configuration.getOrDefault(
                        configurationPath,
                        configurationDefault
                ),
                Pattern.CASE_INSENSITIVE
        );
    }

    private void updateServer(long id) {
        this.serverId.set(id);
    }

    private void updateCommons(boolean dataFlag, QueryAugmentedEventDataType queryType, QueryAugmentedEventDataOperationType queryOperationType, AugmentedEventTable table) {
        this.dataFlag.set(dataFlag);
        this.queryType.set(queryType);
        this.queryOperationType.set(queryOperationType);
        this.table.set(table);
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
            if (this.table.get() != null) {
                String tableName = this.table.get().getName();

                if ((this.queryType.get() == QueryAugmentedEventDataType.DDL_TABLE ||
                     this.queryType.get() == QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE) &&
                     this.getQueryOperationType() != QueryAugmentedEventDataOperationType.CREATE) {
                    this.createTableBefore.set(this.manager.getCreateTable(tableName));
                } else {
                    this.createTableBefore.set(null);
                }

                this.manager.execute(tableName, query);

                if ((this.queryType.get() == QueryAugmentedEventDataType.DDL_TABLE ||
                     this.queryType.get() == QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE) &&
                     this.getQueryOperationType() != QueryAugmentedEventDataOperationType.DROP) {
                    this.createTableAfter.set(this.manager.getCreateTable(tableName));
                } else {
                    this.createTableAfter.set(null);
                }
            } else {
                this.manager.execute(null, query);
            }
        }
    }

    public void updateContext(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        this.updateServer(eventHeader.getServerId());

        switch (eventHeader.getEventType()) {
            case ROTATE:
                this.updateCommons(
                        false,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
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
                            QueryAugmentedEventDataOperationType.UNKNOWN,
                            null
                    );

                    if (!this.transaction.begin()) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already started");
                    }
                } else if (this.commitPattern.matcher(query).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.COMMIT,
                            QueryAugmentedEventDataOperationType.UNKNOWN,
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
                            null
                    );
                } else if ((matcher = this.ddlTablePattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_TABLE,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            new AugmentedEventTable(queryRawEventData.getDatabase(), matcher.group(4))
                    );
                } else if ((matcher = this.ddlTemporaryTablePattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            new AugmentedEventTable(queryRawEventData.getDatabase(), matcher.group(4))
                    );
                } else if ((matcher = this.ddlViewPattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_VIEW,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            null
                    );
                } else if ((matcher = this.ddlAnalyzePattern.matcher(query)).find()) {
                    this.updateCommons(
                            true,
                            QueryAugmentedEventDataType.DDL_ANALYZE,
                            QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                            null
                    );
                } else if ((matcher = this.pseudoGTIDPattern.matcher(query)).find()) {
                    this.updateCommons(
                            false,
                            QueryAugmentedEventDataType.PSEUDO_GTID,
                            QueryAugmentedEventDataOperationType.UNKNOWN,
                            null
                    );

                    this.updatePseudoGTID(matcher.group(0), 0);
                } else {
                    query = null;

                    this.updateCommons(
                            false,
                            QueryAugmentedEventDataType.UNKNOWN,
                            QueryAugmentedEventDataOperationType.UNKNOWN,
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
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                if (!this.transaction.commit(xidRawEventData.getXID(),eventHeader.getTimestamp())) {
                    ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already committed");
                }

                break;
            case TABLE_MAP:
                this.updateCommons(
                        false,
                        QueryAugmentedEventDataType.COMMIT,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                TableMapRawEventData tableMapRawEventData = TableMapRawEventData.class.cast(eventData);

                this.tableIdTableMap.put(
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
                this.updateCommons(
                        true,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                break;
            default:
                this.updateCommons(
                        false,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                break;
        }
    }

    public CurrentTransaction getTransaction() {
        return this.transaction;
    }

    public boolean hasData() {
        return this.dataFlag.get();
    }

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType.get();
    }

    public QueryAugmentedEventDataOperationType getQueryOperationType() {
        return this.queryOperationType.get();
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

    public List<AugmentedEventColumn> getColumns(long tableId, BitSet includedColumns) {
        List<AugmentedEventColumn> columnList = this.manager.listColumns(this.getTable(tableId).getName());
        List<AugmentedEventColumn> includedColumnList = new LinkedList<>();

        for (int columnIndex = 0; columnIndex < columnList.size(); columnIndex++) {
            if (includedColumns.get(columnIndex)) {
                includedColumnList.add(columnList.get(columnIndex));
            }
        }

        return includedColumnList;
    }

    public AugmentedEventTable getTable() {
        return this.table.get();
    }

    public AugmentedEventTable getTable(long tableId) {
        return this.tableIdTableMap.get(tableId);
    }

    @Override
    public void close() throws IOException {
        this.manager.close();
    }
}
