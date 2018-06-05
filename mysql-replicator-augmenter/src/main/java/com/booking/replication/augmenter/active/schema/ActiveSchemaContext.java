package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventTable;
import com.booking.replication.augmenter.model.QueryAugmentedEventDataOperationType;
import com.booking.replication.augmenter.model.QueryAugmentedEventDataType;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.RotateRawEventData;
import com.booking.replication.supplier.model.TableMapRawEventData;

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

public class ActiveSchemaContext {
    public interface Configuration {
        String APPLY_UUID = "augmenter.context.apply.uuid";
        String APPLY_XID = "augmenter.context.apply.xid";
        String TRANSACTION_LIMIT = "agumenter.context.transaction.limit";
        String BEGIN_PATTERN = "augmenter.context.pattern.begin";
        String COMMIT_PATTERN = "augmenter.context.pattern.commit";
        String DDL_DEFINER_PATTERN = "augmenter.context.pattern.ddl.definer";
        String DDL_TABLE_PATTERN = "augmenter.context.pattern.ddl.table";
        String DDL_TEMPORARY_TABLE_PATTERN = "augmenter.context.pattern.ddl.temporary.table";
        String DDL_VIEW_PATTERN = "augmenter.context.pattern.ddl.view";
        String DDL_ANALYZE_PATTERN = "augmenter.context.pattern.ddl.analyze";
        String PSEUDO_GTID_PATTERN = "augmenter.context.pattern.pseudogtid";
        String UUID_FIELD_NAME = "_replicator_uuid";
        String XID_FIELD_NAME = "_replicator_xid";
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

    private final ActiveSchemaTransaction transaction;

    private final Pattern beginPattern;
    private final Pattern commitPattern;
    private final Pattern ddlDefinerPattern;
    private final Pattern ddlTablePattern;
    private final Pattern ddlTemporaryTablePattern;
    private final Pattern ddlViewPattern;
    private final Pattern ddlAnalyzePattern;
    private final Pattern pseudoGTIDPattern;

    private final AtomicBoolean dataFlag;
    private final AtomicBoolean queryFlag;
    private final AtomicReference<String> queryContent;
    private final AtomicReference<QueryAugmentedEventDataType> queryType;
    private final AtomicReference<QueryAugmentedEventDataOperationType> queryOperationType;
    private final AtomicReference<AugmentedEventTable> table;
    private final Map<Long, AugmentedEventTable> tableIdTableMap;

    private final AtomicLong serverId;
    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;
    private final AtomicReference<String> pseudoGTID;
    private final AtomicInteger pseudoGTIDIndex;

    public ActiveSchemaContext(Map<String, String> configuration) {
        this.transaction = new ActiveSchemaTransaction(Integer.parseInt(configuration.getOrDefault(Configuration.TRANSACTION_LIMIT, String.valueOf(ActiveSchemaContext.DEFAULT_TRANSACTION_LIMIT))));
        this.beginPattern = this.getPattern(configuration, Configuration.BEGIN_PATTERN, ActiveSchemaContext.DEFAULT_BEGIN_PATTERN);
        this.commitPattern = this.getPattern(configuration, Configuration.COMMIT_PATTERN, ActiveSchemaContext.DEFAULT_COMMIT_PATTERN);
        this.ddlDefinerPattern = this.getPattern(configuration, Configuration.DDL_DEFINER_PATTERN, ActiveSchemaContext.DEFAULT_DDL_DEFINER_PATTERN);
        this.ddlTablePattern = this.getPattern(configuration, Configuration.DDL_TABLE_PATTERN, ActiveSchemaContext.DEFAULT_DDL_TABLE_PATTERN);
        this.ddlTemporaryTablePattern = this.getPattern(configuration, Configuration.DDL_TEMPORARY_TABLE_PATTERN, ActiveSchemaContext.DEFAULT_DDL_TEMPORARY_TABLE_PATTERN);
        this.ddlViewPattern = this.getPattern(configuration, Configuration.DDL_VIEW_PATTERN, ActiveSchemaContext.DEFAULT_DDL_VIEW_PATTERN);
        this.ddlAnalyzePattern = this.getPattern(configuration, Configuration.DDL_ANALYZE_PATTERN, ActiveSchemaContext.DEFAULT_DDL_ANALYZE_PATTERN);
        this.pseudoGTIDPattern = this.getPattern(configuration, Configuration.PSEUDO_GTID_PATTERN, ActiveSchemaContext.DEFAULT_PSEUDO_GTID_PATTERN);

        this.dataFlag = new AtomicBoolean();
        this.queryFlag = new AtomicBoolean();
        this.queryContent = new AtomicReference<>();
        this.queryType = new AtomicReference<>(QueryAugmentedEventDataType.UNKNOWN);
        this.queryOperationType = new AtomicReference<>(QueryAugmentedEventDataOperationType.UNKNOWN);
        this.table = new AtomicReference<>();

        this.tableIdTableMap = new ConcurrentHashMap<>();

        this.serverId = new AtomicLong();
        this.binlogFilename = new AtomicReference<>();
        this.binlogPosition = new AtomicLong();
        this.pseudoGTID = new AtomicReference<>();
        this.pseudoGTIDIndex = new AtomicInteger();
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

    private void updateContex(boolean dataFlag, boolean queryFlag, String queryContent, QueryAugmentedEventDataType queryType, QueryAugmentedEventDataOperationType queryOperationType, AugmentedEventTable table) {
        this.dataFlag.set(dataFlag);
        this.queryFlag.set(queryFlag);
        this.queryContent.set(queryContent);
        this.queryType.set(queryType);
        this.queryOperationType.set(queryOperationType);
        this.table.set(table);
    }

    public void updateContext(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        this.serverId.set(eventHeader.getServerId());

        switch (eventHeader.getEventType()) {
            case ROTATE:
                this.updateContex(
                        false,
                        false,
                        null,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                RotateRawEventData rotateRawEventData = RotateRawEventData.class.cast(eventData);

                this.binlogFilename.set(rotateRawEventData.getBinlogFilename());
                this.binlogPosition.set(rotateRawEventData.getBinlogPosition());

                break;
            case QUERY:
                QueryRawEventData queryRawEventData = QueryRawEventData.class.cast(eventData);
                String query = queryRawEventData.getSQL();

                if (this.beginPattern.matcher(query).find()) {
                    this.updateContex(
                            false,
                            true,
                            query,
                            QueryAugmentedEventDataType.BEGIN,
                            QueryAugmentedEventDataOperationType.UNKNOWN,
                            null
                    );

                    if (!this.transaction.begin()) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already started");
                    }

                    break;
                }

                if (this.commitPattern.matcher(query).find()) {
                    this.updateContex(
                            false,
                            true,
                            query,
                            QueryAugmentedEventDataType.COMMIT,
                            QueryAugmentedEventDataOperationType.UNKNOWN,
                            null
                    );

                    if (!this.transaction.commit(eventHeader.getTimestamp())) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already committed");
                    }

                    break;
                }

                Matcher ddlDefinerMatcher = this.ddlDefinerPattern.matcher(query);

                if (ddlDefinerMatcher.find()) {
                    this.updateContex(
                            true,
                            true,
                            query,
                            QueryAugmentedEventDataType.DDL_DEFINER,
                            QueryAugmentedEventDataOperationType.valueOf(ddlDefinerMatcher.group(2).toUpperCase()),
                            null
                    );

                    break;
                }

                Matcher ddlTableMatcher = this.ddlTablePattern.matcher(query);

                if (ddlTableMatcher.find()) {
                    this.updateContex(
                            true,
                            true,
                            query,
                            QueryAugmentedEventDataType.DDL_TABLE,
                            QueryAugmentedEventDataOperationType.valueOf(ddlTableMatcher.group(2).toUpperCase()),
                            new AugmentedEventTable(queryRawEventData.getDatabase(), ddlTableMatcher.group(4))
                    );

                    break;
                }

                Matcher ddlTemporaryTableMatcher = this.ddlTemporaryTablePattern.matcher(query);

                if (ddlTemporaryTableMatcher.find()) {
                    this.updateContex(
                            true,
                            true,
                            query,
                            QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE,
                            QueryAugmentedEventDataOperationType.valueOf(ddlTemporaryTableMatcher.group(2).toUpperCase()),
                            new AugmentedEventTable(queryRawEventData.getDatabase(), ddlTemporaryTableMatcher.group(4))
                    );

                    break;
                }

                Matcher ddlViewMatcher = this.ddlViewPattern.matcher(query);

                if (ddlViewMatcher.find()) {
                    this.updateContex(
                            true,
                            true,
                            query,
                            QueryAugmentedEventDataType.DDL_VIEW,
                            QueryAugmentedEventDataOperationType.valueOf(ddlViewMatcher.group(2).toUpperCase()),
                            null
                    );

                    break;
                }

                Matcher ddlAnalyze = this.ddlAnalyzePattern.matcher(query);

                if (ddlAnalyze.find()) {
                    this.updateContex(
                            true,
                            true,
                            query,
                            QueryAugmentedEventDataType.DDL_ANALYZE,
                            QueryAugmentedEventDataOperationType.valueOf(ddlAnalyze.group(2).toUpperCase()),
                            null
                    );

                    break;
                }

                Matcher pseudoGTIDMatcher = this.pseudoGTIDPattern.matcher(query);

                if (pseudoGTIDMatcher.find()) {
                    this.updateContex(
                            false,
                            true,
                            query,
                            QueryAugmentedEventDataType.PSEUDO_GTID,
                            QueryAugmentedEventDataOperationType.UNKNOWN,
                            null
                    );

                    this.pseudoGTID.set(pseudoGTIDMatcher.group(0));
                    this.pseudoGTIDIndex.set(0);

                    break;
                }

                this.updateContex(
                        true,
                        true,
                        query,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                break;
            case XID:
                this.updateContex(
                        false,
                        true,
                        null,
                        QueryAugmentedEventDataType.COMMIT,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                if (!this.transaction.commit(eventHeader.getTimestamp())) {
                    ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already committed");
                }

                break;
            case TABLE_MAP:
                this.updateContex(
                        false,
                        false,
                        null,
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
                this.updateContex(
                        true,
                        false,
                        null,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                break;
            default:
                this.updateContex(
                        false,
                        false,
                        null,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                break;
        }
    }

    public ActiveSchemaTransaction getTransaction() {
        return this.transaction;
    }

    public boolean hasData() {
        return this.dataFlag.get();
    }

    public boolean hasQuery() {
        return this.queryFlag.get();
    }

    public String getQueryContent() {
        return this.queryContent.get();
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
                this.pseudoGTID.get(),
                this.pseudoGTIDIndex.getAndIncrement()
        );
    }

    public AugmentedEventTable getTable() {
        return this.table.get();
    }

    public AugmentedEventTable getTable(long tableId) {
        return this.tableIdTableMap.get(tableId);
    }
}
