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
    private static final String DEFAULT_DDL_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(table)";
    private static final String DEFAULT_DDL_TEMPORARY_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(temporary)\\s+(table)";
    private static final String DEFAULT_DDL_VIEW_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(view)";
    private static final String DEFAULT_DDL_ANALYZE_PATTERN = "^(/\\*.*?\\*/\\s*)?(analyze)\\s+(table)";
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
    private final AtomicReference<QueryAugmentedEventDataType> queryType;
    private final AtomicReference<QueryAugmentedEventDataOperationType> operationType;

    private final AtomicLong serverId;
    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;
    private final AtomicReference<String> pseudoGTID;
    private final AtomicInteger pseudoGTIDIndex;
    private final Map<Long, AugmentedEventTable> tableIdTableMap;

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
        this.queryType = new AtomicReference<>(QueryAugmentedEventDataType.UNKNOWN);
        this.operationType = new AtomicReference<>(QueryAugmentedEventDataOperationType.UNKNOWN);

        this.serverId = new AtomicLong();
        this.binlogFilename = new AtomicReference<>();
        this.binlogPosition = new AtomicLong();
        this.pseudoGTID = new AtomicReference<>();
        this.pseudoGTIDIndex = new AtomicInteger();
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

    public void updateContext(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        this.serverId.set(eventHeader.getServerId());

        switch (eventHeader.getEventType()) {
            case ROTATE:
                this.dataFlag.set(false);
                this.queryFlag.set(false);
                this.queryType.set(QueryAugmentedEventDataType.UNKNOWN);
                this.operationType.set(QueryAugmentedEventDataOperationType.UNKNOWN);

                RotateRawEventData rotateRawEventData = RotateRawEventData.class.cast(eventData);

                this.binlogFilename.set(rotateRawEventData.getBinlogFilename());
                this.binlogPosition.set(rotateRawEventData.getBinlogPosition());

                break;
            case QUERY:
                String query = QueryRawEventData.class.cast(eventData).getSQL();

                if (this.beginPattern.matcher(query).find()) {
                    this.dataFlag.set(false);
                    this.queryFlag.set(true);
                    this.queryType.set(QueryAugmentedEventDataType.BEGIN);
                    this.operationType.set(QueryAugmentedEventDataOperationType.UNKNOWN);

                    if (!this.transaction.begin()) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already started");
                    }

                    break;
                }

                if (this.commitPattern.matcher(query).find()) {
                    this.dataFlag.set(false);
                    this.queryFlag.set(true);
                    this.queryType.set(QueryAugmentedEventDataType.COMMIT);
                    this.operationType.set(QueryAugmentedEventDataOperationType.UNKNOWN);

                    if (!this.transaction.commit(eventHeader.getTimestamp())) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already committed");
                    }

                    break;
                }

                Matcher ddlDefinerMatcher = this.ddlDefinerPattern.matcher(query);

                if (ddlDefinerMatcher.find()) {
                    this.dataFlag.set(true);
                    this.queryFlag.set(true);
                    this.queryType.set(QueryAugmentedEventDataType.DDL_DEFINER);
                    this.operationType.set(QueryAugmentedEventDataOperationType.valueOf(ddlDefinerMatcher.group(2).toUpperCase()));

                    break;
                }

                Matcher ddlTableMatcher = this.ddlTablePattern.matcher(query);

                if (ddlTableMatcher.find()) {
                    this.dataFlag.set(true);
                    this.queryFlag.set(true);
                    this.queryType.set(QueryAugmentedEventDataType.DDL_TABLE);
                    this.operationType.set(QueryAugmentedEventDataOperationType.valueOf(ddlTableMatcher.group(2).toUpperCase()));

                    break;
                }

                Matcher ddlTemporaryTableMatcher = this.ddlTemporaryTablePattern.matcher(query);

                if (ddlTemporaryTableMatcher.find()) {
                    this.dataFlag.set(true);
                    this.queryFlag.set(true);
                    this.queryType.set(QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE);
                    this.operationType.set(QueryAugmentedEventDataOperationType.valueOf(ddlTemporaryTableMatcher.group(2).toUpperCase()));

                    break;
                }

                Matcher ddlViewMatcher = this.ddlViewPattern.matcher(query);

                if (ddlViewMatcher.find()) {
                    this.dataFlag.set(true);
                    this.queryFlag.set(true);
                    this.queryType.set(QueryAugmentedEventDataType.DDL_VIEW);
                    this.operationType.set(QueryAugmentedEventDataOperationType.valueOf(ddlViewMatcher.group(2).toUpperCase()));

                    break;
                }

                Matcher ddlAnalyze = this.ddlAnalyzePattern.matcher(query);

                if (ddlAnalyze.find()) {
                    this.dataFlag.set(true);
                    this.queryFlag.set(true);
                    this.queryType.set(QueryAugmentedEventDataType.DDL_ANALYZE);
                    this.operationType.set(QueryAugmentedEventDataOperationType.valueOf(ddlAnalyze.group(2).toUpperCase()));

                    break;
                }

                Matcher pseudoGTIDMatcher = this.pseudoGTIDPattern.matcher(query);

                if (pseudoGTIDMatcher.find()) {
                    this.dataFlag.set(false);
                    this.queryFlag.set(true);
                    this.queryType.set(QueryAugmentedEventDataType.PSEUDO_GTID);

                    this.pseudoGTID.set(pseudoGTIDMatcher.group(0));
                    this.pseudoGTIDIndex.set(0);

                    break;
                }

                this.dataFlag.set(true);
                this.queryFlag.set(false);
                this.queryType.set(QueryAugmentedEventDataType.UNKNOWN);

                break;
            case XID:
                this.dataFlag.set(false);
                this.queryFlag.set(true);
                this.queryType.set(QueryAugmentedEventDataType.COMMIT);

                if (!this.transaction.commit(eventHeader.getTimestamp())) {
                    ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already committed");
                }

                break;
            case TABLE_MAP:
                this.dataFlag.set(false);
                this.queryFlag.set(false);
                this.queryType.set(QueryAugmentedEventDataType.UNKNOWN);

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
                this.dataFlag.set(true);
                this.queryFlag.set(false);
                this.queryType.set(QueryAugmentedEventDataType.UNKNOWN);

                break;
            default:
                this.dataFlag.set(false);
                this.queryFlag.set(false);
                this.queryType.set(QueryAugmentedEventDataType.UNKNOWN);

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

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType.get();
    }

    public QueryAugmentedEventDataOperationType getOperationType() {
        return this.operationType.get();
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

    public AugmentedEventTable getTableName(long tableId) {
        return this.tableIdTableMap.get(tableId);
    }
}
