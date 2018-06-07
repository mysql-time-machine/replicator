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
import com.booking.replication.supplier.model.XIDRawEventData;

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
    private final AtomicReference<String> queryContent;
    private final AtomicReference<QueryAugmentedEventDataType> queryType;
    private final AtomicReference<QueryAugmentedEventDataOperationType> queryOperationType;
    private final AtomicReference<AugmentedEventTable> table;

    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;

    private final AtomicReference<String> pseudoGTIDValue;
    private final AtomicInteger pseudoGTIDIndex;

    private final Map<Long, AugmentedEventTable> tableIdTableMap;

    public ActiveSchemaContext(Map<String, String> configuration) {
        this.transaction = new CurrentTransaction(Integer.parseInt(configuration.getOrDefault(Configuration.MYSQL_TRANSACTION_LIMIT, String.valueOf(ActiveSchemaContext.DEFAULT_MYSQL_TRANSACTION_LIMIT))));
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
        this.queryContent = new AtomicReference<>();
        this.queryType = new AtomicReference<>(QueryAugmentedEventDataType.UNKNOWN);
        this.queryOperationType = new AtomicReference<>(QueryAugmentedEventDataOperationType.UNKNOWN);
        this.table = new AtomicReference<>();

        this.binlogFilename = new AtomicReference<>();
        this.binlogPosition = new AtomicLong();

        this.pseudoGTIDValue = new AtomicReference<>();
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

    private void updateServer(long id) {
        this.serverId.set(id);
    }

    private void updateCommons(boolean dataFlag, String queryContent, QueryAugmentedEventDataType queryType, QueryAugmentedEventDataOperationType queryOperationType, AugmentedEventTable table) {
        this.dataFlag.set(dataFlag);
        this.queryContent.set(queryContent);
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

    public void updateContext(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        this.updateServer(eventHeader.getServerId());

        switch (eventHeader.getEventType()) {
            case ROTATE:
                this.updateCommons(
                        false,
                        null,
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

                if (this.beginPattern.matcher(query).find()) {
                    this.updateCommons(
                            false,
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
                    this.updateCommons(
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
                    this.updateCommons(
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
                    this.updateCommons(
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
                    this.updateCommons(
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
                    this.updateCommons(
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
                    this.updateCommons(
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
                    this.updateCommons(
                            false,
                            query,
                            QueryAugmentedEventDataType.PSEUDO_GTID,
                            QueryAugmentedEventDataOperationType.UNKNOWN,
                            null
                    );

                    this.updatePseudoGTID(pseudoGTIDMatcher.group(0), 0);

                    break;
                }

                this.updateCommons(
                        false,
                        null,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                break;
            case XID:
                XIDRawEventData xidRawEventData = XIDRawEventData.class.cast(eventData);

                this.updateCommons(
                        true,
                        "COMMIT",
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
                this.updateCommons(
                        true,
                        null,
                        QueryAugmentedEventDataType.UNKNOWN,
                        QueryAugmentedEventDataOperationType.UNKNOWN,
                        null
                );

                break;
            default:
                this.updateCommons(
                        false,
                        null,
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
                this.pseudoGTIDValue.get(),
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
