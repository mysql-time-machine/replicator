package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventTable;
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
        String TRANSACTION_LIMIT = "agumenter.context.transaction.limit";
        String BEGIN_PATTERN = "augmenter.context.pattern.begin";
        String COMMIT_PATTERN = "augmenter.context.pattern.commit";
        String DDL_DEFINER_PATTERN = "augmenter.context.pattern.ddl.definer";
        String DDL_TABLE_PATTERN = "augmenter.context.pattern.ddl.table";
        String DDL_TEMPORARY_TABLE_PATTERN = "augmenter.context.pattern.ddl.temporary.table";
        String DDL_VIEW_PATTERN = "augmenter.context.pattern.ddl.view";
        String DDL_ANALYZE_PATTERN = "augmenter.context.pattern.ddl.analyze";
        String PSEUDO_GTID_PATTERN = "augmenter.context.pattern.pseudogtid";
    }

    private static final Logger LOG = Logger.getLogger(ActiveSchemaContext.class.getName());

    private static final int DEFAULT_TRANSACTION_LIMIT = 1000;
    private static final String DEFAULT_BEGIN_PATTERN = "^(/\\*.*?\\*/\\s*)?(begin)";
    private static final String DEFAULT_COMMIT_PATTERN = "^(/\\*.*?\\*/\\s*)?(commit)";
    private static final String DEFAULT_DDL_DEFINER_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+definer\\s*=";
    private static final String DEFAULT_DDL_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(table)";
    private static final String DEFAULT_DDL_TEMPORARY_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+temporary\\s+(table)";
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
    private final AtomicBoolean beginFlag;
    private final AtomicBoolean commitFlag;
    private final AtomicBoolean ddlDefinerFlag;
    private final AtomicBoolean ddlTableFlag;
    private final AtomicBoolean ddlTemporaryTableFlag;
    private final AtomicBoolean ddlViewFlag;
    private final AtomicBoolean ddlAnalyzeFlag;
    private final AtomicBoolean pseudoGTIDFlag;

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
        this.beginFlag = new AtomicBoolean();
        this.commitFlag = new AtomicBoolean();
        this.ddlDefinerFlag = new AtomicBoolean();
        this.ddlTableFlag = new AtomicBoolean();
        this.ddlTemporaryTableFlag = new AtomicBoolean();
        this.ddlViewFlag = new AtomicBoolean();
        this.ddlAnalyzeFlag = new AtomicBoolean();
        this.pseudoGTIDFlag = new AtomicBoolean();

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
        this.cleanFlags();

        this.serverId.set(eventHeader.getServerId());

        switch (eventHeader.getEventType()) {
            case ROTATE:
                RotateRawEventData rotateRawEventData = RotateRawEventData.class.cast(eventData);

                this.binlogFilename.set(rotateRawEventData.getBinlogFilename());
                this.binlogPosition.set(rotateRawEventData.getBinlogPosition());

                break;
            case QUERY:
                String query = QueryRawEventData.class.cast(eventData).getSQL();

                if (this.beginPattern.matcher(query).find()) {
                    this.beginFlag.set(true);

                    if (!this.transaction.begin()) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already started");
                    }

                    break;
                }

                if (this.commitPattern.matcher(query).find()) {
                    this.commitFlag.set(true);

                    if (!this.transaction.commit(eventHeader.getTimestamp())) {
                        ActiveSchemaContext.LOG.log(Level.WARNING, "transaction already committed");
                    }

                    break;
                }

                if (this.ddlDefinerPattern.matcher(query).find()) {
                    this.ddlDefinerFlag.set(true);

                    break;
                }

                if (this.ddlTablePattern.matcher(query).find()) {
                    this.ddlTableFlag.set(true);

                    break;
                }

                if (this.ddlTemporaryTablePattern.matcher(query).find()) {
                    this.ddlTemporaryTableFlag.set(true);

                    break;
                }

                if (this.ddlViewPattern.matcher(query).find()) {
                    this.ddlViewFlag.set(true);

                    break;
                }

                if (this.ddlAnalyzePattern.matcher(query).find()) {
                    this.ddlAnalyzeFlag.set(true);

                    break;
                }

                Matcher pseudoGTIDMatcher = this.pseudoGTIDPattern.matcher(query);

                if (pseudoGTIDMatcher.find() && pseudoGTIDMatcher.groupCount() == 1) {
                    this.pseudoGTIDFlag.set(true);

                    this.pseudoGTID.set(pseudoGTIDMatcher.group(0));
                    this.pseudoGTIDIndex.set(0);

                    break;
                }

                this.dataFlag.set(true);

                break;
            case TABLE_MAP:
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

                break;
        }
    }

    private void cleanFlags() {
        this.dataFlag.set(false);
        this.beginFlag.set(false);
        this.commitFlag.set(false);
        this.ddlDefinerFlag.set(false);
        this.ddlTableFlag.set(false);
        this.ddlTemporaryTableFlag.set(false);
        this.ddlViewFlag.set(false);
        this.ddlAnalyzeFlag.set(false);
        this.pseudoGTIDFlag.set(false);
    }

    public ActiveSchemaTransaction getTransaction() {
        return this.transaction;
    }

    public boolean hasData() {
        return this.dataFlag.get();
    }

    public boolean hasBegin() {
        return this.beginFlag.get();
    }

    public boolean hasCommit() {
        return this.commitFlag.get();
    }

    public boolean hasDDL() {
        return this.hasDDLDefiner() || this.hasDDLTable() || this.hasDDLTemporaryTable() || this.hasDDLView() || this.hasDDLAnalyze();
    }

    public boolean hasDDLDefiner() {
        return this.ddlDefinerFlag.get();
    }

    public boolean hasDDLTable() {
        return this.ddlTableFlag.get();
    }

    public boolean hasDDLTemporaryTable() {
        return this.ddlTemporaryTableFlag.get();
    }

    public boolean hasDDLView() {
        return this.ddlViewFlag.get();
    }

    public boolean hasDDLAnalyze() {
        return this.ddlAnalyzeFlag.get();
    }

    public boolean hasPseudoGTID() {
        return this.pseudoGTIDFlag.get();
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
