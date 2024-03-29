package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventDataOperationType;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventDataType;
import com.booking.replication.augmenter.model.format.EventDeserializer;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.row.RowBeforeAfter;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.schema.SchemaAtPositionCache;
import com.booking.replication.augmenter.model.schema.SchemaSnapshot;
import com.booking.replication.augmenter.model.schema.SchemaTransitionSequence;
import com.booking.replication.augmenter.model.schema.TableSchema;
import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.GTID;
import com.booking.replication.commons.checkpoint.GTIDType;
import com.booking.replication.commons.metrics.Metrics;
import com.booking.replication.supplier.model.GTIDRawEventData;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.RawEventType;
import com.booking.replication.supplier.model.RotateRawEventData;
import com.booking.replication.supplier.model.TableIdRawEventData;
import com.booking.replication.supplier.model.TableMapRawEventData;
import com.booking.replication.supplier.model.XIDRawEventData;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;

import com.codahale.metrics.MetricRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AugmenterContext implements Closeable {

    public interface Configuration {
        String TRANSACTION_BUFFER_CLASS     = "augmenter.context.transaction.buffer.class";
        String TRANSACTION_BUFFER_LIMIT     = "augmenter.context.transaction.buffer.limit";
        String GTID_TYPE                    = "augmenter.context.gtid.type";
        String BEGIN_PATTERN                = "augmenter.context.pattern.begin";
        String COMMIT_PATTERN               = "augmenter.context.pattern.commit";
        String DDL_DEFINER_PATTERN          = "augmenter.context.pattern.ddl.definer";
        String DDL_TABLE_PATTERN            = "augmenter.context.pattern.ddl.table";
        String DDL_TEMPORARY_TABLE_PATTERN  = "augmenter.context.pattern.ddl.temporary.table";
        String DDL_VIEW_PATTERN             = "augmenter.context.pattern.ddl.view";
        String DDL_ANALYZE_PATTERN          = "augmenter.context.pattern.ddl.analyze";
        String ENUM_PATTERN                 = "augmenter.context.pattern.enum";
        String SET_PATTERN                  = "augmenter.context.pattern.set";
        String EXCLUDE_TABLE                = "augmenter.context.exclude.table";
        String EXCLUDE_PATTERN              = "augmenter.context.exclude.pattern";
        String INCLUDE_TABLE                = "augmenter.context.include.table";
        String TRANSACTIONS_ENABLED         = "augmenter.context.transactions.enabled";
    }

    private static final Logger LOG = LogManager.getLogger(AugmenterContext.class);

    private static final int DEFAULT_TRANSACTION_LIMIT = 1000;
    private static final String DEFAULT_GTID_TYPE = GTIDType.REAL.name();
    private static final String DEFAULT_BEGIN_PATTERN = "^(/\\*.*?\\*/\\s*)?(begin)";
    private static final String DEFAULT_COMMIT_PATTERN = "^(/\\*.*?\\*/\\s*)?(commit)";
    private static final String DEFAULT_DDL_DEFINER_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(definer)\\s*=";
    private static final String DEFAULT_DDL_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|modify)\\s+(table)\\s+(?:if (?:not )?exists\\s+)?(\\S+)";
    private static final String DEFAULT_DDL_TEMPORARY_TABLE_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(temporary)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_DDL_VIEW_PATTERN = "^(/\\*.*?\\*/\\s*)?(alter|drop|create|rename|truncate|modify)\\s+(view)\\s+(\\S+)";
    private static final String DEFAULT_DDL_ANALYZE_PATTERN = "^(/\\*.*?\\*/\\s*)?(analyze)\\s+(table)\\s+(\\S+)";
    private static final String DEFAULT_ENUM_PATTERN = "(?<=enum\\()(.*?)(?=\\))";
    private static final String DEFAULT_SET_PATTERN = "(?<=set\\()(.*?)(?=\\))";

    public static final String RENAME_MULTISCHEMA_PATTERN = "(`?\\S+`?\\.)?(`?\\S+`?)\\s+TO\\s+(`?\\S+`?\\.)?(`?\\S+`?)\\s*,?";

    private final SchemaManager schemaManager;
    private final String replicatedSchema;
    private final CurrentTransaction transaction;

    private final Pattern beginPattern;
    private final Pattern commitPattern;
    private final Pattern ddlDefinerPattern;
    private final Pattern ddlTablePattern;
    private final Pattern ddlTemporaryTablePattern;
    private final Pattern ddlViewPattern;
    private final Pattern ddlAnalyzePattern;
    private final Pattern enumPattern;
    private final Pattern setPattern;
    private final Pattern excludePattern;
    private final Pattern renameMultiSchemaPattern;
    private final boolean transactionsEnabled;
    private final Metrics<?> metrics;
    private final String binlogsBasePath;

    private final List<String> excludeTableList;

    // in case of intersection, the include list overrides the exclude list
    private final List<String> includeTableList;

    private volatile AtomicLong timestamp;
    private volatile AtomicLong previousTimestamp;
    private volatile AtomicLong transactionCounter;
    private final AtomicLong serverId;
    private final AtomicLong nextPosition;

    private final AtomicBoolean shouldAugmentFlag;
    private final AtomicReference<QueryAugmentedEventDataType> queryType;
    private final AtomicReference<QueryAugmentedEventDataOperationType> queryOperationType;
    private final AtomicReference<FullTableName> eventTable;

    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;

    private final GTIDType gtidType;
    private final AtomicReference<String> gtidValue;
    private final AtomicReference<Byte> gtidFlags;
    private final AtomicReference<String> gtidSet;

    private final AtomicReference<Collection<ColumnSchema>> columnsBefore;
    private final AtomicReference<String> createTableBefore;
    private final AtomicReference<Collection<ColumnSchema>> columnsAfter;
    private final AtomicReference<String> createTableAfter;
    private final AtomicReference<SchemaAtPositionCache> schemaCache;

    private final AtomicBoolean isAtDDL;
    private final AtomicReference<SchemaSnapshot> schemaSnapshot;

    public AugmenterContext(SchemaManager schemaManager, Map<String, Object> configuration) {

        this.schemaManager = schemaManager;

        this.schemaCache = new AtomicReference<>();
        this.schemaCache.set(new SchemaAtPositionCache());

        this.schemaSnapshot = new AtomicReference<>();
        this.metrics = Metrics.getInstance(configuration);
        this.binlogsBasePath = MetricRegistry.name(this.metrics.basePath(), "augmenter", "context", "events");

        transactionCounter = new AtomicLong();
        transactionCounter.set(0L);

        this.transaction = new CurrentTransaction(
                configuration.getOrDefault(
                        Configuration.TRANSACTION_BUFFER_CLASS,
                        ConcurrentLinkedQueue.class.getName()
                ).toString(),
                Integer.parseInt(
                        configuration.getOrDefault(
                                Configuration.TRANSACTION_BUFFER_LIMIT,
                                String.valueOf(AugmenterContext.DEFAULT_TRANSACTION_LIMIT)
                        ).toString()
                )
        );
        this.beginPattern = this.getPattern(configuration, Configuration.BEGIN_PATTERN, AugmenterContext.DEFAULT_BEGIN_PATTERN);
        this.commitPattern = this.getPattern(configuration, Configuration.COMMIT_PATTERN, AugmenterContext.DEFAULT_COMMIT_PATTERN);
        this.ddlDefinerPattern = this.getPattern(configuration, Configuration.DDL_DEFINER_PATTERN, AugmenterContext.DEFAULT_DDL_DEFINER_PATTERN);
        this.ddlTablePattern = this.getPattern(configuration, Configuration.DDL_TABLE_PATTERN, AugmenterContext.DEFAULT_DDL_TABLE_PATTERN);
        this.ddlTemporaryTablePattern = this.getPattern(configuration, Configuration.DDL_TEMPORARY_TABLE_PATTERN, AugmenterContext.DEFAULT_DDL_TEMPORARY_TABLE_PATTERN);
        this.ddlViewPattern = this.getPattern(configuration, Configuration.DDL_VIEW_PATTERN, AugmenterContext.DEFAULT_DDL_VIEW_PATTERN);
        this.ddlAnalyzePattern = this.getPattern(configuration, Configuration.DDL_ANALYZE_PATTERN, AugmenterContext.DEFAULT_DDL_ANALYZE_PATTERN);
        this.enumPattern = this.getPattern(configuration, Configuration.ENUM_PATTERN, AugmenterContext.DEFAULT_ENUM_PATTERN);
        this.setPattern = this.getPattern(configuration, Configuration.SET_PATTERN, AugmenterContext.DEFAULT_SET_PATTERN);
        this.excludePattern = this.getPattern(configuration, Configuration.EXCLUDE_PATTERN,null);
        this.transactionsEnabled = Boolean.parseBoolean(configuration.getOrDefault(Configuration.TRANSACTIONS_ENABLED, "true").toString());
        this.excludeTableList = this.getList(configuration.get(Configuration.EXCLUDE_TABLE));
        this.includeTableList = this.getList(configuration.get(Configuration.INCLUDE_TABLE));
        this.renameMultiSchemaPattern = Pattern.compile(this.RENAME_MULTISCHEMA_PATTERN,Pattern.CASE_INSENSITIVE);


        this.timestamp = new AtomicLong();

        this.previousTimestamp = new AtomicLong();
        previousTimestamp.set(0L);

        this.serverId = new AtomicLong();
        this.nextPosition = new AtomicLong();

        this.shouldAugmentFlag = new AtomicBoolean();
        this.queryType = new AtomicReference<>();
        this.queryOperationType = new AtomicReference<>();
        this.eventTable = new AtomicReference<>();

        this.binlogFilename = new AtomicReference<>();
        this.binlogPosition = new AtomicLong();

        this.gtidType = GTIDType.valueOf(configuration.getOrDefault(Configuration.GTID_TYPE, AugmenterContext.DEFAULT_GTID_TYPE).toString());
        this.gtidValue = new AtomicReference<>();
        this.gtidFlags = new AtomicReference<>();
        this.gtidSet = new AtomicReference<>();

        this.columnsBefore = new AtomicReference<>();
        this.createTableBefore = new AtomicReference<>();
        this.columnsAfter = new AtomicReference<>();
        this.createTableAfter = new AtomicReference<>();

        this.isAtDDL = new AtomicBoolean();
        this.isAtDDL.set(false);
        replicatedSchema = (String) configuration.get( BinaryLogSupplier.Configuration.MYSQL_SCHEMA );

    }

    private Pattern getPattern(Map<String, Object> configuration, String configurationPath, String configurationDefault) {
        Object pattern = configuration.getOrDefault(
                configurationPath,
                configurationDefault
        );

        if ( pattern != null ) {
            return Pattern.compile(pattern.toString(),Pattern.CASE_INSENSITIVE);
        }

        return null;
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

    public AtomicLong getTransactionCounter() {
        return transactionCounter;
    }

    public synchronized void updateContext(RawEventHeaderV4 eventHeader, RawEventData eventData, String lastGTIDSet) {

        this.metrics.getRegistry().counter("augmenter.context.update_header.attempt").inc(1L);

        this.updateHeader(
                eventHeader.getTimestamp(),
                eventHeader.getServerId(),
                eventHeader.getNextPosition()
        );

        this.metrics.getRegistry().counter("augmenter.context.update_header.succeeded").inc(1L);

        isAtDDL.set(false);

        switch (eventHeader.getEventType()) {

            case ROTATE:
                processRotateEvent(eventData);
                break;

            case QUERY:
                processQueryEvent(eventHeader, eventData);
                break;

            case XID:
                processXidEvent(eventHeader, eventData);
                break;

            case GTID:
                processGTIDEvent(eventData, lastGTIDSet);
                break;

            case TABLE_MAP:
                processTableMapEvent(eventData);
                break;

            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                processRowsEvent(eventData);
                break;

            default:
                setDefaultCommons();
                break;
        }
    }

    private void setDefaultCommons() {
        this.metrics.getRegistry().counter("augmenter.context.type.default").inc(1L);
        this.updateCommons(
                false,
                null,
                null,
                null,
                null
        );
    }

    private void processRowsEvent(RawEventData eventData) {
        this.metrics.getRegistry()
                .counter("augmenter.context.type.insert_update_delete").inc(1L);

        TableIdRawEventData tableIdRawEventData = TableIdRawEventData.class.cast(eventData);

        FullTableName eventTable = this.getEventTableFromSchemaCache(tableIdRawEventData.getTableId());
        String dbName = null;
        String tblName = null;
        if (eventTable != null) {
            dbName = eventTable.getDb();
            tblName = eventTable.getName();
        } else {
            throw new RuntimeException("ERROR PROCESSING ROW EVENT: Unable to extract eventTable for QUERY event. GTID: " + this.transaction.getIdentifier().toString() );
        }

        this.updateCommons(
                ( replicatedSchema.equals(dbName)  && this.shouldAugmentTable(tblName) ),
                null,
                null,
                dbName,
                tblName
        );
    }

    private void processRotateEvent(RawEventData eventData) {
        this.updateCommons(
                false,
                null,
                null,
                null,
                null
        );

        this.metrics.getRegistry()
                .counter("augmenter.context.type.rotate").inc(1L);

        RotateRawEventData rotateRawEventData = RotateRawEventData.class.cast(eventData);

        this.updateBinlog(
                rotateRawEventData.getBinlogFilename(),
                rotateRawEventData.getBinlogPosition()
        );

        LOG.info(" File ROTATE : " + rotateRawEventData.getBinlogFilename());
    }

    private void processTableMapEvent(RawEventData eventData) {
        this.metrics.getRegistry().counter("augmenter.context.type.table_map").inc(1L);

        TableMapRawEventData tableMapRawEventData = TableMapRawEventData.class.cast(eventData);

        this.updateCommons(
                false,
                null,
                null,
                null,
                tableMapRawEventData.getTable()
        );

        // update cache
        this.schemaCache.get().getTableIdToTableNameMap().put(
                tableMapRawEventData.getTableId(),
                new FullTableName(
                        tableMapRawEventData.getDatabase(),
                        tableMapRawEventData.getTable()
                )
        );
    }

    private void processGTIDEvent(RawEventData eventData, String lastGTIDSet) {
        this.metrics.getRegistry()
                .counter("augmenter_context.type.gtid").inc(1L);

        GTIDRawEventData gtidRawEventData = GTIDRawEventData.class.cast(eventData);
        this.gtidSet.set(lastGTIDSet);

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

        this.getTransaction().setIdentifier( this.getGTID() );
    }

    private void processXidEvent(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        this.metrics.getRegistry()
                .counter("augmenter.context.type.xid").inc(1L);

        XIDRawEventData xidRawEventData = XIDRawEventData.class.cast(eventData);

        this.updateCommons(
                true,
                QueryAugmentedEventDataType.COMMIT,
                null,
                null,
                null
        );

        if (!this.transaction.commit(xidRawEventData.getXID(), eventHeader.getTimestamp(), transactionCounter.get())) {
            AugmenterContext.LOG.warn("transaction already markedForCommit");
        }
    }

    private void processQueryEvent(RawEventHeaderV4 eventHeader, RawEventData eventData) {

        QueryRawEventData queryRawEventData = QueryRawEventData.class.cast(eventData);
        String query = queryRawEventData.getSQL();
        Matcher matcher;

        this.metrics.getRegistry()
                .counter("augmenter.context.type.query").inc(1L);

        // BEGIN
        if (this.beginPattern.matcher(query).find()) {
            this.updateCommons(
                    false,
                    QueryAugmentedEventDataType.BEGIN,
                    null,
                    queryRawEventData.getDatabase(),
                    null
            );
            if (!this.transaction.begin()) {
                AugmenterContext.LOG.warn("transaction already started");
            }

        // COMMIT
        } else if (commitPattern.matcher(query).find()) {
            // commit
            this.updateCommons(
                    true,
                    QueryAugmentedEventDataType.COMMIT,
                    null,
                    queryRawEventData.getDatabase(),
                    null
            );
            this.metrics.getRegistry()
                    .counter("augmenter.context.type.commit").inc(1L);
            if (!this.transaction.commit(eventHeader.getTimestamp(), transactionCounter.get())) {
                AugmenterContext.LOG.warn("transaction already markedForCommit");
            }

        // Definer
        } else if ((matcher = ddlDefinerPattern.matcher(query)).find()) {
            // ddl definer
            this.metrics.getRegistry()
                    .counter("augmenter.context.type.ddl_definer").inc(1L);
            this.updateCommons(
                    false,
                    QueryAugmentedEventDataType.DDL_DEFINER,
                    QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                    queryRawEventData.getDatabase(),
                    null
            );

        // Table DDL
        } else if ((matcher = ddlTablePattern.matcher(query)).find()) {
            // ddl table
            this.metrics.getRegistry()
                    .counter("augmenter.context.type.ddl_table").inc(1L);

            Boolean shouldProcess = true;

            if ( matcher.group(2).toLowerCase().equals("rename") ) {
                // Now skip if we are renaming TO a different schema
                shouldProcess = ActiveSchemaHelpers.getShouldProcess(query, renameMultiSchemaPattern, replicatedSchema);
            }

            if ( !shouldProcess ) {
                LOG.info("Skipping DDL TABLE event due to mismatching schemas. Next LOG.info line contains the relevant query");
            }

            shouldProcess = shouldProcess && ( queryRawEventData.getDatabase().equals(replicatedSchema) );
            String eventType = matcher.group(2).toUpperCase();
            String tableName = matcher.group(4);

            // If the tableName contains a period it has both a schema and table name
            if ( tableName.indexOf(".") != -1 ) {
                String[] matches = tableName.split("\\.");
                String schema = matches[0].replaceAll("`","");
                if ( !schema.equals(replicatedSchema) ) {
                    LOG.info("Skipping DDL TABLE event due to affected table schema (" + schema + ") not matching replicatedSchema (" + replicatedSchema + "), while the getDatabase() ("+ queryRawEventData.getDatabase() + ") schema *DOES* match replicatedSchema");
                    shouldProcess = false;
                } else {
                    tableName = matches[1];
                }
            }

            if (tableName.startsWith("`") && tableName.endsWith("`")) {
                tableName = tableName.substring(1, tableName.length() -1);
            }

            if (shouldProcess) {
                shouldProcess    = this.shouldAugmentTable(tableName);
                if (!shouldProcess) {
                    LOG.info("Skipping DDL event as the table (" + tableName + ") is part of exclude list. The query is : " + queryRawEventData.getSQL());
                }
            }

            this.updateCommons(
                    shouldProcess,
                    QueryAugmentedEventDataType.DDL_TABLE,
                    QueryAugmentedEventDataOperationType.valueOf(eventType),
                    queryRawEventData.getDatabase(),
                    tableName
            );

            // Because we don't want to create tables for non-replicated schemas
            if ( shouldProcess ) {

                this.metrics.getRegistry()
                        .counter("augmenter.context.type.ddl_table.should_process.true").inc(1L);

                isAtDDL.set(true);

                long schemaChangeTimestamp = eventHeader.getTimestamp();
                this.updateSchema(query, schemaChangeTimestamp);

            } else {
                this.metrics.getRegistry()
                        .counter("augmenter.context.type.ddl_table.should_process.false").inc(1L);
            }

        } else if ((matcher = this.ddlTemporaryTablePattern.matcher(query)).find()) {
            // ddl temp table
            this.metrics.getRegistry()
                    .counter("augmenter.context.type.ddl_temp_table").inc(1L);
            this.updateCommons(
                    ( queryRawEventData.getDatabase().equals(replicatedSchema) ),
                    QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE,
                    QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                    queryRawEventData.getDatabase(),
                    matcher.group(4)
            );
        } else if ((matcher = this.ddlViewPattern.matcher(query)).find()) {
            // ddl view
            this.metrics.getRegistry()
                    .counter("augmenter.context.type.ddl_view").inc(1L);
            this.updateCommons(
                    ( queryRawEventData.getDatabase().equals(replicatedSchema) ),
                    QueryAugmentedEventDataType.DDL_VIEW,
                    QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                    queryRawEventData.getDatabase(),
                    null
            );
        } else if ((matcher = this.ddlAnalyzePattern.matcher(query)).find()) {
            this.metrics.getRegistry()
                    .counter("augmenter.context.type.ddl_analyse").inc(1L);
            this.updateCommons(
                    false,
                    QueryAugmentedEventDataType.DDL_ANALYZE,
                    QueryAugmentedEventDataOperationType.valueOf(matcher.group(2).toUpperCase()),
                    queryRawEventData.getDatabase(),
                    null
            );
        } else {
            this.metrics.getRegistry()
                    .counter("augmenter.context.type.unknown").inc(1L);
            this.updateCommons(
                    false,
                    null,
                    null,
                    queryRawEventData.getDatabase(),
                    null
            );
        }
    }

    private synchronized void updateTransactionCounter() {

        if (transactionCounter.get() > 9998L) {
            transactionCounter.set(0L);
            LOG.warn("TransactionCounter counter is overflowed, resetting to 0.");
        }
        if (timestamp.get() > previousTimestamp.get()) {
            transactionCounter.set(0);
            previousTimestamp.set(timestamp.get());
        } else if (timestamp.get() == previousTimestamp.get()) {
            transactionCounter.incrementAndGet();
        }
    }

    private void updateHeader(long timestamp, long serverId, long nextPosition) {
        this.timestamp.set(timestamp);

        this.serverId.set(serverId);
        this.nextPosition.set(nextPosition);
    }

    private void updateCommons(
            boolean shouldAugmentFlag,
            QueryAugmentedEventDataType queryType,
            QueryAugmentedEventDataOperationType queryOperationType,
            String database,
            String table
    ) {
        this.shouldAugmentFlag.set(shouldAugmentFlag);
        this.queryType.set(queryType);
        this.queryOperationType.set(queryOperationType);

        if (table != null) {
            this.eventTable.set(new FullTableName(database, table));
        }

        if (queryType != null) {
            if (queryType.equals(QueryAugmentedEventDataType.COMMIT)) {
                updateTransactionCounter();
            }
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

    private void updateSchema(String query, long schemaChangeTimestamp) {

        if (query != null) {

            if (this.eventTable.get() != null) {

                SchemaAtPositionCache schemaPositionCacheBefore = schemaCache.get().deepCopy();

                String tableName = this.eventTable.get().getName();

                if (isDDLAndIsNot(QueryAugmentedEventDataOperationType.CREATE)) {
                    // if not create, then before exists
                    this.columnsBefore.set(this.schemaManager.listColumns(tableName));
                    this.createTableBefore.set(this.schemaManager.getCreateTable(tableName));
                } else {
                    this.columnsBefore.set(null);
                    this.createTableBefore.set(null);
                }

                this.schemaManager.execute(tableName, query);
                this.schemaCache.get().reloadTableSchema(
                        tableName,
                        this.schemaManager.getComputeTableSchemaLambda()
                );

                if (isDDLAndIsNot(QueryAugmentedEventDataOperationType.DROP)) {
                    // Not drop, so create/alter, set after?
                    this.columnsAfter.set(this.schemaManager.listColumns(tableName));
                    this.createTableAfter.set(this.schemaManager.getCreateTable(tableName));
                } else {
                    // if delete, then after does not exists
                    this.columnsAfter.set(null);
                    this.createTableAfter.set(null);
                }

                SchemaAtPositionCache schemaPositionCacheAfter = schemaCache.get().deepCopy();
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
                || this.queryType.get() == QueryAugmentedEventDataType.DDL_TEMPORARY_TABLE )
                && this.getQueryOperationType() != ddlOpType);
    }

    private boolean shouldAugmentTable(String tableName) {
        return !this.excludeTable(tableName) && this.includeTable(tableName);
    }

    private boolean excludeTable(String tableName) {
        if ( this.excludePattern != null && this.excludePattern.matcher(tableName).find() ) {
            return true;
        }
        return this.excludeTableList.contains(tableName);
    }

    private boolean includeTable(String tableName) {
        // By default include all tables. If white list is explicitly specified
        // then filter out all tables that are not on the white list.
        if (this.includeTableList != null && this.includeTableList.size() > 0) {
            return this.includeTableList.contains(tableName);
        } else {
            return true;
        }
    }

    public void updatePosition() {
        if (this.nextPosition.get() > 0) {
            this.binlogPosition.set(this.nextPosition.get());
        }
    }

    public CurrentTransaction getTransaction() {
        return this.transaction;
    }


    public boolean shouldAugment() {
        return this.shouldAugmentFlag.get();
    }

    public QueryAugmentedEventDataType getQueryType() {
        return this.queryType.get();
    }

    public QueryAugmentedEventDataOperationType getQueryOperationType() {
        return this.queryOperationType.get();
    }

    public FullTableName getEventTableFromSchemaCache() {
        return this.eventTable.get();
    }

    public FullTableName getEventTableFromSchemaCache(long tableId) {
        return this.schemaCache.get().getTableIdToTableNameMap().get(tableId);
    }

    public Checkpoint newCheckpoint() {
        return new Checkpoint(
                this.timestamp.get(),
                this.serverId.get(),
                this.getGTID(),
                this.getBinlog(),
                this.gtidSet.get()
        );
    }

    public Checkpoint newCheckpointFromGTIDSet() {
        return new Checkpoint(
                this.gtidSet.get()
        );
    }


    public boolean isTransactionsEnabled() {
        return transactionsEnabled;
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
            String tableName = this.eventTable.get().getName();
            String schemaName = this.eventTable.get().getDb();
            return new TableSchema(
                    new FullTableName(schemaName, tableName),
                    this.columnsBefore.get(),
                    this.createTableBefore.get()
            );
        } else {
            return null;
        }
    }

    public TableSchema getSchemaAfter() {
        if (this.columnsAfter.get() != null && this.createTableAfter != null) {
            String tableName = this.eventTable.get().getName();
            String schemaName = this.eventTable.get().getDb();
            return new TableSchema(
                    new FullTableName(schemaName, tableName),
                    this.columnsAfter.get(),
                    this.createTableAfter.get()
            );
        } else {
            return null;
        }
    }

    public Collection<Boolean> getIncludedColumns(BitSet includedColumns) {
        Collection<Boolean> includedColumnList = new ArrayList<>(includedColumns.length());

        for (int index = 0; index < includedColumns.length(); index++) {
            includedColumnList.add(includedColumns.get(index));
        }

        return includedColumnList;
    }

    public void incrementRowCounterMetrics(RawEventType eventType, int rows) {
        FullTableName eventTable = this.getEventTableFromSchemaCache();
        String tblName = eventTable == null ? "null" : eventTable.getName();
        String name = MetricRegistry.name(binlogsBasePath, tblName, eventType.name());
        this.metrics.incrementCounter(name, rows);
    }

    public Collection<ColumnSchema> getColumns(long tableId) {
        FullTableName eventTable = this.getEventTableFromSchemaCache(tableId);

        if (eventTable != null) {
            return this.schemaManager.listColumns(eventTable.getName());
        } else {
            return null;
        }
    }

    public Collection<AugmentedRow> computeAugmentedEventRows(
            AugmentedEventType eventType,
            Long commitTimestamp,
            String transactionUUID,
            Long xxid,
            long tableId,
            BitSet includedColumns,
            List<RowBeforeAfter> rows ) {

        FullTableName eventTable = this.getEventTableFromSchemaCache(tableId);

        if (eventTable != null) {

            Collection<AugmentedRow> augmentedRows = new ArrayList<>();
            List<ColumnSchema> columns = this.schemaManager.listColumns(eventTable.getName());
            Map<String, String[]> cache = this.getCache(columns);

            for (RowBeforeAfter row : rows) {

                augmentedRows.add(
                        this.getAugmentedRow(
                                eventType,
                                commitTimestamp,
                                transactionUUID,
                                xxid,
                                columns,
                                includedColumns,
                                row,
                                cache,
                                eventTable
                        )
                );
            }
            return augmentedRows;
        } else {
            return null;
        }
    }

    private AugmentedRow getAugmentedRow(
            AugmentedEventType eventType,
            Long commitTimestamp,
            String transactionUUID,
            Long transactionXid,
            List<ColumnSchema> columnSchemas,
            BitSet includedColumns,
            RowBeforeAfter row,
            Map<String, String[]> cache,
            FullTableName eventTable
    ) {
        Map<String, Object> deserializeCellValues ;
        try {
            deserializeCellValues = EventDeserializer.getDeserializeCellValues(eventType, columnSchemas, includedColumns, row, cache);
        } catch (Exception e) {
            LOG.error("Error while deserialize row: EventType: " + eventType + " table: " + this.getEventTableFromSchemaCache() + ", row: " + row.toString(), e);
            throw e;
        }

        String schemaName = eventTable.getDb();
        String tableName = eventTable.getName();

        List<String> primaryKeyColumns = TableSchema.getPrimaryKeyColumns(columnSchemas);

        AugmentedRow augmentedRow = new AugmentedRow(
                eventType, schemaName, tableName,
                commitTimestamp, transactionUUID, transactionXid,
                primaryKeyColumns, deserializeCellValues
        );

        return augmentedRow;
    }

    private Map<String, String[]> getCache(List<ColumnSchema> columns) {
        Matcher matcher;
        Map<String, String[]> cache = new HashMap<>();

        for (ColumnSchema column : columns) {
            String columnType = column.getColumnType().toLowerCase();

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
