package com.booking.replication.pipeline;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.booking.replication.Coordinator;
import com.booking.replication.applier.Applier;
import com.booking.replication.checkpoints.*;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.augmenter.EventAugmenter;
import com.booking.replication.Metrics;
import com.booking.replication.queues.ReplicatorQueues;
import com.booking.replication.schema.TableNameMapper;
import com.booking.replication.schema.HBaseSchemaManager;
import com.booking.replication.schema.exception.TableMapException;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.util.MySQLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * PipelineOrchestrator
 *
 * On each event handles:
 *      1. schema version management
 *      2  augmenting events with schema info
 *      3. sending of events to applier.
 */
public class PipelineOrchestrator extends Thread {

    public  final  Configuration      configuration;
    private final  Applier            applier;
    private final  ReplicatorQueues   queues;
    private static EventAugmenter     eventAugmenter;
    private static HBaseSchemaManager hBaseSchemaManager;

    public CurrentTransactionMetadata currentTransactionMetadata;

    private final ConcurrentHashMap<Integer, BinlogPositionInfo> binlogPositionLastKnownInfo;

    private volatile boolean running = false;

    private volatile boolean replicatorShutdownRequested = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineOrchestrator.class);

    private static final Counter XIDCounter = Metrics.registry.counter(name("events", "XIDCounter"));
    private static final Counter deleteEventCounter = Metrics.registry.counter(name("events", "deleteEventCounter"));
    private static final Counter insertEventCounter = Metrics.registry.counter(name("events", "insertEventCounter"));
    private static final Counter commitQueryCounter = Metrics.registry.counter(name("events", "commitQueryCounter"));
    private static final Counter updateEventCounter = Metrics.registry.counter(name("events", "updateEventCounter"));
    private static final Counter heartBeatCounter = Metrics.registry.counter(name("events", "heartBeatCounter"));
    private static final Counter eventsReceivedCounter = Metrics.registry.counter(name("events", "eventsReceivedCounter"));
    private static final Counter eventsProcessedCounter = Metrics.registry.counter(name("events", "eventsProcessedCounter"));
    private static final Counter eventsSkippedCounter = Metrics.registry.counter(name("events", "eventsSkippedCounter"));

    private static final int BUFFER_FLUSH_INTERVAL = 30000; // <- force buffer flush every 30 sec

    private static final int DEFAULT_VERSIONS_FOR_MIRRORED_TABLES = 1000;

    private HashMap<String,Boolean> rotateEventAllreadySeenForBinlogFile = new HashMap<String, Boolean>();

    public long consumerStatsNumberOfProcessedRows = 0;
    public long consumerStatsNumberOfProcessedEvents = 0;

    public long consumerTimeM1 = 0;
    public long consumerTimeM1_WriteV2 = 0;
    public long consumerTimeM2 = 0;
    public long consumerTimeM3 = 0;
    public long consumerTimeM4 = 0;
    public long consumerTimeM5 = 0;

    public long eventCounter = 0;

   /**
    * fakeMicrosecondCounter: this is a special feature that
    * requires some explanation
    *
    * MySQL binlog events have second-precision timestamps. This
    * obviously means that we can't have microsecond precision,
    * but that is not the intention here. The idea is to at least
    * preserve the information about ordering of events,
    * especially if one ID has multiple events within the same
    * second. We want to know what was their order. That is the
    * main purpose of this counter.
    */
    private static long fakeMicrosecondCounter = 0;

    public void requestReplicatorShutdown(){
        this.replicatorShutdownRequested = true;
    }

    public boolean isReplicatorShutdownRequested() {
        return replicatorShutdownRequested;
    }

    public static void setFakeMicrosecondCounter(Long fakeMicrosecondCounter) {
        PipelineOrchestrator.fakeMicrosecondCounter = fakeMicrosecondCounter;
    }

    public PipelineOrchestrator(
            ReplicatorQueues                  repQueues,
            ConcurrentHashMap<Integer, BinlogPositionInfo> chm,
            Configuration                     repcfg,
            Applier                           applier
        ) throws SQLException, URISyntaxException, IOException {

        this.queues            = repQueues;
        this.configuration     = repcfg;

        eventAugmenter = new EventAugmenter(repcfg);

        currentTransactionMetadata = new CurrentTransactionMetadata();

        if (configuration.getApplierType().equals("hbase")) {
            hBaseSchemaManager = new HBaseSchemaManager(repcfg.getHBaseQuorum());
        }

        this.applier = applier;

        binlogPositionLastKnownInfo = chm;

        LOGGER.info("Created consumer with binlogPositionLastKnownInfo position => { "
                + " binlogFileName => "
                +   binlogPositionLastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION).getBinlogFilename()
                + ", binlogPosition => "
                +   binlogPositionLastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION).getBinlogPosition()
                + " }"
        );

        Metrics.registry.register(MetricRegistry.name("events", "replicatorReplicationDelay"),
            new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return replDelay;
            }
        });
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void stopRunning() {
        setRunning(false);
    }

    @Override
    public void run() {

        setRunning(true);

        long timeOfLastEvent = System.currentTimeMillis();

        while (isRunning()) {
            try {
                if (queues.rawQueue.size() > 0) {

                    BinlogEventV4 event =
                            queues.rawQueue.poll(100, TimeUnit.MILLISECONDS);

                    if (event != null) {

                        timeOfLastEvent = System.currentTimeMillis();

                        eventCounter++;

                        eventsReceivedCounter.inc();
                        if (!skipEvent(event)) {
                            calculateAndPropagateChanges(event);
                            eventsProcessedCounter.inc();
                        } else {
                            eventsSkippedCounter.inc();
                        }
                        if (eventCounter % 5000 == 0) {
                            LOGGER.info("Pipeline report: producer rawQueue size => " + queues.rawQueue.size());
                        }
                    } else {
                        LOGGER.warn("Poll timeout. Will sleep for 1s and try again.");
                        Thread.sleep(1000);
                    }
                }
                else {
                    LOGGER.info("Pipeline report: no items in producer event rawQueue. Will sleep for 0.5s and check again.");
                    Thread.sleep(500);
                    long currentTime = System.currentTimeMillis();
                    long tDiff = currentTime - timeOfLastEvent;
                    boolean forceFlush = (tDiff > BUFFER_FLUSH_INTERVAL);
                    if (forceFlush) {
                        applier.forceFlush();
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException, requesting replicator shutdown...", e);
                requestReplicatorShutdown();
            } catch (TableMapException e) {
                LOGGER.error("TableMapException, requesting replicator shutdown...",e);
                requestReplicatorShutdown();
            } catch (IOException e) {
                LOGGER.error("IOException, requesting replicator shutdown...",e);
                requestReplicatorShutdown();
            } catch (Exception e) {
                LOGGER.error("Exception, requesting replicator shutdown...",e);
                requestReplicatorShutdown();
            }
        }
    }

    private Long replDelay = 0L;

    /**
     *  calculateAndPropagateChanges
     *
     *     STEPS:
     *     ======
     *  1. check event type
     *
     *  2. if DDL:
     *      a. pass to eventAugmenter which will update the schema
     *      b. calculateAndPropagateChanges event to AugmentedQueue
     *
     *  3. if DATA:
     *      a. match column names and types
     *      b. calculateAndPropagateChanges event to AugmentedQueue
     */
    public void calculateAndPropagateChanges(BinlogEventV4 event) throws IOException, TableMapException {

        AugmentedRowsEvent augmentedRowsEvent;
        AugmentedSchemaChangeEvent augmentedSchemaChangeEvent;

        if (fakeMicrosecondCounter > 999998) {
            fakeMicrosecondCounter = 0;
        }

        long tStart;
        long tEnd;
        long tDelta;

        // Calculate replication delay before the event timestamp is extended with fake miscrosecond part
        replDelay = event.getHeader().getTimestampOfReceipt() - event.getHeader().getTimestamp();

        // Process Event
        switch (event.getHeader().getEventType()) {

            // DDL Event:
            case MySQLConstants.QUERY_EVENT:
                fakeMicrosecondCounter++;
                doTimestampOverride(event);
                String querySQL = ((QueryEvent) event).getSql().toString();
                boolean isDDL = isDDL(querySQL);
                if (isCOMMIT(querySQL, isDDL)) {
                    commitQueryCounter.inc();
                    applier.applyCommitQueryEvent((QueryEvent) event);
                }
                else if (isBEGIN(querySQL, isDDL)) {
                    currentTransactionMetadata = null;
                    currentTransactionMetadata = new CurrentTransactionMetadata();
                }
                else if (isDDL) {
                    long tStart5 = System.currentTimeMillis();
                    augmentedSchemaChangeEvent = eventAugmenter.transitionSchemaToNextVersion(event);
                    long tEnd5 = System.currentTimeMillis();
                    long tDelta5 = tEnd5 - tStart5;
                    consumerTimeM5 += tDelta5;
                    applier.applyAugmentedSchemaChangeEvent(augmentedSchemaChangeEvent, this);
                }
                else {
                    LOGGER.warn("Unexpected query event: " + querySQL);
                }
                break;

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                String tableName = ((TableMapEvent) event).getTableName().toString();

                if (tableName.equals(Constants.HEART_BEAT_TABLE)) {
                    // reset the fake microsecond counter on hearth beat event. In our case
                    // hearth-beat is a regular update and it is treated as such in the rest
                    // of the code (therefore replicated in HBase table so we have the
                    // hearth-beat in HBase and can use it to check replication delay). The only
                    // exception is that when we see this event we reset the fake-microseconds counter.
                    LOGGER.debug("fakeMicrosecondCounter before reset => " + fakeMicrosecondCounter);
                    fakeMicrosecondCounter = 0;
                    doTimestampOverride(event);
                    heartBeatCounter.inc();
                } else {
                    fakeMicrosecondCounter++;
                    doTimestampOverride(event);
                }
                try {
                    currentTransactionMetadata.updateCache((TableMapEvent) event);
                    long tableID = ((TableMapEvent) event).getTableId();
                    String dbName = currentTransactionMetadata.getDBNameFromTableID(tableID);
                    LOGGER.debug("processing events for { db => " + dbName + " table => " + tableName + " } ");
                    LOGGER.debug("fakeMicrosecondCounter at tableMap event => " + fakeMicrosecondCounter);
                    String hbaseTableName = dbName.toLowerCase()
                            + ":"
                            + tableName.toLowerCase();
                    if (configuration.getApplierType().equals("hbase")) {
                        if (!hBaseSchemaManager.isTableKnownToHBase(hbaseTableName)) {
                            // This should not happen in tableMapEvent, unless we are
                            // replaying the binlog.
                            // TODO: load hbase tables on start-up so this never happens
                            hBaseSchemaManager.createMirroredTableIfNotExists(hbaseTableName, DEFAULT_VERSIONS_FOR_MIRRORED_TABLES);
                        }
                        if(configuration.isWriteRecentChangesToDeltaTables()) {
                            //String replicantSchema = ((TableMapEvent) event).getDatabaseName().toString();
                            String mysqlTableName = ((TableMapEvent) event).getTableName().toString();

                            if (configuration.getTablesForWhichToTrackDailyChanges().contains(mysqlTableName)) {

                                long eventTimestampMicroSec = event.getHeader().getTimestamp();

                                String deltaTableName = TableNameMapper.getCurrentDeltaTableName(
                                        eventTimestampMicroSec,
                                        configuration.getHbaseNamespace(),
                                        mysqlTableName,
                                        configuration.isInitialSnapshotMode());
                                if (!hBaseSchemaManager.isTableKnownToHBase(deltaTableName)) {
                                    boolean isInitialSnapshotMode = configuration.isInitialSnapshotMode();
                                    hBaseSchemaManager.createDeltaTableIfNotExists(deltaTableName, isInitialSnapshotMode);
                                }
                            }
                        }
                    }
                    updateLastKnownPositionForMapEvent((TableMapEvent) event);
                }
                catch (Exception e) {
                    LOGGER.error("Could not execute mapEvent block. Requesting replicator shutdown...", e);
                    requestReplicatorShutdown();
                }
                break;

            // Data event:
            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                fakeMicrosecondCounter++;
                doTimestampOverride(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.applyAugmentedRowsEvent(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                updateEventCounter.inc();
                break;

            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
                fakeMicrosecondCounter++;
                doTimestampOverride(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.applyAugmentedRowsEvent(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                insertEventCounter.inc();
                break;

            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                fakeMicrosecondCounter++;
                doTimestampOverride(event);
                tStart = System.currentTimeMillis();
                augmentedRowsEvent = eventAugmenter.mapDataEventToSchema((AbstractRowEvent) event, this);
                tEnd = System.currentTimeMillis();
                tDelta = tEnd - tStart;
                consumerTimeM1 += tDelta;
                applier.applyAugmentedRowsEvent(augmentedRowsEvent,this);
                updateLastKnownPosition((AbstractRowEvent) event);
                deleteEventCounter.inc();
                break;

            case MySQLConstants.XID_EVENT:
                // Latter we may want to tag previous data events with xid_id
                // (so we can know if events were in the same transaction).
                // For now we just increase the counter.
                fakeMicrosecondCounter++;
                doTimestampOverride(event);
                applier.applyXIDEvent((XidEvent) event);
                XIDCounter.inc();
                currentTransactionMetadata = null;
                currentTransactionMetadata = new CurrentTransactionMetadata();
                break;

            // reset the fakeMicrosecondCounter at the beginning of the new binlog file
            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                fakeMicrosecondCounter = 0;
                applier.applyFormatDescriptionEvent((FormatDescriptionEvent) event);
                break;

            // flush buffer at the end of binlog file
            case MySQLConstants.ROTATE_EVENT:
                RotateEvent re = (RotateEvent) event;
                applier.applyRotateEvent(re);
                LOGGER.info("End of binlog file. Waiting for all tasks to finish before moving forward...");

                //TODO: Investigate if this is the right thing to do.
                applier.waitUntilAllRowsAreCommitted();

                LOGGER.info("All rows committed");
                String currentBinlogFileName =
                        binlogPositionLastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION).getBinlogFilename();

                String nextBinlogFileName = re.getBinlogFileName().toString();
                long nextBinlogPosition = re.getBinlogPosition();

                int currentSlaveId = configuration.getReplicantDBServerID();
                LastVerifiedBinlogFile marker = new LastVerifiedBinlogFile(currentSlaveId, nextBinlogFileName, nextBinlogPosition);

                try {
                    Coordinator.saveCheckpointMarker(marker);
                } catch (Exception e) {
                    LOGGER.error("Failed to save Checkpoint!");
                    e.printStackTrace();
                }

                if (currentBinlogFileName.equals(configuration.getLastBinlogFileName())){
                    LOGGER.info("processed the last binlog file " + configuration.getLastBinlogFileName());
                    setRunning(false);
                    requestReplicatorShutdown();
                }
                break;

            // Events that we expect to appear in the binlog, but we don't do
            // any extra processing.
            case MySQLConstants.STOP_EVENT:
                break;

            // Events that we do not expect to appear in the binlog
            // so a warning should be logged for those types
            default:
                LOGGER.warn("Unexpected event type: " + event.getHeader().getEventType());
                break;
        }
    }

    public boolean isDDL(String querySQL) {

        long tStart = System.currentTimeMillis();
        boolean isDDL;
        // optimization
        if (querySQL.equals("BEGIN")) {
            isDDL = false;
        }
        else {

            String ddlPattern = "(alter|drop|create|rename|truncate|modify)\\s+(table|column)";

            Pattern p = Pattern.compile(ddlPattern, Pattern.CASE_INSENSITIVE);

            Matcher m = p.matcher(querySQL);

            isDDL = m.find();
        }

        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;

        consumerTimeM2 += tDelta;
        return isDDL;
    }

    public boolean isBEGIN(String querySQL, boolean isDDL) {

        boolean hasBEGIN;

        // optimization
        if (querySQL.equals("COMMIT")) {
            hasBEGIN = false;
        }
        else {

            String beginPattern = "(begin)";

            Pattern p = Pattern.compile(beginPattern, Pattern.CASE_INSENSITIVE);

            Matcher m = p.matcher(querySQL);

            hasBEGIN = m.find();
        }

        boolean isBEGIN = (hasBEGIN && !isDDL);

        return isBEGIN;
    }

    public boolean isCOMMIT(String querySQL, boolean isDDL) {

        boolean hasCOMMIT;

        // optimization
        if (querySQL.equals("BEGIN")) {
            hasCOMMIT = false;
        }
        else {

            String commitPattern = "(commit)";

            Pattern p = Pattern.compile(commitPattern, Pattern.CASE_INSENSITIVE);

            Matcher m = p.matcher(querySQL);

            hasCOMMIT = m.find();
        }

        boolean isCOMMIT = (hasCOMMIT && !isDDL);

        return isCOMMIT;
    }

    public boolean isReplicant(String schemaName) {
        if (schemaName.equals(configuration.getReplicantSchemaName())) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean isCreateOnly(String querySQL) {

        // TODO: use this to skip table create for tables that allready exists

        boolean isCreateOnly;

        String createPattern = "(create)\\s+(table)";
        Pattern pC = Pattern.compile(createPattern, Pattern.CASE_INSENSITIVE);
        Matcher mC = pC.matcher(querySQL);

        boolean hasCreate = mC.find();

        String otherPattern = "(alter|drop|rename|truncate|modify)\\s+(table|column)";
        Pattern pO = Pattern.compile(otherPattern, Pattern.CASE_INSENSITIVE);
        Matcher mO = pO.matcher(querySQL);

        boolean hasOther = mO.find();

        if (hasCreate && !hasOther) {
            isCreateOnly = true;
        }
        else {
            isCreateOnly = false;
        }
        return isCreateOnly;
    }

    /**
     * Returns true if event type is not tracked, or does not belong to the
     * tracked database
     * @param  event Binlog event that needs to be checked
     * @return shouldSkip Weather event should be skipped or processed
     * @throws TableMapException
     */
    public boolean skipEvent(BinlogEventV4 event) throws TableMapException {

        long tStart = System.currentTimeMillis();
        boolean eventIsTracked      = false;
        switch (event.getHeader().getEventType()) {
            // Query Event:
            case MySQLConstants.QUERY_EVENT:
                String querySQL  = ((QueryEvent) event).getSql().toString();
                boolean isDDL    = isDDL(querySQL);
                boolean isCOMMIT = isCOMMIT(querySQL, isDDL);
                boolean isBEGIN  = isBEGIN(querySQL, isDDL);
                if (isCOMMIT) {
                    // COMMIT does not always contain database name so we get it
                    // from current transaction metadata.
                    // There is an assumption that all tables in the transaction
                    // are from the same database. Cross database transactions
                    // are not supported.
                    TableMapEvent firstMapEvent = currentTransactionMetadata.getFirstMapEventInTransaction();
                    if (firstMapEvent != null) {
                        String currentTransactionDBName = firstMapEvent.getDatabaseName().toString();
                        if (isReplicant(currentTransactionDBName)) {
                            eventIsTracked = true;
                        } else {
                            LOGGER.warn("non-replicated database [" + currentTransactionDBName + "] in current transaction.");
                        }
                    }
                    else {
                        String message = "";
                        for (String tblName : currentTransactionMetadata.getCurrentTransactionTableMapEvents().keySet()) {
                            message += tblName;
                            message += ", ";
                        }
                        LOGGER.warn("Received COMMIT event, but currentTransactionMetadata is empty! " +
                                "Tables in transaction are " + message
                        );
                    }
                }
                else if (isBEGIN) {
                    eventIsTracked = true;
                }
                else if (isDDL) {
                    // DDL event should always contain db name
                    String dbName = ((QueryEvent) event).getDatabaseName().toString();
                    if (isReplicant(dbName)) {
                        eventIsTracked = true;
                    }
                    else {
                        eventIsTracked = false;
                        LOGGER.warn("DDL statement " + querySQL + " on non-replicated database [" + dbName + "].");

                    }
                }
                else {
                    // TODO: handle View statement
                    // LOGGER.warn("Received non-DDL, non-COMMIT, non-BEGIN query: " + querySQL);
                }
                break;

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                if (isReplicant(((TableMapEvent)event).getDatabaseName().toString())) {
                    eventIsTracked = true;
                }
                else{
                    eventIsTracked = false;
                }
                break;

            // Data event:
            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                if (currentTransactionMetadata.getFirstMapEventInTransaction() == null) {
                    // row event and no map event -> blacklisted schema so map event was skipped
                    eventIsTracked = false;
                }
                else {
                    eventIsTracked = true;
                }
                break;

            case MySQLConstants.XID_EVENT:
                if (currentTransactionMetadata.getFirstMapEventInTransaction() == null) {
                    // xid event and no map event -> blacklisted schema so map event was skipped
                    eventIsTracked = false;
                }
                else {
                    eventIsTracked = true;
                }
                break;

            case MySQLConstants.ROTATE_EVENT:
                // This is a  workaround for a bug in open replicator
                // which results in rotate event being created twice per
                // binlog file - once at the end of the binlog file (as it should be)
                // and once at the beginning of the next binlog file (which is a bug)
                String currentBinlogFile =
                        ((BinlogPositionInfo) binlogPositionLastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogFilename();
                if (rotateEventAllreadySeenForBinlogFile.containsKey(currentBinlogFile)) {
                    eventIsTracked = false;
                }
                else {
                    eventIsTracked = true;
                    rotateEventAllreadySeenForBinlogFile.put(currentBinlogFile, true);
                }
                break;

            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                eventIsTracked = true;
                break;

            case MySQLConstants.STOP_EVENT:
                eventIsTracked = true;
                break;

            default:
                eventIsTracked = false;
                LOGGER.warn("Unexpected event type => " + event.getHeader().getEventType());
                break;
        }

        boolean skipEvent;
        skipEvent = !eventIsTracked;

        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        consumerTimeM3 += tDelta;

        return  skipEvent;
    }

    private void doTimestampOverride(BinlogEventV4 event) {
        long tStart = System.currentTimeMillis();

        if (this.configuration.isInitialSnapshotMode()) {
            doInitialSnapshotEventTimestampOverride(event);
        }
        else {
            injectFakeMicroSecondsIntoEventTimestamp(event);
        }
        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;

        consumerTimeM4 += tDelta;
    }

    private void injectFakeMicroSecondsIntoEventTimestamp(BinlogEventV4 event) {

        long overriddenTimestamp = event.getHeader().getTimestamp();

        if (overriddenTimestamp != 0) {
            // timestamp is in millisecond form, but the millisecond part is actually 000 (for example 1447755881000)
            String timestampString = Long.toString(overriddenTimestamp).substring(0,10);
            overriddenTimestamp = Long.parseLong(timestampString) * 1000000;
            overriddenTimestamp += fakeMicrosecondCounter;
            ((BinlogEventV4HeaderImpl)(event.getHeader())).setTimestamp(overriddenTimestamp);
        }
    }

    // set initial snapshot time to unix epoch.
    private void doInitialSnapshotEventTimestampOverride(BinlogEventV4 event) {

        long overriddenTimestamp = event.getHeader().getTimestamp();

        if (overriddenTimestamp != 0) {
            overriddenTimestamp = 0;
            ((BinlogEventV4HeaderImpl)(event.getHeader())).setTimestamp(overriddenTimestamp);
        }
    }

    private void updateLastKnownPositionForMapEvent(TableMapEvent event) {

        String lastBinlogFileName;
        if (binlogPositionLastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION) != null) {
            lastBinlogFileName = ((BinlogPositionInfo) binlogPositionLastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogFilename();
        }
        else {
            lastBinlogFileName = "";
        }

        if (!event.getBinlogFilename().equals(lastBinlogFileName)) {
            LOGGER.info("moving to next binlog file [ " + lastBinlogFileName + " ] ==>> [ " + event.getBinlogFilename() + " ]");
        }

        binlogPositionLastKnownInfo.put(
                Constants.LAST_KNOWN_MAP_EVENT_POSITION,
                new BinlogPositionInfo(
                        event.getBinlogFilename(),
                        event.getHeader().getPosition()
                )
        );

        LOGGER.debug("Updated last known map event position to => ["
                + ((BinlogPositionInfo) binlogPositionLastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogFilename()
                + ":"
                + ((BinlogPositionInfo) binlogPositionLastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogPosition()
                + "]"
        );
    }

    private void updateLastKnownPosition(AbstractRowEvent event) {
        BinlogPositionInfo lastKnownPosition = new BinlogPositionInfo(
                event.getBinlogFilename(),
                event.getHeader().getPosition()
        );
        binlogPositionLastKnownInfo.put(Constants.LAST_KNOWN_BINLOG_POSITION, lastKnownPosition);
    }
}
