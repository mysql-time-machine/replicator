package com.booking.replication.applier;

import com.booking.replication.Constants;
import com.booking.replication.applier.hbase.HBaseApplierWriter;
import com.booking.replication.applier.hbase.TaskBufferInconsistencyException;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.HBaseSchemaManager;
import com.booking.replication.schema.TableNameMapper;
import com.booking.replication.validation.ValidationService;
import com.codahale.metrics.Counter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class abstracts the HBase store.
 * <p>
 * Conventions used:
 *
 *      1. Each replication chain is replicated to a namespace "${chain_name}_replication".
 *
 *      2. All table names are converted to low-caps. For example My_Schema.My_Table will be replicated
 *         to 'my_schema:my_table'
 * </p>
 */
public class HBaseApplier implements Applier {

    // TODO: move configuration vars to Configuration
    private static final int POOL_SIZE = 30;

    private static final int UUID_BUFFER_SIZE = 1000; // <- max number of rows in one uuid buffer

    private static final int BUFFER_FLUSH_INTERVAL = 60000; // <- force buffer flush every 60 sec

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplier.class);

    private static final int DEFAULT_VERSIONS_FOR_MIRRORED_TABLES = 1000;

    private final HBaseSchemaManager hbaseSchemaManager;

    private final HBaseApplierWriter hbaseApplierWriter;

    private long timeOfLastFlush = 0;

    private final com.booking.replication.Configuration configuration;

    /**
     * HBaseApplier constructor.
     * @param config config
     * @param mainProgressIndicator mainProgressIndicator
     * @param validationService validationService
     */
    public HBaseApplier(
        com.booking.replication.Configuration config,
        Counter mainProgressIndicator,
        ValidationService validationService
    ) {
        configuration = config;

        hbaseApplierWriter =
            new HBaseApplierWriter(
                    POOL_SIZE,
                    configuration,
                    mainProgressIndicator,
                    validationService
            );

        hbaseSchemaManager = new HBaseSchemaManager(
                configuration.getHBaseQuorum(),
                configuration.isDryRunMode(),
                configuration
        );
    }

    @Override
    public void applyBeginQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {
        markCurrentTransactionForCommit();
    }

    @Override
    public void applyXidEvent(XidEvent event, CurrentTransaction currentTransaction) {
        // TODO: add transactionID to storage
        // long transactionID = event.getXid();
        markCurrentTransactionForCommit();
    }

    @Override
    public void applyRotateEvent(RotateEvent event) throws ApplierException, IOException {
        LOGGER.info("binlog rotate ["
                + event.getBinlogFilename()
                + "], flushing buffer of "
                + hbaseApplierWriter.rowsBufferedInCurrentTask.get()
                + " rows before moving to the next binlog file.");
        LOGGER.info("Stats snapshot: ");
        markAndSubmit(); // mark current as ready; flush all;
    }

    public PseudoGTIDCheckpoint getLastCommittedPseudGTIDCheckPoint() {
        return HBaseApplierWriter.getLatestCommittedPseudoGTIDCheckPoint();
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent event,
            PipelineOrchestrator caller) {
        hbaseSchemaManager.writeSchemaSnapshotToHBase(event, configuration);
    }

    @Override
    public void applyPseudoGTIDEvent(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) throws Exception {
        hbaseApplierWriter.markCurrentTaskWithPseudoGTID(pseudoGTIDCheckPoint);
    }

    @Override
    public SupportedAppliers.ApplierName getApplierName() throws ApplierException {
        return SupportedAppliers.ApplierName.HBaseApplier;
    }

    /**
     * Core logic of the applier. Processes data events and writes to HBase.
     *  @param augmentedRowsEvent Rows event
     * @param currentTransaction Current transaction metadata instance
     */
    @Override
    public void applyAugmentedRowsEvent(
            final AugmentedRowsEvent augmentedRowsEvent,
            final CurrentTransaction currentTransaction) throws ApplierException, IOException {

        String mysqlTableName = augmentedRowsEvent.getMysqlTableName();
        if (configuration.getTablesToSkip().contains(mysqlTableName)) {
            return;
        }

        String hbaseNamespace = getHBaseNamespace(currentTransaction);
        if (hbaseNamespace == null) {
            return;
        }

        // buffer
        try {
            hbaseApplierWriter.pushToCurrentTaskBuffer(augmentedRowsEvent);
        } catch (TaskBufferInconsistencyException e) {
            throw new ApplierException(e);
        }

        // flush on buffer size or time limit
        long currentTime = System.currentTimeMillis();
        long tdiff = currentTime - timeOfLastFlush;

        boolean forceFlush = (tdiff > BUFFER_FLUSH_INTERVAL);
        if ((hbaseApplierWriter.rowsBufferedInCurrentTask.get() >= UUID_BUFFER_SIZE) || forceFlush) {
            markAndSubmit();
        }
    }

    private String getHBaseNamespace(CurrentTransaction currentTransaction) {

        // get database name from event
        String mySqlDbName = configuration.getReplicantSchemaName();
        String currentTransactionDB = currentTransaction
                .getFirstMapEventInTransaction()
                .getDatabaseName()
                .toString();

        String hbaseNamespace = null;

        if (currentTransactionDB.equals(mySqlDbName)) {
            hbaseNamespace = mySqlDbName.toLowerCase();
        } else if (currentTransactionDB.equals(Constants.BLACKLISTED_DB)) {
            return null;
        } else {
            LOGGER.error("Invalid database name: " + currentTransactionDB);
        }

        return hbaseNamespace;
    }

    @Override
    public void forceFlush() throws ApplierException, IOException {
        markAndSubmit();
    }

    private void markAndSubmit() throws ApplierException, IOException {
        markCurrentTaskAsReadyToGo();
        submitAllTasksThatAreReadyToGo();
        timeOfLastFlush = System.currentTimeMillis();
    }

    private void resubmitIfThereAreFailedTasks() throws IOException, ApplierException {
        submitAllTasksThatAreReadyToGo();
        hbaseApplierWriter.updateTaskStatuses();
        timeOfLastFlush = System.currentTimeMillis();
    }

    // mark current uuid buffer as READY_FOR_PICK_UP and create new uuid buffer
    private void markCurrentTaskAsReadyToGo() throws ApplierException, IOException {
        try {
            hbaseApplierWriter.markCurrentTaskAsReadyAndCreateNewUuidBuffer();
        } catch (TaskBufferInconsistencyException te) {
            throw new ApplierException(te);
        }
    }

    private void submitAllTasksThatAreReadyToGo() throws IOException, ApplierException {
        // Submit all tasks that are ready for pick up
        try {
            hbaseApplierWriter.submitTasksThatAreReadyForPickUp();
        } catch (TaskBufferInconsistencyException te) {
            throw new ApplierException(te);
        }
    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {
        LOGGER.info("Processing file " + event.getBinlogFilename());
    }

    @Override
    public void applyTableMapEvent(TableMapEvent event) {

        String tableName = event.getTableName().toString();

        String hbaseTableName = configuration.getHbaseNamespace().toLowerCase()
                + ":"
                + tableName.toLowerCase();

        if (! hbaseSchemaManager.isTableKnownToHBase(hbaseTableName)) {
            // This should not happen in tableMapEvent, unless we are
            // replaying the binlog.
            // TODO: load hbase tables on start-up so this never happens
            hbaseSchemaManager.createMirroredTableIfNotExists(hbaseTableName, DEFAULT_VERSIONS_FOR_MIRRORED_TABLES);
        }

        if (configuration.isWriteRecentChangesToDeltaTables()) {

            //String replicantSchema = ((TableMapEvent) event).getDatabaseName().toString();
            String mysqlTableName = ((TableMapEvent) event).getTableName().toString();

            if (configuration.getTablesForWhichToTrackDailyChanges().contains(mysqlTableName)) {

                long eventTimestampMicroSec = event.getHeader().getTimestamp();

                String deltaTableName = TableNameMapper.getCurrentDeltaTableName(
                        eventTimestampMicroSec,
                        configuration.getHbaseNamespace(),
                        mysqlTableName,
                        configuration.isInitialSnapshotMode());
                if (! hbaseSchemaManager.isTableKnownToHBase(deltaTableName)) {
                    boolean isInitialSnapshotMode = configuration.isInitialSnapshotMode();
                    hbaseSchemaManager.createDeltaTableIfNotExists(deltaTableName, isInitialSnapshotMode);
                }
            }
        }
    }

    @Override
    public void waitUntilAllRowsAreCommitted() throws IOException, ApplierException {
        boolean wait = true;

        while (wait) {
            if (hbaseApplierWriter.areAllTasksDone()) {
                LOGGER.debug("All tasks have completed!");
                wait = false;
            } else {
                resubmitIfThereAreFailedTasks();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void markCurrentTransactionForCommit() {
        hbaseApplierWriter.markCurrentTransactionForCommit();
    }

}
