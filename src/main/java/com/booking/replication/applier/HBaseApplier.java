package com.booking.replication.applier;

import com.booking.replication.Constants;
import com.booking.replication.audit.CheckPointTests;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.metrics.Metric;
import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.pipeline.PipelineOrchestrator;

import com.booking.replication.schema.TableNameMapper;
import com.booking.replication.util.MutableLong;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class abstracts the HBase store.
 *
 * Conventions used:
 *
 *      1. Each replication chain is replicated to a namespace "${chain_name}_replication".
 *
 *      2. All table names are converted to low-caps. For example My_Schema.My_Table will be replicated
 *         to 'my_schema:my_table'
 */
public class HBaseApplier implements Applier {

    // TODO: move configuration vars to Configuration
    private static final int UUID_BUFFER_SIZE = 5000; // <- max number of rows in one uuid buffer
    private static final int POOL_SIZE = 44;
    private static final int BUFFER_FLUSH_INTERVAL = 30000; // <- force buffer flush every 30 sec

    private static final String DIGEST_ALGORITHM = "MD5";

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplier.class);

    private static final Configuration hbaseConf = HBaseConfiguration.create();

    private static final byte[] CF = Bytes.toBytes("d");

    private final ReplicatorMetrics replicatorMetrics;

    private final HBaseApplierTaskManager hbaseApplierTaskManager;

    private long timeOfLastFlush = 0;

    private final com.booking.replication.Configuration replicatorConfiguration;

    /**
     * HBaseApplier constructor
     *
     * @param ZOOKEEPER_QUORUM
     * @param repMetrics
     * @throws IOException
     */
    public HBaseApplier(
            String ZOOKEEPER_QUORUM,
            ReplicatorMetrics repMetrics,
            com.booking.replication.Configuration repCfg
            ) throws IOException {
        hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        this.replicatorMetrics = repMetrics;
        hbaseApplierTaskManager = new HBaseApplierTaskManager(POOL_SIZE, repMetrics, hbaseConf);
        replicatorConfiguration = repCfg;
    }

    /**
     * Applier interface methods
     *
     *  applyCommitQueryEvent
     *  applyXIDEvent
     *  applyAugmentedSchemaChangeEvent
     *
     *  bufferData
     *
     * @param event
     */

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {
        markCurrentTransactionForCommit();
    }

    @Override
    public void applyXIDEvent(XidEvent event) {
        // TODO: add transactionID to storage
        // long transactionID = event.getXid();
        markCurrentTransactionForCommit();
    }

    @Override
    public void applyRotateEvent(RotateEvent event) {
        LOGGER.info("binlog rotate ["
                + event.getBinlogFilename()
                + "], flushing buffer of "
                + hbaseApplierTaskManager.rowsBufferedInCurrentTask.get()
                + " rows before moving to the next binlog file.");
        LOGGER.info("Stats snapshot: ");
        dumpStats();
        markAndSubmit(); // mark current as ready; flush all;
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent e, PipelineOrchestrator caller) {

        // TODO:

        // 1. read database_name

        // 2. read table_name

        // 3. read sql_statement

        // 4. read old/new jsons

        // 5. read old/new creates

        // 6. construct Put object with:
        //      row_key = 'database_name;event_timestamp'

        // 7. Write to table:
        //      'schema_replication:schema_version_history'
    }

    /**
     * Core logic of the applier. Processes data events and writes to HBase.
     *
     * @param augmentedRowsEvent
     * @param pipeline
     */
    @Override
    public void bufferData(AugmentedRowsEvent augmentedRowsEvent, PipelineOrchestrator pipeline) {

        // 1. get database name from event
        String mySQLDBName = pipeline.configuration.getReplicantSchemaName();

        String currentTransactionDB = pipeline.currentTransactionMetadata.getFirstMapEventInTransaction().getDatabaseName().toString();

        String hbaseNamespace = null;
        if (currentTransactionDB != null) {

            if (currentTransactionDB.equals(mySQLDBName)) {
                // regular data
                hbaseNamespace = mySQLDBName.toLowerCase();
            }
            else if(currentTransactionDB.equals(Constants.BLACKLISTED_DB)) {
                // skipping blacklisted db
                return;
            }
            else {
                LOGGER.error("Invalid database name: " + currentTransactionDB);
            }
        }
        else {
            LOGGER.error("CurrentTransactionDB can not be null");
        }

        if (hbaseNamespace == null) {
            LOGGER.error("Namespace can not be null");
        }

        // 2. prepare and buffer Put objects for all rows in the received event
        for (AugmentedRow row : augmentedRowsEvent.getSingleRowEvents()) {

            // ==============================================================================
            // I. Mirrored table

            // get table_name from event
            String mySQLTableName = row.getTableName();
            String hbaseTableName = hbaseNamespace + ":" + mySQLTableName.toLowerCase();

            Pair<String,Put> rowIDPutPair = getPutForMirroredTable(row);

            String rowID = rowIDPutPair.getFirst();
            Put p = rowIDPutPair.getSecond();

            // Push to buffer
            hbaseApplierTaskManager.pushMutationToTaskBuffer(hbaseTableName, rowID, p);

            // ==============================================================================
            // II. Optional Delta table used for incremental imports to Hive
            //
            // Delta tables have 2 important differences from mirrored tables:
            //
            // 1. Columns have only 1 version
            //
            // 2. we are storing the entire row (instead only the changes columns - since 1.)
            //
            List<String> tablesForDelta = replicatorConfiguration.getTablesForWhichToTrackDailyChanges();

            if (pipeline.configuration.isWriteRecentChangesToDeltaTables()
                    && tablesForDelta.contains(mySQLTableName)) {

                boolean isInitialSnapshot = pipeline.configuration.isInitialSnapshotMode();
                String  replicantSchema   = pipeline.configuration.getReplicantSchemaName();
                String  mysqlTableName    = row.getTableName();
                Long    timestampMicroSec = row.getEventV4Header().getTimestamp();

                String deltaTableName = TableNameMapper.getCurrentDeltaTableName(
                        timestampMicroSec,
                        replicantSchema,
                        mysqlTableName,
                        isInitialSnapshot
                    );

                Pair<String, Put> deltaPair = getPutForDeltaTable(row);
                String deltaRowID           = deltaPair.getFirst();
                Put deltaPut                = deltaPair.getSecond();

                hbaseApplierTaskManager.pushMutationToTaskBuffer(deltaTableName, deltaRowID, deltaPut);
            }

        } // next row

        // Flush on buffer size or time limit
        long currentTime = System.currentTimeMillis();
        long tDiff = currentTime - timeOfLastFlush;
        boolean forceFlush = (tDiff > BUFFER_FLUSH_INTERVAL);
        if ((hbaseApplierTaskManager.rowsBufferedInCurrentTask.get() >= UUID_BUFFER_SIZE) || forceFlush) {
            markAndSubmit();
        }
    }

    @Override
    public void forceFlush() {
        markAndSubmit();
    }

    // TODO: find better name for this
    private void markAndSubmit() {
        markCurrentTaskAsReadyToGo();
        submitAllTasksThatAreReadyToGo(); // TODO: implement this in a while loop in a separate thread
        timeOfLastFlush = System.currentTimeMillis();
    }

    // TODO: find better name for this
    @Override
    public void resubmitIfThereAreFailedTasks() {
        hbaseApplierTaskManager.markAllTasksAsReadyToGo();
        submitAllTasksThatAreReadyToGo(); // TODO: implement this in a while loop in a separate thread
        hbaseApplierTaskManager.updateTaskStatuses();
        timeOfLastFlush = System.currentTimeMillis();
    }

    // mark current uuid buffer as READY_FOR_PICK_UP and create new uuid buffer
    private void markCurrentTaskAsReadyToGo() {
        hbaseApplierTaskManager.markCurrentTaskAsReadyAndCreateNewUUIDBuffer();
    }

    private void submitAllTasksThatAreReadyToGo() {
        // Submit all tasks that are ready for pick up
        hbaseApplierTaskManager.submitTasksThatAreReadyForPickUp();
    }

    private Pair<String,Put> getPutForMirroredTable(AugmentedRow row) {

        // RowID
        List<String> pkColumnNames = row.getPrimaryKeyColumns(); // <- this is sorted by column OP
        List<String> pkColumnValues = new ArrayList<String>();

        // LOGGER.info("table => " + mySQLTableName + ", pk columns => " + Joiner.on(";").join(pkColumnNames));

        for (String pkColumnName : pkColumnNames) {

            Map<String, String> pkCell = row.getEventColumns().get(pkColumnName);

            if (row.getEventType().equals("INSERT") || row.getEventType().equals("DELETE")) {
                pkColumnValues.add(pkCell.get("value"));
            } else if (row.getEventType().equals("UPDATE")) {
                pkColumnValues.add(pkCell.get("value_after"));
            } else {
                LOGGER.error("Wrong event type. Expected RowType event.");
            }
        }

        String hbaseRowID = Joiner.on(";").join(pkColumnValues);
        String saltingPartOfKey = pkColumnValues.get(0);

        // avoid region hot-spotting
        hbaseRowID = saltRowKey(hbaseRowID, saltingPartOfKey);

        Put p = new Put(Bytes.toBytes(hbaseRowID));

        if (row.getEventType().equals("DELETE")) {

            // No need to process columns on DELETE. Only write delete marker.

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnName  = "row_status";
            String columnValue = "D";
            p.addColumn(
                    CF,
                    Bytes.toBytes(columnName),
                    columnTimestamp,
                    Bytes.toBytes(columnValue)
            );
        }
        else if (row.getEventType().equals("UPDATE")) {

            // Only write values that have changed

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnValue;

            for (String columnName : row.getEventColumns().keySet()) {

                String valueBefore = row.getEventColumns().get(columnName).get("value_before");
                String valueAfter  = row.getEventColumns().get(columnName).get("value_after");

                if ((valueAfter == null) && (valueBefore == null)) {
                    // no change, skip;
                }
                else if (
                        ((valueBefore == null) && (valueAfter != null))
                        ||
                        ((valueBefore != null) && (valueAfter == null))
                        ||
                        (!valueAfter.equals(valueBefore))) {

                    columnValue = valueAfter;
                    p.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(columnValue)
                    );
                }
                else {
                    // no change, skip
                }
            }

            p.addColumn(
                    CF,
                    Bytes.toBytes("row_status"),
                    columnTimestamp,
                    Bytes.toBytes("U")
            );
        }
        else if (row.getEventType().equals("INSERT")) {

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnValue;

            for (String columnName : row.getEventColumns().keySet()) {

                columnValue = row.getEventColumns().get(columnName).get("value");
                if (columnValue == null) {
                    columnValue = "NULL";
                }

                p.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
            }


            p.addColumn(
                CF,
                Bytes.toBytes("row_status"),
                columnTimestamp,
                Bytes.toBytes("I")
            );
        }
        else {
            LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
            System.exit(1);
        }

        Pair<String,Put> idPut = new Pair<>(hbaseRowID,p);
        return idPut;
    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {
        LOGGER.info("Processing file " + event.getBinlogFilename());
        hbaseApplierTaskManager.initBuffers();
    }

    @Override
    public void waitUntilAllRowsAreCommitted(CheckPointTests checkPointTests) {

        ConcurrentHashMap<Integer, MutableLong> totals = replicatorMetrics.getTotals();

        if (totals != null) {

            boolean wait = true;

            while (wait) {

                long committedRows = totals.get(Metric.TOTAL_ROW_OPS_SUCCESSFULLY_COMMITED).getValue();
                long processedRows = totals.get(Metric.TOTAL_ROWS_PROCESSED).getValue();

                LOGGER.info("hbaseTotalRowsCommited  => " + committedRows);
                LOGGER.info("mysqlTotalRowsProcessed => " + processedRows);

                if (checkPointTests.verifyConsistentCountersOnRotateEvent(committedRows, processedRows)) {
                    wait = false;
                }
                else {
                    resubmitIfThereAreFailedTasks();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else {
            LOGGER.error("Could not retrieve totals metrics. Internal monitoring lost. Exiting...");
            System.exit(1);
        }
    }

    private Pair<String,Put> getPutForDeltaTable(AugmentedRow row) {

        // TODO: on schema change delta table should also change its name so we can easily
        //       map it in Hive (one schema per table)

        // RowID
        List<String> pkColumnNames = row.getPrimaryKeyColumns(); // <- this is sorted by column OP
        List<String> pkColumnValues = new ArrayList<String>();

        for (String pkColumnName : pkColumnNames) {

            Map<String, String> pkCell = row.getEventColumns().get(pkColumnName);

            if (row.getEventType().equals("INSERT") || row.getEventType().equals("DELETE")) {
                pkColumnValues.add(pkCell.get("value"));
            } else if (row.getEventType().equals("UPDATE")) {
                pkColumnValues.add(pkCell.get("value_after"));
            } else {
                LOGGER.error("Wrong event type. Expected RowType event.");
                // TODO: throw WrongEventTypeException
            }
        }

        String hbaseRowID = Joiner.on(";").join(pkColumnValues);
        String saltingPartOfKey = pkColumnValues.get(0);

        // avoid region hot-spotting
        hbaseRowID = saltRowKey(hbaseRowID, saltingPartOfKey);

        Put p = new Put(Bytes.toBytes(hbaseRowID));

        if (row.getEventType().equals("DELETE")) {

            // For delta tables in case of DELETE, just write a delete marker

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnName  = "row_status";
            String columnValue = "D";
            p.addColumn(
                    CF,
                    Bytes.toBytes(columnName),
                    columnTimestamp,
                    Bytes.toBytes(columnValue)
            );
        }
        else if (row.getEventType().equals("UPDATE")) {

            // for delta tables write the latest version of the entire row

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnValue;

            for (String columnName : row.getEventColumns().keySet()) {

                String valueAfter  = row.getEventColumns().get(columnName).get("value_after");

                columnValue = valueAfter;
                p.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
            }

            p.addColumn(
                    CF,
                    Bytes.toBytes("row_status"),
                    columnTimestamp,
                    Bytes.toBytes("U")
            );
        }
        else if (row.getEventType().equals("INSERT")) {

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnValue;

            for (String columnName : row.getEventColumns().keySet()) {

                columnValue = row.getEventColumns().get(columnName).get("value");
                if (columnValue == null) {
                    columnValue = "NULL";
                }

                p.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
            }

            p.addColumn(
                    CF,
                    Bytes.toBytes("row_status"),
                    columnTimestamp,
                    Bytes.toBytes("I")
            );
        }
        else {
            LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
            System.exit(1);
        }

        Pair<String,Put> idPut = new Pair<>(hbaseRowID,p);
        return idPut;
    }

    private void markCurrentTransactionForCommit() {
        hbaseApplierTaskManager.markCurrentTransactionForCommit();
    }


    /**
     * Salting the row keys with hex representation of first two bytes of md5:
     *      hbaseRowID = md5(hbaseRowID)[0] + md5(hbaseRowID)[1] + "-" + hbaseRowID;
     */
     private String saltRowKey(String hbaseRowID, String firstPartOfRowKey) {

         byte[] bytesOfSaltingPartOfRowKey = firstPartOfRowKey.getBytes(StandardCharsets.US_ASCII);

         MessageDigest md = null;
         try {
            md = MessageDigest.getInstance(DIGEST_ALGORITHM);
         } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            LOGGER.error("md5 algorithm not available. Shutting down...");
            System.exit(1);
         }
         byte[] bytes_md5 = md.digest(bytesOfSaltingPartOfRowKey);

         String byte_1_hex = Integer.toHexString(bytes_md5[0] & 0xFF);
         String byte_2_hex = Integer.toHexString(bytes_md5[1] & 0xFF);

         //String salt = "00".substring(0,2 - byte_1_hex.length()) + byte_1_hex
         //            + "00".substring(0,2 - byte_2_hex.length()) + byte_2_hex;

         // add 0-padding
         String salt = ("00" + byte_1_hex).substring(byte_1_hex.length())
                     + ("00" + byte_2_hex).substring(byte_2_hex.length());

         String saltedRowKey = salt + "-" + hbaseRowID;

         return saltedRowKey;
    }

    @Override
    public void dumpStats() {

        for (Integer timebucket : replicatorMetrics.getMetrics().keySet()) {

            LOGGER.debug("dumping stats for bucket => " + timebucket);

            HashMap<Integer,MutableLong> timebucketStats;
            timebucketStats = replicatorMetrics.getMetrics().get(timebucket);

            if (timebucketStats != null) {
                // all is good
            }
            else {
                LOGGER.warn("Metrics missing for timebucket " + timebucket);
                return;
            }

            for (Integer metricsID : timebucketStats.keySet()) {

                Long value = timebucketStats.get(metricsID).getValue();

                String pointInfo = Metric.getCounterName(metricsID)
                                + " => " + value.toString()
                                + " @ " + timebucket.toString();

                LOGGER.info(pointInfo);
            }
        }
    }
}
