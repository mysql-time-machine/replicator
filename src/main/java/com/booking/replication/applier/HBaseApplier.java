package com.booking.replication.applier;

import com.booking.replication.Constants;
import com.booking.replication.checkpoints.CheckPointTests;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.metrics.*;
import com.booking.replication.pipeline.PipelineOrchestrator;

import com.booking.replication.queues.ReplicatorQueues;
import com.booking.replication.schema.HBaseSchemaManager;

import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

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
    private static final int POOL_SIZE = 30;

    private static final int UUID_BUFFER_SIZE = 1000; // <- max number of rows in one uuid buffer

    private static final int BUFFER_FLUSH_INTERVAL = 60000; // <- force buffer flush every 60 sec

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplier.class);

    private static final Configuration hbaseConf = HBaseConfiguration.create();

    private final ReplicatorMetrics replicatorMetrics;

    private final HBaseSchemaManager hBaseSchemaManager;

    private final HBaseApplierWriter hbaseApplierWriter;

    private long timeOfLastFlush = 0;

    private final com.booking.replication.Configuration configuration;

    private final ReplicatorQueues queues;

    /**
     * HBaseApplier constructor
     *
     * @param ZOOKEEPER_QUORUM
     * @param repMetrics
     * @throws IOException
     */
    public HBaseApplier(

            ReplicatorQueues                      repQueues,
            String                                ZOOKEEPER_QUORUM,
            ReplicatorMetrics                     repMetrics,
            com.booking.replication.Configuration repCfg

        ) throws IOException {

        configuration     = repCfg;
        queues            = repQueues;
        replicatorMetrics = repMetrics;

        hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        hbaseConf.set("hbase.client.keyvalue.maxsize", "0");

        hbaseApplierWriter =
            new HBaseApplierWriter(
                repQueues,
                POOL_SIZE,
                repMetrics,
                hbaseConf,
                repCfg
            );

        hBaseSchemaManager = new HBaseSchemaManager(configuration.getHBaseQuorum());
    }

    /**
     * Applier interface methods
     *
     *  applyCommitQueryEvent
     *  applyXIDEvent
     *  applyAugmentedSchemaChangeEvent
     *
     *  applyAugmentedRowsEvent
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
                + hbaseApplierWriter.rowsBufferedInCurrentTask.get()
                + " rows before moving to the next binlog file.");
        LOGGER.info("Stats snapshot: ");
        dumpStats();
        markAndSubmit(); // mark current as ready; flush all;
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent e, PipelineOrchestrator caller) {
        hBaseSchemaManager.writeSchemaSnapshotToHBase(e, configuration);
    }

    /**
     * Core logic of the applier. Processes data events and writes to HBase.
     *
     * @param augmentedRowsEvent
     * @param pipeline
     */
    @Override
    public void applyAugmentedRowsEvent(final AugmentedRowsEvent augmentedRowsEvent, final PipelineOrchestrator pipeline) {

        String hbaseNamespace = getHBaseNamespace(pipeline);
        if (hbaseNamespace == null) return;

        //HBasePreparedAugmentedRowsEvent hBasePreparedAugmentedRowsEvent =
        //        new HBasePreparedAugmentedRowsEvent(hbaseNamespace, augmentedRowsEvent);

        // buffer
        hbaseApplierWriter.pushToCurrentTaskBuffer(augmentedRowsEvent);

        // flush on buffer size or time limit
        long currentTime = System.currentTimeMillis();
        long tDiff = currentTime - timeOfLastFlush;

        boolean forceFlush = (tDiff > BUFFER_FLUSH_INTERVAL);
        if ((hbaseApplierWriter.rowsBufferedInCurrentTask.get() >= UUID_BUFFER_SIZE) || forceFlush) {
            markAndSubmit();
        }
    }

    private String getHBaseNamespace(PipelineOrchestrator pipeline) {

        // get database name from event
        String mySQLDBName = configuration.getReplicantSchemaName();
        String currentTransactionDB = pipeline.currentTransactionMetadata.getFirstMapEventInTransaction().getDatabaseName().toString();

        String hbaseNamespace = null;
        if (currentTransactionDB != null) {
            if (currentTransactionDB.equals(mySQLDBName)) {
                hbaseNamespace = mySQLDBName.toLowerCase();
            }
            else if(currentTransactionDB.equals(Constants.BLACKLISTED_DB)) {
                return null;
            }
            else {
                LOGGER.error("Invalid database name: " + currentTransactionDB);
            }
        }
        else {
            LOGGER.error("CurrentTransactionDB can not be null");
        }
        return hbaseNamespace;
    }

    @Override
    public void forceFlush() {
        markAndSubmit();
    }

    private void markAndSubmit() {
        markCurrentTaskAsReadyToGo();
        submitAllTasksThatAreReadyToGo();
        timeOfLastFlush = System.currentTimeMillis();
    }

    @Override
    public void resubmitIfThereAreFailedTasks() {
        hbaseApplierWriter.markAllTasksAsReadyToGo();
        submitAllTasksThatAreReadyToGo();
        hbaseApplierWriter.updateTaskStatuses();
        timeOfLastFlush = System.currentTimeMillis();
    }

    // mark current uuid buffer as READY_FOR_PICK_UP and create new uuid buffer
    private void markCurrentTaskAsReadyToGo() {
        hbaseApplierWriter.markCurrentTaskAsReadyAndCreateNewUUIDBuffer();
    }

    private void submitAllTasksThatAreReadyToGo() {
        // Submit all tasks that are ready for pick up
        hbaseApplierWriter.submitTasksThatAreReadyForPickUp();
    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {
        LOGGER.info("Processing file " + event.getBinlogFilename());
        hbaseApplierWriter.initBuffers();
    }

    @Override
    public void waitUntilAllRowsAreCommitted(CheckPointTests checkPointTests) {
        boolean wait = true;

        while (wait) {

            Totals totals = replicatorMetrics.getTotalsSnapshot();
            BigInteger totalHBaseRowsAffected = totals.getTotalHbaseRowsAffected().getValue();

            // TODO: BigInteger totalMySQLRowsProcessed = totals.getMetricValue(Metric.TOTAL_ROWS_PROCESSED);
            BigInteger totalMySQLRowsProcessed = totals.getHbaseRowsAffected().getValue();

            LOGGER.info("hbaseTotalRowsCommited  => " + totalHBaseRowsAffected);
            LOGGER.info("mysqlTotalRowsProcessed => " + totalMySQLRowsProcessed);

            if (checkPointTests.verifyConsistentCountersOnRotateEvent(totalHBaseRowsAffected, totalMySQLRowsProcessed)) {
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

    private void markCurrentTransactionForCommit() {
        hbaseApplierWriter.markCurrentTransactionForCommit();
    }

    @Override
    public void dumpStats() {

        Map<Integer, TotalsPerTimeSlot> metricsSnapshot = replicatorMetrics.getMetricsSnapshot();

        for (Integer timebucket : metricsSnapshot.keySet()) {

            LOGGER.debug("dumping stats for bucket => " + timebucket);

            TotalsPerTimeSlot timebucketStats;
            timebucketStats = replicatorMetrics.getMetricsSnapshot().get(timebucket);

            INameValue[] metricNamesAndValues = timebucketStats.getAllNamesAndValues();

            for (int i = 0; i < metricNamesAndValues.length; i++)
            {
                LOGGER.info(String.format("%s => %s @ %s", metricNamesAndValues[i].getName(), metricNamesAndValues[i].getValue(),
                        timebucket));
            }
        }
    }
}
