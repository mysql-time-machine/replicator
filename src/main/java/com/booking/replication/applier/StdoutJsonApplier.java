package com.booking.replication.applier;

import com.booking.replication.Configuration;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StdoutJsonApplier implements Applier  {

    private static long totalRowsCounter = 0;

    // TODO: move these to Cmd config params
    private static final String FILTERED_TABLE_NAME = null;
    private static final Boolean VERBOSE = true;
    private static final Boolean STATS_OUT = true;
    private static final Boolean DATA_OUT = true;
    private static final Boolean SCHEMA_OUT = false;
    private static final Boolean META_OUT = true;

    private static final Map<String, Long> stats = new ConcurrentHashMap<>();

    private PseudoGTIDCheckpoint lastCommittedPositionCheckpoint;

    private static final Logger LOGGER = LoggerFactory.getLogger(StdoutJsonApplier.class);

    public StdoutJsonApplier(Configuration configuration) {}

    @Override
    public void applyXidEvent(XidEvent event, CurrentTransaction currentTransaction) {
        if (VERBOSE) {
            for (String table : stats.keySet()) {
                LOGGER.info("XID Event, current stats: { table => " + table + ", rows => " + stats.get(table));
            }
        }
    }

    @Override
    public void waitUntilAllRowsAreCommitted(BinlogEventV4 event) {

        try {
            LOGGER.info("Sleeping as to simulate waiting for all rows being committed");
            Thread.sleep(1000);
            LOGGER.info("DONE.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void applyPseudoGTIDEvent(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) throws Exception {
        this.lastCommittedPositionCheckpoint = pseudoGTIDCheckPoint;
        if (META_OUT) {
            LOGGER.info("new pseudoGTID: " + lastCommittedPositionCheckpoint);

        }
    }

    @Override
    public PseudoGTIDCheckpoint getLastCommittedPseudGTIDCheckPoint() {
        return this.lastCommittedPositionCheckpoint;
    }

    @Override
    public SupportedAppliers.ApplierName getApplierName() throws ApplierException {
        return SupportedAppliers.ApplierName.StdoutJsonApplier;
    }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedRowsEvent, CurrentTransaction currentTransaction) {
        if (VERBOSE) {
            LOGGER.info("Row Event: number of rows in event => " + augmentedRowsEvent.getSingleRowEvents().size());
        }

        for (AugmentedRow row : augmentedRowsEvent.getSingleRowEvents()) {
            String tableName = row.getTableName();
            if (tableName != null) {
                if (FILTERED_TABLE_NAME != null) {
                    // track only specified table
                    if (row.getTableName().equals(FILTERED_TABLE_NAME)) {
                        totalRowsCounter++;
                        if (stats.containsKey(tableName)) {
                            stats.put(tableName, stats.get(tableName) + 1);
                        } else {
                            stats.put(tableName, 0L);
                        }
                        if (STATS_OUT) {
                            System.out.println(FILTERED_TABLE_NAME + ":" + totalRowsCounter);

                            if ((totalRowsCounter % 10000) == 0) {
                                LOGGER.info("totalRowsCounter => " + totalRowsCounter);
                                for (String table : stats.keySet()) {
                                    LOGGER.info("{ table => " + table + ", rows => " + stats.get(table));
                                }
                            }
                        }
                        if (DATA_OUT) {
                            System.out.println(row.toJson());
                        }
                    }
                } else {
                    // track all tables
                    totalRowsCounter++;
                    if (stats.containsKey(tableName)) {
                        stats.put(tableName, stats.get(tableName) + 1);
                    } else {
                        stats.put(tableName, 0L);
                    }
                    if (STATS_OUT) {
                        if ((totalRowsCounter % 10000) == 0) {
                            LOGGER.info("totalRowsCounter => " + totalRowsCounter);
                            for (String table : stats.keySet()) {
                                LOGGER.info("{ table => " + table + ", rows => " + stats.get(table));
                            }
                        }
                    }
                    if (DATA_OUT) {
                        System.out.println(row.toJson());
                    }
                }
            } else {
                LOGGER.error("table name in a row event can not be null");
            }
        }
    }

    @Override
    public void applyBeginQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {
        if (VERBOSE) {
            LOGGER.info("BEGIN");
        }
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {
        if (VERBOSE) {
            LOGGER.info("COMMIT");
            for (String table : stats.keySet()) {
                LOGGER.info("COMMIT, current stats: { table => " + table + ", rows => " + stats.get(table));
            }
        }
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller) {

        if (SCHEMA_OUT) {
            String json = augmentedSchemaChangeEvent.toJson();
            if (json != null) {
                System.out.println("Schema Change: augmentedSchemaChangeEvent => \n" + json);
            } else {
                LOGGER.error("Received empty schema change event");
            }
        }
    }

    @Override
    public void forceFlush() {
        LOGGER.info("force flush");
    }

    @Override
    public void applyRotateEvent(RotateEvent event) {
        LOGGER.info("binlog rotate: " + event.getBinlogFilename());
        LOGGER.info("STDOUTApplier totalRowsCounter => " + totalRowsCounter);
    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {

    }

    @Override
    public void applyTableMapEvent(TableMapEvent event) {

    }
}