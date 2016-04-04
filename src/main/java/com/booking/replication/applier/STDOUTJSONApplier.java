package com.booking.replication.applier;

import com.booking.replication.Configuration;
import com.booking.replication.audit.CheckPointTests;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.metrics.Metric;
import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.util.MutableLong;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class STDOUTJSONApplier implements Applier {

    private static long totalEventsCounter = 0;
    private static long totalRowsCounter = 0;

    // TODO: move these to CMD config params
    public static final String FILTERED_TABLE_NAME = null;
    public static final Boolean VERBOSE = false;
    public static final Boolean STATS_OUT = true;
    public static final Boolean DATA_OUT = false;
    public static final Boolean SCHEMA_OUT = false;

    private static final HashMap<String, MutableLong> stats = new HashMap<String, MutableLong>();

    private final ReplicatorMetrics replicatorMetrics;
    private final com.booking.replication.Configuration replicatorConfiguration;

    private static final Logger LOGGER = LoggerFactory.getLogger(STDOUTJSONApplier.class);

    public STDOUTJSONApplier(ReplicatorMetrics repMetrics, Configuration configuration) {
        replicatorMetrics = repMetrics;
        replicatorConfiguration = configuration;
    }

    @Override
    public void applyXIDEvent(XidEvent event) {
        if (VERBOSE) {
            for (String table : stats.keySet()) {
                LOGGER.info("XID Event, current stats: { table => " + table + ", rows => " + stats.get(table).getValue());
            }
        }
    }

    @Override
    public void resubmitIfThereAreFailedTasks() {

    }

    @Override
    public void waitUntilAllRowsAreCommitted(CheckPointTests checkPointTests) {

    }

    @Override
    public void bufferData(AugmentedRowsEvent augmentedRowsEvent, PipelineOrchestrator caller) {
        totalEventsCounter++;

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
                            stats.get(tableName).increment();
                        } else {
                            stats.put(tableName, new MutableLong());
                        }
                        if (STATS_OUT) {
                            System.out.println(FILTERED_TABLE_NAME + ":" + totalRowsCounter);

                            if ((totalRowsCounter % 10000) == 0) {
                                LOGGER.info("totalRowsCounter => " + totalRowsCounter);
                                for (String table : stats.keySet()) {
                                    LOGGER.info("{ table => " + table + ", rows => " + stats.get(table).getValue());
                                }
                            }
                        }
                        if (DATA_OUT) {
                            System.out.println(row.toJSON());
                        }
                    }
                } else {
                    // track all tables
                    totalRowsCounter++;
                    if (stats.containsKey(tableName)) {
                        stats.get(tableName).increment();
                    } else {
                        stats.put(tableName, new MutableLong());
                    }
                    if (STATS_OUT) {
                        if ((totalRowsCounter % 10000) == 0) {
                            LOGGER.info("totalRowsCounter => " + totalRowsCounter);
                            for (String table : stats.keySet()) {
                                LOGGER.info("{ table => " + table + ", rows => " + stats.get(table).getValue());
                            }
                        }
                    }
                    if (DATA_OUT) {
                        System.out.println(row.toJSON());
                    }
                }
            } else {
                LOGGER.error("table name in a row event can not be null");
            }


        }
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {
        if (VERBOSE) {
            LOGGER.info("COMMIT");
            for (String table : stats.keySet()) {
                LOGGER.info("COMMIT, current stats: { table => " + table + ", rows => " + stats.get(table).getValue());
            }
        }
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {

        totalEventsCounter++;

        if (SCHEMA_OUT) {
            String json = augmentedSchemaChangeEvent.toJSON();
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
        dumpStats();
    }

    @Override
    public void dumpStats() {

        List<Integer> timebuckets = new ArrayList<Integer>(replicatorMetrics.getMetrics().keySet());
        Collections.sort(timebuckets, Collections.reverseOrder());

        LOGGER.info("Available time buckets:");
        for (Integer t : timebuckets) {
            LOGGER.info("\tbucket => " + t);
        }

        Integer timebucket = timebuckets.get(0);

        LOGGER.info("dumping stats for latest time bucket => " + timebucket);

        HashMap<Integer, MutableLong> timebucketStats;
        timebucketStats = replicatorMetrics.getMetrics().get(timebucket);

        if (timebucketStats != null) {
            // all is good
        } else {
            LOGGER.error("Metrics missing for timebucket " + timebucket);
            return;
        }

        for (Integer metricsID : timebucketStats.keySet()) {

            Long value = timebucketStats.get(metricsID).getValue();

            String pointInfo = Metric.getCounterName(metricsID)
                    + " => " + value.toString()
                    + " @ " + timebucket.toString();

            LOGGER.info("\t" + pointInfo);
        }
    }
    
    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {

    }
}