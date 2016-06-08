package com.booking.replication.applier;

import com.booking.replication.Configuration;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.util.MutableLong;
import com.booking.replication.pipeline.PipelineOrchestrator;

import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

import java.util.*;
import java.io.IOException;

/**
 * Created by raynald on 08/06/16.
 */

public class KafkaApplier implements Applier {
    private final com.booking.replication.Configuration replicatorConfiguration;
    private static long totalEventsCounter = 0;
    private static long totalRowsCounter = 0;

    private static final HashMap<String, MutableLong> stats = new HashMap<String, MutableLong>();

    public KafkaApplier(Configuration configuration) {
        replicatorConfiguration = configuration;
    }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller) throws IOException {
        totalEventsCounter ++;

        for(AugmentedRow row : augmentedSingleRowEvent.getSingleRowEvents()) {
            String tableName = row.getTableName();
            if (tableName != null) {
                totalRowsCounter ++;
                if (stats.containsKey(tableName)) {
                    stats.get(tableName).increment();
                } else {
                    stats.put(tableName, new MutableLong());
                }
            }
            System.out.println(row.toJSON());
        }
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {

    }

    @Override
    public void applyXIDEvent(XidEvent event) {

    }

    @Override
    public void applyRotateEvent(RotateEvent event) {

    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {

    }

    @Override
    public void forceFlush() {

    }

    @Override
    public void resubmitIfThereAreFailedTasks() {

    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {

    }

    @Override
    public void waitUntilAllRowsAreCommitted() {

    }
}
