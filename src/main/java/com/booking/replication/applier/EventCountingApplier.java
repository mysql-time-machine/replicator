package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.codahale.metrics.Counter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;

import java.io.IOException;

/**
 * Wraps an applier to count incoming events
 */
public class EventCountingApplier implements Applier {

    private final Applier wrapped;
    private final Counter counter;

    public EventCountingApplier(Applier wrapped, Counter counter)
        {
            if (wrapped == null)
            {
                throw new IllegalArgumentException("wrapped must not be null");
            }

            if (counter == null)
            {
                throw new IllegalArgumentException("counter must not be null");
            }

            this.wrapped = wrapped;
            this.counter = counter;
        }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller) throws ApplierException, IOException {
        wrapped.applyAugmentedRowsEvent(augmentedSingleRowEvent, caller);
        counter.inc();
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {
        wrapped.applyCommitQueryEvent(event);
        counter.inc();
    }

    @Override
    public void applyXidEvent(XidEvent event) {
        wrapped.applyXidEvent(event);
        counter.inc();
    }

    @Override
    public void applyRotateEvent(RotateEvent event) throws ApplierException, IOException {
        wrapped.applyRotateEvent(event);
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {
        wrapped.applyAugmentedSchemaChangeEvent(augmentedSchemaChangeEvent, caller);
        counter.inc();
    }

    @Override
    public void forceFlush() throws ApplierException, IOException {
        wrapped.forceFlush();
    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {
        wrapped.applyFormatDescriptionEvent(event);
        counter.inc();
    }

    @Override
    public void applyTableMapEvent(TableMapEvent event) {
        wrapped.applyTableMapEvent(event);
        counter.inc();
    }

    @Override
    public void waitUntilAllRowsAreCommitted(BinlogEventV4 event) throws IOException, ApplierException {
        wrapped.waitUntilAllRowsAreCommitted(event);
    }
}
