package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;

import java.io.IOException;

/**
 * Created by edmitriev on 8/2/17.
 */
public class DummyApplier implements Applier {
    @Override
    public SupportedAppliers.ApplierName getApplierName() throws ApplierException {
        return SupportedAppliers.ApplierName.DummyApplier;
    }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction) throws ApplierException, IOException {

    }

    @Override
    public void applyBeginQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyXidEvent(XidEvent event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyRotateEvent(RotateEvent event) throws ApplierException, IOException {

    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {

    }

    @Override
    public void forceFlush() throws ApplierException, IOException {

    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {

    }

    @Override
    public void applyTableMapEvent(TableMapEvent event) {

    }

    @Override
    public void waitUntilAllRowsAreCommitted(BinlogEventV4 event) throws IOException, ApplierException {

    }
}
