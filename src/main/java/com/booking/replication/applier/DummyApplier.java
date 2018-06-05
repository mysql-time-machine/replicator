package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.event.impl.*;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;

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
    public void applyBeginQueryEvent(BinlogEventQuery event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyCommitQueryEvent(BinlogEventQuery event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyXidEvent(BinlogEventXid event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyRotateEvent(BinlogEventRotate event) throws ApplierException, IOException {

    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {

    }

    @Override
    public void forceFlush() throws ApplierException, IOException {

    }

    @Override
    public void applyFormatDescriptionEvent(BinlogEventFormatDescription event) {

    }

    @Override
    public void applyTableMapEvent(BinlogEventTableMap event) {

    }

    @Override
    public void waitUntilAllRowsAreCommitted() throws IOException, ApplierException {

    }

    @Override
    public void applyPseudoGTIDEvent(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) throws Exception {

    }

    @Override
    public PseudoGTIDCheckpoint getLastCommittedPseudGTIDCheckPoint() {
        return null;
    }
}
