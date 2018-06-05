package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.event.impl.*;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.pipeline.CurrentTransaction;

import com.booking.replication.pipeline.PipelineOrchestrator;

import java.io.IOException;

public interface Applier {

    SupportedAppliers.ApplierName getApplierName() throws ApplierException;

    void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction)
            throws ApplierException, IOException;

    void applyBeginQueryEvent(BinlogEventQuery event, CurrentTransaction currentTransaction);

    void applyCommitQueryEvent(BinlogEventQuery event, CurrentTransaction currentTransaction);

    void applyXidEvent(BinlogEventXid event, CurrentTransaction currentTransaction);

    void applyRotateEvent(BinlogEventRotate event) throws ApplierException, IOException;

    void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller);

    void forceFlush() throws ApplierException, IOException;

    void applyFormatDescriptionEvent(BinlogEventFormatDescription event);

    void applyTableMapEvent(BinlogEventTableMap event);

    void waitUntilAllRowsAreCommitted() throws IOException, ApplierException;

    void applyPseudoGTIDEvent(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) throws Exception;

    PseudoGTIDCheckpoint getLastCommittedPseudGTIDCheckPoint();
}
