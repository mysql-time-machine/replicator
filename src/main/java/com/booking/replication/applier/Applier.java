package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.event.*;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.pipeline.CurrentTransaction;

import com.booking.replication.pipeline.PipelineOrchestrator;

import java.io.IOException;

public interface Applier {

    SupportedAppliers.ApplierName getApplierName() throws ApplierException;

    void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction)
            throws ApplierException, IOException;

    void applyBeginQueryEvent(RawBinlogEventQuery event, CurrentTransaction currentTransaction);

    void applyCommitQueryEvent(RawBinlogEventQuery event, CurrentTransaction currentTransaction);

    void applyXidEvent(RawBinlogEventXid event, CurrentTransaction currentTransaction);

    void applyRotateEvent(RawBinlogEventRotate event) throws ApplierException, IOException;

    void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller);

    void forceFlush() throws ApplierException, IOException;

    void applyFormatDescriptionEvent(RawBinlogEventFormatDescription event);

    void applyTableMapEvent(RawBinlogEventTableMap event);

    void waitUntilAllRowsAreCommitted() throws IOException, ApplierException;

    void applyPseudoGTIDEvent(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) throws Exception;

    PseudoGTIDCheckpoint getLastCommittedPseudGTIDCheckPoint();
}
