package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.event.*;
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
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction) throws ApplierException, IOException {

    }

    @Override
    public void applyBeginQueryEvent(RawBinlogEventQuery event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyCommitQueryEvent(RawBinlogEventQuery event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyXidEvent(RawBinlogEventXid event, CurrentTransaction currentTransaction) {

    }

    @Override
    public void applyRotateEvent(RawBinlogEventRotate event) throws ApplierException, IOException {

    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {

    }

    @Override
    public void forceFlush() throws ApplierException, IOException {

    }

    @Override
    public void applyFormatDescriptionEvent(RawBinlogEventFormatDescription event) {

    }

    @Override
    public void applyTableMapEvent(RawBinlogEventTableMap event) {

    }

    @Override
    public void waitUntilAllRowsAreCommitted() throws IOException, ApplierException {

    }
}
