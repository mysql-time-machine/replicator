package com.booking.replication.applier;

import com.booking.replication.applier.kafka.KafkaMessageBufferException;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;

import com.booking.replication.applier.SupportedAppliers.ApplierName;

import com.booking.replication.binlog.event.impl.*;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.exceptions.RowListMessageSerializationException;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.codahale.metrics.Counter;
import java.io.IOException;

/**
 * Wraps an applier to count incoming events
 */
public class EventCountingApplier implements Applier {

    public Applier getWrapped() {
        return wrapped;
    }

    private final Applier wrapped;
    private final Counter counter;

    @Override
    public ApplierName getApplierName() throws ApplierException {
        if (wrapped instanceof HBaseApplier) {
            return ApplierName.HBaseApplier;
        } else if (wrapped instanceof  KafkaApplier) {
            return ApplierName.KafkaApplier;
        } else if (wrapped instanceof StdoutJsonApplier) {
            return ApplierName.StdoutJsonApplier;
        } else if (wrapped instanceof  DummyApplier) {
            return ApplierName.DummyApplier;
        } else {
            throw new ApplierException("Unsupported applier: " + wrapped.toString());
        }
    }

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
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction) throws ApplierException, IOException, RowListMessageSerializationException, KafkaMessageBufferException {
        wrapped.applyAugmentedRowsEvent(augmentedSingleRowEvent, currentTransaction);
        counter.inc();
    }

    @Override
    public void applyBeginQueryEvent(BinlogEventQuery event, CurrentTransaction currentTransaction) throws RowListMessageSerializationException, KafkaMessageBufferException {
        wrapped.applyBeginQueryEvent(event, currentTransaction);
        counter.inc();
    }

    @Override
    public void applyCommitQueryEvent(BinlogEventQuery event, CurrentTransaction currentTransaction)
            throws RowListMessageSerializationException, KafkaMessageBufferException {
        wrapped.applyCommitQueryEvent(event, currentTransaction);
        counter.inc();
    }

    @Override
    public void applyXidEvent(BinlogEventXid event, CurrentTransaction currentTransaction)
            throws RowListMessageSerializationException, KafkaMessageBufferException {
        wrapped.applyXidEvent(event, currentTransaction);
        counter.inc();
    }

    @Override
    public void applyRotateEvent(BinlogEventRotate event) throws ApplierException, IOException {
        wrapped.applyRotateEvent(event);
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) throws SchemaTransitionException {
        wrapped.applyAugmentedSchemaChangeEvent(augmentedSchemaChangeEvent, caller);
        counter.inc();
    }

    @Override
    public void forceFlush() throws ApplierException, IOException, RowListMessageSerializationException {
        wrapped.forceFlush();
    }

    @Override
    public void applyFormatDescriptionEvent(BinlogEventFormatDescription event) {
        wrapped.applyFormatDescriptionEvent(event);
        counter.inc();
    }

    @Override
    public void applyTableMapEvent(BinlogEventTableMap event) {
        wrapped.applyTableMapEvent(event);
        counter.inc();
    }

    @Override
    public void waitUntilAllRowsAreCommitted() throws IOException, ApplierException {
        wrapped.waitUntilAllRowsAreCommitted();
    }

    @Override
    public void applyPseudoGTIDEvent(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) throws Exception {
        wrapped.applyPseudoGTIDEvent(pseudoGTIDCheckPoint);
    }

    @Override
    public PseudoGTIDCheckpoint getLastCommittedPseudGTIDCheckPoint() {
        return wrapped.getLastCommittedPseudGTIDCheckPoint();
    }
}
