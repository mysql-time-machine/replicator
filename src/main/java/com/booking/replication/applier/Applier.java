package com.booking.replication.applier;

import com.booking.replication.applier.kafka.KafkaMessageBufferException;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.event.impl.*;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.exceptions.RowListMessageSerializationException;
import com.booking.replication.pipeline.CurrentTransaction;

import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.exception.SchemaTransitionException;

import java.io.IOException;

public interface Applier {

    SupportedAppliers.ApplierName getApplierName() throws ApplierException;

    void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction)
            throws ApplierException, IOException, RowListMessageSerializationException, KafkaMessageBufferException;

    void applyBeginQueryEvent(BinlogEventQuery event, CurrentTransaction currentTransaction) throws RowListMessageSerializationException, KafkaMessageBufferException;

    void applyCommitQueryEvent(BinlogEventQuery event, CurrentTransaction currentTransaction) throws RowListMessageSerializationException, KafkaMessageBufferException;

    void applyXidEvent(BinlogEventXid event, CurrentTransaction currentTransaction) throws RowListMessageSerializationException, KafkaMessageBufferException;

    void applyRotateEvent(BinlogEventRotate event) throws ApplierException, IOException;

    void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller) throws SchemaTransitionException;

    void forceFlush() throws ApplierException, IOException, RowListMessageSerializationException;

    void applyFormatDescriptionEvent(BinlogEventFormatDescription event);

    void applyTableMapEvent(BinlogEventTableMap event);

    void waitUntilAllRowsAreCommitted() throws IOException, ApplierException;

    void applyPseudoGTIDEvent(PseudoGTIDCheckpoint pseudoGTIDCheckPoint) throws Exception;

    PseudoGTIDCheckpoint getLastCommittedPseudGTIDCheckPoint();
}
