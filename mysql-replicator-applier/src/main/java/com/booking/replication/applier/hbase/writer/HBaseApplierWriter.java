package com.booking.replication.applier.hbase.writer;

import com.booking.replication.augmenter.model.event.AugmentedEvent;

import java.io.IOException;
import java.util.Collection;

public interface HBaseApplierWriter {

    void buffer(String transactionUUID, Collection<AugmentedEvent> events);

    long getBufferClearTime();

    int getTransactionBufferSize(String transactionUUID);

    boolean forceFlush() throws IOException;

    boolean flushTransactionBuffer(String transactionUUID);
}
