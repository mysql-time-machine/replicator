package com.booking.replication.applier.hbase.writer;

import com.booking.replication.augmenter.model.event.AugmentedEvent;

import java.io.IOException;
import java.util.Collection;

public interface HBaseApplierWriter {

    void buffer(Long threadID, String transactionUUID, Collection<AugmentedEvent> events);

    long getThreadLastFlushTime();

    int getThreadBufferSize(Long threadID);

    boolean forceFlushThreadBuffer(Long threadID) throws IOException;

    boolean flushThreadBuffer(Long threadID);

    boolean forceFlushAllThreadBuffers() throws IOException;
}
