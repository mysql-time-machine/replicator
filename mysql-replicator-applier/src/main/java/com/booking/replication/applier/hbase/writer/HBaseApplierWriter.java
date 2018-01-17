package com.booking.replication.applier.hbase.writer;

import com.booking.replication.augmenter.model.event.AugmentedEvent;

import java.util.Collection;

public interface HBaseApplierWriter {

    void buffer(Collection<AugmentedEvent> events);

    long getBufferClearTime();

    int getBufferSize();

    boolean flush();

}
