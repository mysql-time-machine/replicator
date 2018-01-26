package com.booking.replication.applier.hbase;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.mysql.binlog.model.Event;

import java.util.Map;

public class HBaseEventApplier implements EventApplier {
    public HBaseEventApplier(Map<String, String> configuration) {
    }

    @Override
    public void accept(Event event) {
        switch (event.getHeader().getEventType()) {

            default:

        }
    }

    @Override
    public void close() {
    }
}
