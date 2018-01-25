package com.booking.replication.applier;

import com.booking.replication.mysql.binlog.model.Event;

import java.util.Map;

public class HBaseEventApplier implements EventApplier {
    public HBaseEventApplier(Map<String, String> configuration) {
    }

    @Override
    public void accept(Event event) {
    }

    @Override
    public void close() {
    }
}
