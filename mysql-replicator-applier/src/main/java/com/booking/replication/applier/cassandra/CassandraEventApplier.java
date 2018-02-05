package com.booking.replication.applier.cassandra;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.mysql.binlog.model.Event;

import java.util.Map;

public class CassandraEventApplier implements EventApplier {
    public CassandraEventApplier(Map<String, String> configuration) {
    }

    @Override
    public void accept(Event event) {

    }

    @Override
    public void close() {

    }
}
