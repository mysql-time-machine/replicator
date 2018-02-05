package com.booking.replication.supplier;

import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;
import com.booking.replication.supplier.mysql.binlog.BinaryLogConnectorSupplier;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public interface EventSupplier {
    void onEvent(Consumer<Event> consumer);
    void start() throws IOException;
    void stop() throws IOException;

    @SuppressWarnings("unchecked")
    static EventSupplier build(Map<String, String> configuration, Checkpoint checkpoint) throws IOException {
        return new BinaryLogConnectorSupplier(configuration, checkpoint);
    }
}
