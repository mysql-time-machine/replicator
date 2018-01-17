package com.booking.replication.mysql.binlog.supplier;

import com.booking.replication.mysql.binlog.model.Checkpoint;
import com.booking.replication.mysql.binlog.model.Event;
import com.booking.replication.mysql.binlog.supplier.connector.BinaryLogConnectorSupplier;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public interface EventSupplier {
    interface Configuration {
        String MYSQL_HOSTNAME = "mysql.hostname";
        String MYSQL_PORT     = "mysql.port";
        String MYSQL_USERNAME = "mysql.username";
        String MYSQL_PASSWORD = "mysql.password";
    }

    void onEvent(Consumer<Event> consumer);
    void start() throws IOException;
    void stop() throws IOException;

    @SuppressWarnings("unchecked")
    static EventSupplier build(Map<String, String> configuration, Checkpoint checkpoint) throws IOException {
        return new BinaryLogConnectorSupplier(configuration, checkpoint);
    }
}
