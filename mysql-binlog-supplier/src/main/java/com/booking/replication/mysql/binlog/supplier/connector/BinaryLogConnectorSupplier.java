package com.booking.replication.mysql.binlog.supplier.connector;

import com.booking.replication.mysql.binlog.model.Checkpoint;
import com.booking.replication.mysql.binlog.model.Event;
import com.booking.replication.mysql.binlog.supplier.EventSupplier;
import com.booking.replication.mysql.binlog.supplier.connector.handler.EventInvocationHandler;
import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

public class BinaryLogConnectorSupplier implements EventSupplier {
    private final BinaryLogClient client;

    public BinaryLogConnectorSupplier(Map<String, String> configuration, Checkpoint checkpoint) throws IOException {
        this.client = new BinaryLogClient(
                configuration.get(EventSupplier.Configuration.MYSQL_HOSTNAME),
                Integer.parseInt(configuration.getOrDefault(EventSupplier.Configuration.MYSQL_PORT, "3306")),
                configuration.get(EventSupplier.Configuration.MYSQL_USERNAME),
                configuration.get(EventSupplier.Configuration.MYSQL_PASSWORD)
        );

        if (checkpoint != null) {
            this.client.setServerId(checkpoint.getServerId());
            this.client.setBinlogFilename(checkpoint.getBinlogFilename());
            this.client.setBinlogPosition(checkpoint.getBinlogPosition());
        }
    }

    @Override
    public void onEvent(Consumer<Event> consumer) {
        this.client.registerEventListener(
            event -> {
                try {
                    consumer.accept(Event.decorate(new EventInvocationHandler(event)));
                } catch (ReflectiveOperationException exception) {
                    throw new RuntimeException(exception);
                }
            }
        );
    }

    @Override
    public void start() throws IOException {
        if (!this.client.isConnected()) {
            this.client.connect();
        }
    }

    @Override
    public void stop() throws IOException {
        if (this.client.isConnected()) {
            this.client.disconnect();
        }
    }
}
