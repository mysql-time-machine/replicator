package com.booking.replication.supplier.mysql.binlog;

import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;
import com.booking.replication.supplier.EventSupplier;
import com.booking.replication.supplier.mysql.binlog.handler.EventInvocationHandler;
import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class BinaryLogSupplier implements EventSupplier {
    public interface Configuration {
        String MYSQL_HOSTNAME = "mysql.hostname";
        String MYSQL_PORT = "mysql.port";
        String MYSQL_SCHEMA = "mysql.schema";
        String MYSQL_USERNAME = "mysql.username";
        String MYSQL_PASSWORD = "mysql.password";
    }

    private final BinaryLogClient client;

    public BinaryLogSupplier(Map<String, String> configuration, Checkpoint checkpoint) {
        String hostname = configuration.get(Configuration.MYSQL_HOSTNAME);
        String port = configuration.getOrDefault(Configuration.MYSQL_PORT, "3306");
        String schema = configuration.get(Configuration.MYSQL_SCHEMA);
        String username = configuration.get(Configuration.MYSQL_USERNAME);
        String password = configuration.get(Configuration.MYSQL_PASSWORD);

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", Configuration.MYSQL_PASSWORD));

        this.client = this.getClient(hostname, Integer.parseInt(port), schema, username, password, checkpoint);
    }

    private BinaryLogClient getClient(String hostname, int port, String schema, String username, String password, Checkpoint checkpoint) {
        BinaryLogClient client = new BinaryLogClient(hostname, port, schema, username, password);

        if (checkpoint != null) {
            client.setServerId(checkpoint.getServerId());
            client.setBinlogFilename(checkpoint.getBinlogFilename());
            client.setBinlogPosition(checkpoint.getBinlogPosition());
        }

        return client;
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
