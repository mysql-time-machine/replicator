package com.booking.replication.supplier.mysql.binlog;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.handler.RawEventInvocationHandler;
import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class BinaryLogSupplier implements Supplier {
    private static final Logger LOG = Logger.getLogger(BinaryLogSupplier.class.getName());

    public interface Configuration {
        String MYSQL_HOSTNAME = "mysql.hostname";
        String MYSQL_PORT = "mysql.port";
        String MYSQL_SCHEMA = "mysql.schema";
        String MYSQL_USERNAME = "mysql.username";
        String MYSQL_PASSWORD = "mysql.password";
    }

    private final BinaryLogClient client;

    public BinaryLogSupplier(Map<String, String> configuration) {
        String hostname = configuration.get(Configuration.MYSQL_HOSTNAME);
        String port = configuration.getOrDefault(Configuration.MYSQL_PORT, "3306");
        String schema = configuration.get(Configuration.MYSQL_SCHEMA);
        String username = configuration.get(Configuration.MYSQL_USERNAME);
        String password = configuration.get(Configuration.MYSQL_PASSWORD);

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", Configuration.MYSQL_PASSWORD));

        this.client = this.getClient(hostname, Integer.parseInt(port), schema, username, password);
    }

    private BinaryLogClient getClient(String hostname, int port, String schema, String username, String password) {
        return new BinaryLogClient(hostname, port, schema, username, password);
    }

    @Override
    public void onEvent(Consumer<RawEvent> consumer) {
        this.client.registerEventListener(
                event -> {
                    try {
                        consumer.accept(RawEvent.getRawEventProxy(new RawEventInvocationHandler(event)));
                    } catch (ReflectiveOperationException exception) {
                        throw new RuntimeException(exception);
                    }
                }
        );
    }

    @Override
    public void start(Checkpoint checkpoint) throws IOException {
        if (!this.client.isConnected()) {
            if (checkpoint != null) {
                this.client.setServerId(checkpoint.getServerId());
                this.client.setBinlogFilename(checkpoint.getBinlogFilename());
                this.client.setBinlogPosition(checkpoint.getBinlogPosition());
            }

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
