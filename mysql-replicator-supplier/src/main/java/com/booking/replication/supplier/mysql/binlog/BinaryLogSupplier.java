package com.booking.replication.supplier.mysql.binlog;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.handler.RawEventInvocationHandler;
import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
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

    private final ExecutorService executor;
    private final AtomicBoolean running;

    private final String hostname;
    private final int port;
    private final String schema;
    private final String username;
    private final String password;

    private BinaryLogClient client;
    private Consumer<RawEvent> consumer;

    public BinaryLogSupplier(Map<String, Object> configuration) {
        Object hostname = configuration.get(Configuration.MYSQL_HOSTNAME);
        Object port = configuration.getOrDefault(Configuration.MYSQL_PORT, "3306");
        Object schema = configuration.get(Configuration.MYSQL_SCHEMA);
        Object username = configuration.get(Configuration.MYSQL_USERNAME);
        Object password = configuration.get(Configuration.MYSQL_PASSWORD);

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", Configuration.MYSQL_PASSWORD));

        this.executor = Executors.newSingleThreadExecutor();
        this.running = new AtomicBoolean(false);

        this.hostname = hostname.toString();
        this.port = Integer.parseInt(port.toString());
        this.schema = schema.toString();
        this.username = username.toString();
        this.password = password.toString();
    }

    private BinaryLogClient getClient() {
        // TODO: Implement status variable parser: https://github.com/shyiko/mysql-binlog-connector-java/issues/174
        return new BinaryLogClient(this.hostname, this.port, this.schema, this.username, this.password);
    }

    @Override
    public void onEvent(Consumer<RawEvent> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void start(Checkpoint checkpoint) {
        if (!this.running.getAndSet(true)) {
            this.connect(checkpoint);
        }
    }

    @Override
    public void connect(Checkpoint checkpoint) {
        if (this.client == null || !this.client.isConnected()) {
            this.client = this.getClient();

            if (this.consumer != null) {
                this.client.registerEventListener(
                    event -> {
                        try {
                            this.consumer.accept(RawEvent.getRawEventProxy(new RawEventInvocationHandler(event)));
                        } catch (ReflectiveOperationException exception) {
                            throw new RuntimeException(exception);
                        }
                    }
                );
            }

            if (checkpoint != null) {
                this.client.setGtidSet(null);
                this.client.setServerId(checkpoint.getServerId());
                this.client.setBinlogFilename(checkpoint.getBinlogFilename());
                this.client.setBinlogPosition(checkpoint.getBinlogPosition());
            }

            this.executor.submit(() -> {
                try {
                    this.client.connect();
                } catch (IOException exception) {
                    BinaryLogSupplier.LOG.log(Level.SEVERE, "error connecting", exception);
                }
            });
        }
    }

    @Override
    public void disconnect() {
        if (this.client != null && this.client.isConnected()) {
            try {
                this.client.disconnect();
                this.client = null;
            } catch (IOException exception) {
                BinaryLogSupplier.LOG.log(Level.SEVERE, "error disconnecting", exception);
            }
        }
    }

    @Override
    public void stop() {
        if (this.running.getAndSet(false)) {
            this.disconnect();

            try {
                this.executor.shutdown();
                this.executor.awaitTermination(5L, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                throw new RuntimeException(exception);
            } finally {
                this.executor.shutdownNow();
            }
        }
    }
}
