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

        this.executor = Executors.newSingleThreadExecutor();
        this.running = new AtomicBoolean(false);
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
    public void start(Checkpoint checkpoint) {
        if (!this.client.isConnected() && !this.running.getAndSet(true)) {
            if (checkpoint != null) {
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
    public void stop() {
        if (this.client.isConnected() && this.running.getAndSet(false)) {
            try {
                this.client.disconnect();
            } catch (IOException exception) {
                BinaryLogSupplier.LOG.log(Level.SEVERE, "error disconnecting", exception);
            }

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
