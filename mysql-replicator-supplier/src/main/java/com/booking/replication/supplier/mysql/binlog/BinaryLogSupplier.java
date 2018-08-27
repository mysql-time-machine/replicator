package com.booking.replication.supplier.mysql.binlog;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.GTIDType;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.handler.RawEventInvocationHandler;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
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

    public enum PositionType {
        ANY,
        BINLOG,
        GTID
    }

    public interface Configuration {
        String MYSQL_HOSTNAME = "mysql.hostname";
        String MYSQL_PORT = "mysql.port";
        String MYSQL_SCHEMA = "mysql.schema";
        String MYSQL_USERNAME = "mysql.username";
        String MYSQL_PASSWORD = "mysql.password";
        String POSITION_TYPE = "supplier.binlog.position.type";
    }

    private final AtomicBoolean running;
    private final List<String> hostname;
    private final int port;
    private final String schema;
    private final String username;
    private final String password;
    private final PositionType positionType;

    private ExecutorService executor;
    private BinaryLogClient client;
    private Consumer<RawEvent> consumer;
    private Consumer<Exception> handler;

    public BinaryLogSupplier(Map<String, Object> configuration) {
        Object hostname = configuration.get(Configuration.MYSQL_HOSTNAME);
        Object port = configuration.getOrDefault(Configuration.MYSQL_PORT, "3306");
        Object schema = configuration.get(Configuration.MYSQL_SCHEMA);
        Object username = configuration.get(Configuration.MYSQL_USERNAME);
        Object password = configuration.get(Configuration.MYSQL_PASSWORD);
        Object positionType = configuration.getOrDefault(Configuration.POSITION_TYPE, PositionType.ANY.name());

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", Configuration.MYSQL_PASSWORD));

        this.running = new AtomicBoolean(false);
        this.hostname = this.getList(hostname);
        this.port = Integer.parseInt(port.toString());
        this.schema = schema.toString();
        this.username = username.toString();
        this.password = password.toString();
        this.positionType = PositionType.valueOf(positionType.toString());
    }

    @SuppressWarnings("unchecked")
    private List<String> getList(Object object) {
        if (List.class.isInstance(object)) {
            return (List<String>) object;
        } else {
            return Collections.singletonList(object.toString());
        }
    }

    private BinaryLogClient getClient(String hostname) {
        // TODO: Implement status variable parser: https://github.com/shyiko/mysql-binlog-connector-java/issues/174
        BinaryLogClient client = new BinaryLogClient(hostname, this.port, this.schema, this.username, this.password);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        client.setEventDeserializer(eventDeserializer);
        return client;
    }

    @Override
    public void onEvent(Consumer<RawEvent> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onException(Consumer<Exception> handler) {
        this.handler = handler;
    }

    @Override
    public void start(Checkpoint checkpoint) {
        if (!this.running.getAndSet(true)) {
            BinaryLogSupplier.LOG.log(Level.INFO, "starting binary log supplier connecting");

            if (this.executor == null) {
                this.executor = Executors.newSingleThreadExecutor();
            }

            this.connect(checkpoint);
        }
    }

    @Override
    public void connect(Checkpoint checkpoint) {
        if (this.client == null || !this.client.isConnected()) {
            this.executor.submit(() -> {
                for (String hostname : this.hostname) {
                    try {
                        this.client = this.getClient(hostname);

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
                            this.client.setServerId(checkpoint.getServerId());

                            if ((this.positionType == PositionType.ANY || this.positionType == PositionType.BINLOG) && checkpoint.getBinlog() != null) {
                                this.client.setBinlogFilename(checkpoint.getBinlog().getFilename());
                                this.client.setBinlogPosition(checkpoint.getBinlog().getPosition());
                            }

                            if ((this.positionType == PositionType.ANY || this.positionType == PositionType.GTID) && checkpoint.getGTID() != null && checkpoint.getGTID().getType() == GTIDType.REAL) {
                                this.client.setGtidSet(checkpoint.getGTID().getValue());
                            }
                        }

                        this.client.connect();

                        return;
                    } catch (IOException exception) {
                        BinaryLogSupplier.LOG.log(Level.WARNING, String.format("error connecting to %s, falling over to the next one", hostname), exception);
                    }
                }

                if (this.running.get() && this.handler != null) {
                    this.handler.accept(new IOException("error connecting"));
                } else {
                    BinaryLogSupplier.LOG.log(Level.SEVERE, "error connecting");
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
            BinaryLogSupplier.LOG.log(Level.INFO, "stopping binary log supplier connecting");

            this.disconnect();

            if (this.executor != null) {
                try {
                    this.executor.shutdown();
                    this.executor.awaitTermination(5L, TimeUnit.SECONDS);
                } catch (InterruptedException exception) {
                    throw new RuntimeException(exception);
                } finally {
                    this.executor.shutdownNow();
                    this.executor = null;
                }
            }
        }
    }
}
