package com.booking.replication.supplier.mysql.binlog;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.Supplier;
import com.booking.replication.supplier.mysql.binlog.handler.RawEventInvocationHandler;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class BinaryLogSupplier implements Supplier {

    private static final Logger LOG = LogManager.getLogger(BinaryLogSupplier.class);

    public enum PositionType {
        ANY,
        BINLOG,
        GTID
    }

    public interface Configuration {
        String MYSQL_HOSTNAME                       = "mysql.hostname";
        String MYSQL_PORT                           = "mysql.port";
        String MYSQL_SCHEMA                         = "mysql.schema";
        String MYSQL_USERNAME                       = "mysql.username";
        String MYSQL_PASSWORD                       = "mysql.password";
        String POSITION_TYPE                        = "supplier.binlog.position.type";
        String OVERRIDE_CHECKPOINT_START_POSITION   = "override.checkpoint.start.position";
        String BINLOG_START_FILENAME                = "supplier.binlog.start.filename";
        String BINLOG_START_POSITION                = "supplier.binlog.start.position";
    }

    private final AtomicBoolean running;
    private final AtomicBoolean connected;

    private final List<String> hostname;
    private final int port;
    private final String schema;
    private final String username;
    private final String password;
    private final PositionType positionType;
    private final Boolean positionOverride;
    private final AtomicReference<String> binlogClientGTIDSet;

    private ExecutorService executor;
    private BinaryLogClient client;
    private Consumer<RawEvent> consumer;
    private Consumer<Exception> handler;

    public BinaryLogSupplier(Map<String, Object> configuration) {
        Object hostname         = configuration.get(Configuration.MYSQL_HOSTNAME);
        Object port             = configuration.getOrDefault(Configuration.MYSQL_PORT, "3306");
        Object schema           = configuration.get(Configuration.MYSQL_SCHEMA);
        Object username         = configuration.get(Configuration.MYSQL_USERNAME);
        Object password         = configuration.get(Configuration.MYSQL_PASSWORD);
        Object positionType     = configuration.getOrDefault(Configuration.POSITION_TYPE, PositionType.GTID);
        Object positionOverride = configuration.getOrDefault(Configuration.OVERRIDE_CHECKPOINT_START_POSITION, false);

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", Configuration.MYSQL_PASSWORD));

        this.running                = new AtomicBoolean(false);
        this.connected              = new AtomicBoolean(false);
        this.hostname               = this.getList(hostname);
        this.port                   = Integer.parseInt(port.toString());
        this.schema                 = schema.toString();
        this.username               = username.toString();
        this.password               = password.toString();
        this.positionType           = PositionType.valueOf(positionType.toString());
        this.positionOverride       = (Boolean) positionOverride;
        this.binlogClientGTIDSet    = new AtomicReference<>();
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
        BinaryLogClient client = new BinaryLogClient(
                hostname,
                this.port,
                this.schema,
                this.username,
                this.password
        );
        client.setHeartbeatInterval(TimeUnit.MILLISECONDS.toMillis(1000));

        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        client.setEventDeserializer(eventDeserializer);

        return client;
    }

    @Override
    public String getGTIDSet() {
        return this.client.getGtidSet();
    }

    public void updateGTIDSet(String gtidSet) {
        this.binlogClientGTIDSet.set(gtidSet);
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
            BinaryLogSupplier.LOG.info("starting binary log supplier connecting");

            if (this.executor == null) {
                this.executor = Executors.newSingleThreadExecutor();
            }

            this.connect(checkpoint);
        }
    }

    @Override
    public void connect(Checkpoint checkpoint) {
        if (this.client == null || !this.client.isConnected()) {

                for (String hostname : this.hostname) {
                    try {
                        this.client = this.getClient(hostname);

                        if (this.consumer != null) {

                            this.client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
                                @Override
                                public void onConnect(BinaryLogClient client) {
                                    BinaryLogSupplier.LOG.info(
                                            String.format("Binlog client connected to:%s : %s, %s",
                                                    hostname,
                                                    client.getBinlogFilename(),
                                                    client.getBinlogPosition(),
                                                    client.getGtidSet()));
                                }

                                @Override
                                public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
                                    BinaryLogSupplier.LOG.error(String.format("Binlog client had communication failure :%s, %s", hostname, ex.getMessage()));
                                }

                                @Override
                                public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
                                    BinaryLogSupplier.LOG.error(String.format("Binlog client had Event deserialization failure : %s, %s", hostname, ex.getMessage()));
                                }

                                @Override
                                public void onDisconnect(BinaryLogClient client) {
                                    BinaryLogSupplier.LOG.info(String.format("Binlog client disconnected from: %s", hostname));
                                }
                            });

                            this.client.registerEventListener(
                                    event -> {
                                        try {
                                            this.consumer.accept(RawEvent.getRawEventProxy(new RawEventInvocationHandler(this.client, event)));
                                        } catch (ReflectiveOperationException exception) {
                                            throw new RuntimeException(exception);
                                        }
                                    }
                            );

                        }

                        this.client.setServerId(new Random().nextLong() );

                        if (checkpoint != null) {
                            if (checkpoint.getGtidSet() != null && !checkpoint.getGtidSet().equals("")) {
                                LOG.info("Starting Binlog Client from GTIDSet checkpoint. GTIDSet: " + checkpoint.getGtidSet());
                                this.client.setGtidSet(checkpoint.getGtidSet());
                                this.binlogClientGTIDSet.set(this.client.getGtidSet());
                                this.client.connect();
                                LOG.info("Started binlog Client from GTIDSet checkpoint. GTIDSet: " + checkpoint.getGtidSet());
                                this.connected.set(true);
                            } else {
                                LOG.info("Starting binlog Client from binlogFilename and position: "
                                        + checkpoint.getBinlog().getFilename()
                                        + "/"
                                        + checkpoint.getBinlog().getPosition()
                                );
                                this.client.setBinlogFilename(checkpoint.getBinlog().getFilename());
                                this.client.setBinlogPosition(checkpoint.getBinlog().getPosition());
                                this.client.connect();
                                LOG.info("Started binlog Client from binlogFilename and position: "
                                        + checkpoint.getBinlog().getFilename()
                                        + "/"
                                        + checkpoint.getBinlog().getPosition()
                                );
                                this.connected.set(true);
                            }
                        } else {
                            throw new RuntimeException("No startup checkpoint provided.");
                        }
                    } catch (IOException exception) {
                        BinaryLogSupplier.LOG.warn(String.format("error connecting to %s, falling over to the next one", hostname), exception);
                    }
                }

                if (!this.connected.get()) {
                    if (this.running.get() && this.handler != null) {
                        this.handler.accept(new IOException("error connecting"));
                    } else {
                        BinaryLogSupplier.LOG.error("error connecting");
                        throw  new RuntimeException("MySQL server pool depleted, could not connect to any of the provided hosts.");
                    }
                }
                return;
            }
    }

    @Override
    public void disconnect() {
        if (this.client != null && this.client.isConnected()) {
            try {
                this.client.disconnect();
                this.client = null;
                this.connected.set(false);
            } catch (IOException exception) {
                BinaryLogSupplier.LOG.error("error disconnecting", exception);
            }
        } else {
            BinaryLogSupplier.LOG.warn("Trying to disconnect supplier which is already disconnected");
        }
    }

    @Override
    public void stop() {
        if (this.running.getAndSet(false)) {
            BinaryLogSupplier.LOG.info("stopping binary log supplier connecting");

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
