package com.booking.replication;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.applier.cassandra.CassandraEventApplier;
import com.booking.replication.applier.console.ConsoleEventApplier;
import com.booking.replication.applier.hbase.HBaseEventApplier;
import com.booking.replication.applier.kafka.KafkaEventApplier;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;
import com.booking.replication.streams.Streams;
import com.booking.replication.supplier.EventSupplier;
import com.booking.replication.supplier.kafka.KafkaSupplier;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

//import com.booking.infra.bigdata.augmenter.Augmenter;

public class Replicator {
    public interface Configuration extends
            Coordinator.Configuration,
            BinaryLogSupplier.Configuration,
            KafkaSupplier.Configuration,
            CassandraEventApplier.Configuration,
            ConsoleEventApplier.Configuration,
            HBaseEventApplier.Configuration,
            KafkaEventApplier.Configuration {
    }

    private static final Logger LOG = Logger.getLogger(Replicator.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private void start(Map<String, String> configuration) {
        try {
            Coordinator coordinator = Coordinator.build(
                    configuration
            );

            EventSupplier supplier = EventSupplier.build(
                    configuration,
                    this.loadCheckpoint(
                            coordinator,
                            configuration
                    )
            );

            EventApplier applier = EventApplier.build(
                    configuration
            );

            Consumer<Event> storeCheckpoint = (event) -> {
                try {
                    this.storeCheckpoint(
                            Checkpoint.of(event),
                            coordinator,
                            configuration
                    );
                } catch (IOException exception) {
                    Replicator.LOG.log(Level.SEVERE, "error storing checkpoint", exception);
                }
            };

            Consumer<Exception> exceptionHandle = (streamsException) -> {
                try {
                    Replicator.LOG.log(Level.SEVERE, "error inside streams", streamsException);
                    Replicator.LOG.log(Level.INFO, "stopping coordinator");

                    coordinator.stop();
                } catch (InterruptedException exception) {
                    Replicator.LOG.log(Level.SEVERE, "error stopping", exception);
                }
            };

            Augmenter augmenter = Augmenter.build(
                    configuration
            );

            Streams<Event, Event> streamsApplier = Streams.<Event>builder()
                    .threads(100)
                    .tasks(100)
                    .fromPush()
                    .to(applier)
                    .build();

            Streams<Event, Event> streamsSupplier = Streams.<Event>builder()
                    .fromPush()
                    // .process(augmenter)
                    .to(streamsApplier::push)
                    .build();

            supplier.onEvent(streamsSupplier::push);

            streamsSupplier.onException(exceptionHandle);
            streamsApplier.onException(exceptionHandle);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Replicator.LOG.log(Level.INFO, "stopping coordinator");

                    coordinator.stop();
                } catch (InterruptedException exception) {
                    Replicator.LOG.log(Level.SEVERE, "error stopping", exception);
                }
            }));

            coordinator.onLeadershipTake(() -> {
                try {
                    Replicator.LOG.log(Level.INFO, "starting replicator");

                    streamsApplier.start();
                    streamsSupplier.start();
                    supplier.start();
                } catch (IOException | InterruptedException exception) {
                    Replicator.LOG.log(Level.SEVERE, "error starting", exception);
                }
            });

            coordinator.onLeadershipLoss(() -> {
                try {
                    Replicator.LOG.log(Level.INFO, "stopping replicator");

                    supplier.stop();
                    streamsSupplier.stop();
                    streamsApplier.stop();
                } catch (IOException | InterruptedException exception) {
                    Replicator.LOG.log(Level.SEVERE, "error stopping", exception);
                }
            });

            Replicator.LOG.log(Level.INFO, "starting coordinator");

            coordinator.start();
            coordinator.join();
        } catch (Exception exception) {
            Replicator.LOG.log(Level.SEVERE, "error executing replicator", exception);
        }
    }

    private Checkpoint loadCheckpoint(Coordinator coordinator, Map<String, String> configuration) throws IOException {
        byte[] checkpointBytes = coordinator.loadCheckpoint(
                configuration.getOrDefault(
                        Configuration.CHECKPOINT_PATH,
                        coordinator.defaultCheckpointPath()
                )
        );

        if (checkpointBytes != null && checkpointBytes.length > 0) {
            return Replicator.MAPPER.readValue(checkpointBytes, Checkpoint.class);
        } else {
            return null;
        }
    }

    private void storeCheckpoint(Checkpoint checkpoint, Coordinator coordinator, Map<String, String> configuration) throws IOException {
        if (checkpoint != null) {
            byte[] checkpointBytes = Replicator.MAPPER.writeValueAsBytes(checkpoint);

            if (checkpointBytes != null && checkpointBytes.length > 0) {
                coordinator.storeCheckpoint(
                        configuration.getOrDefault(
                                Configuration.CHECKPOINT_PATH,
                                coordinator.defaultCheckpointPath()
                        ),
                        checkpointBytes
                );
            }
        }
    }

    /**
     * Start the JVM with the argument -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
     * @param arguments
     */
    public static void main(String[] arguments) {
        Map<String, String> configuration = new HashMap<>();

        configuration.put(Configuration.MYSQL_HOSTNAME, arguments[0]);
        configuration.put(Configuration.MYSQL_USERNAME, arguments[1]);
        configuration.put(Configuration.MYSQL_PASSWORD, arguments[2]);

        new Replicator().start(configuration);
    }
}
