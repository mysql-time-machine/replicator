package com.booking.replication;

import com.booking.replication.applier.EventApplier;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.mysql.binlog.model.Checkpoint;
import com.booking.replication.mysql.binlog.model.Event;
import com.booking.replication.streams.Streams;
import com.booking.replication.mysql.binlog.supplier.EventSupplier;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

//import com.booking.infra.bigdata.augmenter.Augmenter;

public class Replicator {
    private static final Logger log = Logger.getLogger(Replicator.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private void start(Map<String, String> configuration) {
        try {
            Coordinator coordinator = Coordinator.build(
                    configuration
            );

            EventSupplier supplier = EventSupplier.build(
                    configuration,
                    Replicator.mapper.readValue(
                            coordinator.loadCheckpoint(
                                    configuration.getOrDefault(
                                            Coordinator.Configuration.CHECKPOINT_PATH,
                                            coordinator.defaultCheckpointPath()
                                    )
                            ),
                            Checkpoint.class
                    )
            );

            EventApplier applier = EventApplier.build(
                    configuration
            );

            Consumer<Event> storeCheckpoint = (event) -> {
                try {
                    byte[] checkpoint = Replicator.mapper.writeValueAsBytes(Checkpoint.of(event));

                    if (checkpoint != null) {
                        coordinator.storeCheckpoint(
                                configuration.getOrDefault(
                                        Coordinator.Configuration.CHECKPOINT_PATH,
                                        coordinator.defaultCheckpointPath()
                                ),
                                checkpoint
                        );
                    }
                } catch (IOException exception) {
                    Replicator.log.log(Level.SEVERE, "error storing checkpoint", exception);
                }
            };

//            EventApplier<Event> augmenter = Augmenter.build(
//                    configuration
//            );

            Streams<Event, Event> streams = Streams.<Event>builder()
                    .fromPush()
                   // .process(augmenter)
                    .to(applier)
                    .post(storeCheckpoint)
                    .build();

            supplier.onEvent(streams::push);

            streams.onException((streamsException) -> {
                try {
                    Replicator.log.log(Level.SEVERE, "error inside streams", streamsException);
                    Replicator.log.log(Level.INFO, "stopping coordinator");

                    coordinator.stop();
                } catch (InterruptedException exception) {
                    Replicator.log.log(Level.SEVERE, "error stopping", exception);
                }
            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Replicator.log.log(Level.INFO, "stopping coordinator");

                    coordinator.stop();
                } catch (InterruptedException exception) {
                    Replicator.log.log(Level.SEVERE, "error stopping", exception);
                }
            }));

            coordinator.onLeadershipTake(() -> {
                try {
                    Replicator.log.log(Level.INFO, "starting replicator");

                    streams.start();
                    supplier.start();
                } catch (IOException | InterruptedException exception) {
                    Replicator.log.log(Level.SEVERE, "error starting", exception);
                }
            });

            coordinator.onLeadershipLoss(() -> {
                try {
                    Replicator.log.log(Level.INFO, "stopping replicator");

                    supplier.stop();
                    streams.stop();
                } catch (IOException | InterruptedException exception) {
                    Replicator.log.log(Level.SEVERE, "error stopping", exception);
                }
            });

            Replicator.log.log(Level.INFO, "starting coordinator");

            coordinator.start();
            coordinator.join();
        } catch (Exception exception) {
            Replicator.log.log(Level.SEVERE, "error executing replicator", exception);
        }
    }

    /**
     * Start the JVM with the argument -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
     * @param arguments
     */
    public static void main(String[] arguments) {
        Map<String, String> configuration = new HashMap<>();

        configuration.put(EventSupplier.Configuration.MYSQL_HOSTNAME, "fdamasceno-mysql57-01.fab4.dev.booking.com");
        configuration.put(EventSupplier.Configuration.MYSQL_USERNAME, "fdamasceno");
        configuration.put(EventSupplier.Configuration.MYSQL_PASSWORD, arguments[0]);

        new Replicator().start(configuration);
    }
}
