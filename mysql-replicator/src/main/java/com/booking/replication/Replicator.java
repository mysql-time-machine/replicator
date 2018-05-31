package com.booking.replication;

import com.booking.replication.applier.Seeker;
import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.commons.map.MapFlatter;
import com.booking.replication.coordinator.Coordinator;

import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.RawEvent;

import com.booking.replication.streams.Streams;
import com.booking.replication.supplier.Supplier;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Replicator {
    private static final Logger LOG = Logger.getLogger(Replicator.class.getName());
    private static final String COMMAND_LINE_SYNTAX = "java -jar mysql-replicator-<version>.jar";

    private final Map<String, String> configuration;

    public Replicator(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    public void start() {
        try {
            Coordinator coordinator = Coordinator.build(
                    this.configuration
            );

            Checkpoint checkpoint = coordinator.loadCheckpoint(
                    this.configuration.get(CheckpointApplier.Configuration.PATH)
            );

            Supplier supplier = Supplier.build(
                    this.configuration,
                    checkpoint
            );

            Augmenter augmenter = Augmenter.build(
                    this.configuration
            );

            Seeker seeker = Seeker.build(
                    this.configuration,
                    checkpoint
            );

            Applier applier = Applier.build(
                    this.configuration
            );

            CheckpointApplier checkpointApplier = CheckpointApplier.build(
                    this.configuration,
                    coordinator
            );

            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

            AtomicLong delay = new AtomicLong();
            AtomicLong count = new AtomicLong();

            Streams<AugmentedEvent, AugmentedEvent> streamsApplier = Streams.<AugmentedEvent>builder()
                    .threads(10)
                    .tasks(8)
                    .queue()       // <- use queue, default: ConcurrentLinkedDeque
                    .fromPush()    // <- this sets from to null.
                    .to(applier)
                    .post(checkpointApplier)
                    .build();

            Streams<RawEvent, AugmentedEvent> streamsSupplier = Streams.<RawEvent>builder()
                    .queue() // TODO: internal buffer for push - for some reason reduces performance - investigate
                    .fromPush()
                    .process(augmenter)
                    .process(seeker)
                    .to(streamsApplier::push)
                    .post(event -> {
                        delay.set(System.currentTimeMillis() - event.getHeader().getTimestamp());
                        count.incrementAndGet();
                    })
                    .build();

            executor.scheduleAtFixedRate(() -> {
                long timestamp = delay.get();
                long quantity = count.getAndSet(0);

                Replicator.LOG.info(
                    String.format("Delay: %03d hours %02d minutes %02d seconds, Quantity: %d",
                            TimeUnit.MILLISECONDS.toHours(timestamp),
                            TimeUnit.MILLISECONDS.toMinutes(timestamp) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timestamp)),
                            TimeUnit.MILLISECONDS.toSeconds(timestamp) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timestamp)),
                            quantity
                    )
                );
            }, 10, 10, TimeUnit.SECONDS);

            supplier.onEvent(streamsSupplier::push);

            Runnable shutdown = () -> {
                try {
                    Replicator.LOG.log(Level.INFO, "stopping coordinator");

                    coordinator.stop();
                } catch (InterruptedException exception) {
                    Replicator.LOG.log(Level.SEVERE, "error stopping", exception);
                }
            };

            Consumer<Exception> exceptionHandle = (externalException) -> {
                Replicator.LOG.log(Level.SEVERE, "error", externalException);

                if (ForceRewindException.class.isInstance(externalException)) {
                    ForceRewindException forceRewindException = ForceRewindException.class.cast(externalException);


                }

                shutdown.run();
            };

            streamsSupplier.onException(exceptionHandle);
            streamsApplier.onException(exceptionHandle);

            Runtime.getRuntime().addShutdownHook(new Thread(shutdown));

            coordinator.onLeadershipTake(() -> {
                try {
                    Replicator.LOG.log(Level.INFO, "starting replicator");

                    streamsApplier.start();
                    streamsSupplier.start();
                    supplier.start();

                    // wait
                } catch (IOException | InterruptedException exception) {
                    exceptionHandle.accept(exception);
                }
            });

            coordinator.onLeadershipLoss(() -> {
                try {
                    Replicator.LOG.log(Level.INFO, "stopping replicator");

                    supplier.stop();
                    streamsSupplier.stop();
                    streamsApplier.stop();
                    applier.close();
                    executor.shutdown();
                } catch (IOException | InterruptedException exception) {
                    exceptionHandle.accept(exception);
                }
            });

            Replicator.LOG.log(Level.INFO, "starting coordinator");

            coordinator.start();
            coordinator.join();
        } catch (Exception exception) {
            Replicator.LOG.log(Level.SEVERE, "error executing replicator", exception);
        }
    }

    /*
     * Start the JVM with the argument -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
     */
    public static void main(String[] arguments) {
        Options options = new Options();

        options.addOption(Option.builder().longOpt("help").desc("print the help message").build());
        options.addOption(Option.builder().longOpt("config").argName("key-value").desc("the configuration to be used with the format <key>=<value>").hasArgs().build());
        options.addOption(Option.builder().longOpt("config-file").argName("filename").desc("the configuration file to be used (YAML)").hasArg().build());
        options.addOption(Option.builder().longOpt("supplier").argName("supplier").desc("the supplier to be used").hasArg().build());
        options.addOption(Option.builder().longOpt("applier").argName("applier").desc("the applier to be used").hasArg().build());

        try {
            CommandLine line = new DefaultParser().parse(options, arguments);

            if (line.hasOption("help")) {
                new HelpFormatter().printHelp(Replicator.COMMAND_LINE_SYNTAX, options);
            } else {
                Map<String, String> configuration = new HashMap<>();

                if (line.hasOption("config")) {
                    for (String keyValue : line.getOptionValues("config")) {
                        int startIndex = keyValue.indexOf('=');

                        if (startIndex > 0) {
                            int endIndex = startIndex + 1;

                            if (endIndex < keyValue.length()) {
                                configuration.put(keyValue.substring(0, startIndex), keyValue.substring(endIndex));
                            }
                        }
                    }
                }

                if (line.hasOption("config-file")) {
                    configuration.putAll(new MapFlatter(".").flattenMap(new ObjectMapper(new YAMLFactory()).readValue(
                            new File(line.getOptionValue("config-file")),
                            new TypeReference<Map<String, Object>>() {
                            }
                    )));
                }

                if (line.hasOption("supplier")) {
                    configuration.put(Supplier.Configuration.TYPE, line.getOptionValue("supplier").toUpperCase());
                }

                if (line.hasOption("applier")) {
                    configuration.put(Applier.Configuration.TYPE, line.getOptionValue("applier").toUpperCase());
                }

                new Replicator(configuration).start();
            }
        } catch (Exception exception) {
            new HelpFormatter().printHelp(Replicator.COMMAND_LINE_SYNTAX, null, options, exception.getMessage());
        }
    }
}
