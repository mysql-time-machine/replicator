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
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Replicator {
    private static final Logger LOG = Logger.getLogger(Replicator.class.getName());
    private static final String COMMAND_LINE_SYNTAX = "java -jar mysql-replicator-<version>.jar";

    private final Coordinator coordinator;
    private final Supplier supplier;
    private final Augmenter augmenter;
    private final Seeker seeker;
    private final Applier applier;
    private final CheckpointApplier checkpointApplier;
    private final Streams<AugmentedEvent, AugmentedEvent> streamsApplier;
    private final Streams<RawEvent, AugmentedEvent> streamsSupplier;

    public Replicator(Map<String, String> configuration) {
        try {
            this.coordinator = Coordinator.build(configuration);

            Checkpoint checkpoint = this.coordinator.loadCheckpoint(configuration.get(CheckpointApplier.Configuration.PATH));

            this.supplier = Supplier.build(configuration, checkpoint);
            this.augmenter = Augmenter.build(configuration);
            this.seeker = Seeker.build(configuration, checkpoint);
            this.applier = Applier.build(configuration);
            this.checkpointApplier = CheckpointApplier.build(configuration, this.coordinator);

            this.streamsApplier = Streams.<AugmentedEvent>builder().threads(10).tasks(8).queue().fromPush().to(this.applier).post(this.checkpointApplier).build();
            this.streamsSupplier = Streams.<RawEvent>builder().queue().fromPush().process(this.augmenter).process(this.seeker).to(streamsApplier::push).build();

            this.supplier.onEvent(this.streamsSupplier::push);

            Consumer<Exception> exceptionHandle = (externalException) -> {
                Replicator.LOG.log(Level.SEVERE, "error", externalException);

                if (ForceRewindException.class.isInstance(externalException)) {
                    ForceRewindException forceRewindException = ForceRewindException.class.cast(externalException);


                }

                this.stop();
            };

            this.streamsSupplier.onException(exceptionHandle);
            this.streamsApplier.onException(exceptionHandle);

            this.coordinator.onLeadershipTake(() -> {
                try {
                    Replicator.LOG.log(Level.INFO, "starting replicator");

                    this.streamsApplier.start();
                    this.streamsSupplier.start();
                    this.supplier.start();
                } catch (IOException | InterruptedException exception) {
                    exceptionHandle.accept(exception);
                }
            });

            this.coordinator.onLeadershipLoss(() -> {
                try {
                    Replicator.LOG.log(Level.INFO, "stopping replicator");

                    this.supplier.stop();
                    this.streamsSupplier.stop();
                    this.streamsApplier.stop();
                    this.applier.close();
                } catch (IOException | InterruptedException exception) {
                    exceptionHandle.accept(exception);
                }
            });
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    public void start() {
        try {
            Replicator.LOG.log(Level.INFO, "starting coordinator");

            this.coordinator.start();
        } catch (InterruptedException exception) {
            Replicator.LOG.log(Level.SEVERE, "error starting coordinator", exception);
        }
    }

    public void join() {
        try {
            Replicator.LOG.log(Level.INFO, "running coordinator");

            this.coordinator.join();
        } catch (InterruptedException exception) {
            Replicator.LOG.log(Level.SEVERE, "error starting coordinator", exception);
        }
    }

    public void stop() {
        try {
            Replicator.LOG.log(Level.INFO, "stopping coordinator");

            this.coordinator.stop();
        } catch (InterruptedException exception) {
            Replicator.LOG.log(Level.SEVERE, "error stopping coordinator", exception);
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

                Replicator replicator = new Replicator(configuration);

                Runtime.getRuntime().addShutdownHook(new Thread(replicator::stop));

                replicator.start();
                replicator.join();
            }
        } catch (Exception exception) {
            new HelpFormatter().printHelp(Replicator.COMMAND_LINE_SYNTAX, null, options, exception.getMessage());
        }
    }
}
