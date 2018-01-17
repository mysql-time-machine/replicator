package com.booking.replication;

import com.booking.replication.applier.Seeker;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.commons.map.MapFlatter;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.metrics.MetricsApplier;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Replicator {
    interface Configuration {
        String CHECKPOINT_PATH = "checkpoint.path";
        String CHECKPOINT_DEFAULT = "checkpoint.default";
        String REPLICATOR_THREADS = "replicator.threads";
        String REPLICATOR_TASKS = "replicator.tasks";
    }

    private static final Logger LOG = Logger.getLogger(Replicator.class.getName());
    private static final String COMMAND_LINE_SYNTAX = "java -jar mysql-replicator-<version>.jar";

    private final String checkpointPath;
    private final String checkpointDefault;
    private final Coordinator coordinator;
    private final Supplier supplier;
    private final Augmenter augmenter;
    private final Seeker seeker;
    private final Partitioner partitioner;
    private final Applier applier;
    private final MetricsApplier<?> metricsApplier;
    private final CheckpointApplier checkpointApplier;
    private final Streams<Collection<AugmentedEvent>, Collection<AugmentedEvent>> streamsApplier;
    private final Streams<RawEvent, Collection<AugmentedEvent>> streamsSupplier;

    public Replicator(final Map<String, Object> configuration) {
        Object checkpointPath = configuration.get(Configuration.CHECKPOINT_PATH);
        Object checkpointDefault = configuration.get(Configuration.CHECKPOINT_DEFAULT);

        Objects.requireNonNull(checkpointPath, String.format("Configuration required: %s", Configuration.CHECKPOINT_PATH));

        int threads = Integer.parseInt(configuration.getOrDefault(Configuration.REPLICATOR_THREADS, "1").toString());
        int tasks = Integer.parseInt(configuration.getOrDefault(Configuration.REPLICATOR_TASKS, "1").toString());

        this.checkpointPath = checkpointPath.toString();
        this.checkpointDefault = (checkpointDefault != null)?(checkpointDefault.toString()):(null);
        this.coordinator = Coordinator.build(configuration);
        this.supplier = Supplier.build(configuration);
        this.augmenter = Augmenter.build(configuration);
        this.seeker = Seeker.build(configuration);
        this.partitioner = Partitioner.build(configuration);
        this.applier = Applier.build(configuration);
        this.metricsApplier = MetricsApplier.build(configuration);
        this.checkpointApplier = CheckpointApplier.build(configuration, this.coordinator, this.checkpointPath);

        this.streamsApplier = Streams.<Collection<AugmentedEvent>>builder()
                .threads(threads)
                .tasks(tasks)
                .partitioner((events, totalPartitions) -> this.partitioner.apply(events.iterator().next(), totalPartitions))
                .queue()
                .fromPush()
                .to(this.applier)
                .to((events) -> {
                    for (AugmentedEvent event : events) {
                        this.metricsApplier.apply(event);
                    }

                    return true;
                })
                .post((events, task) -> {
                    for (AugmentedEvent event : events) {
                        this.checkpointApplier.accept(event, task);
                    }
                }).build();

        this.streamsSupplier = Streams.<RawEvent>builder()
                .fromPush()
                .process(this.augmenter)
                .process(this.seeker)
                .to((events) -> {
                    Map<Integer, Collection<AugmentedEvent>> splitEventsMap = new HashMap<>();

                    for (AugmentedEvent event : events) {
                        splitEventsMap.computeIfAbsent(
                                this.partitioner.apply(event, tasks), partition -> new ArrayList<>()
                        ).add(event);
                    }

                    for (Collection<AugmentedEvent> splitEvents : splitEventsMap.values()) {
                        this.streamsApplier.push(splitEvents);
                    }

                    return true;
                }).build();

        Consumer<Exception> exceptionHandle = (exception) -> {
            if (ForceRewindException.class.isInstance(exception)) {
                Replicator.LOG.log(Level.WARNING, exception.getMessage());

                this.rewind();
            } else {
                Replicator.LOG.log(Level.SEVERE, "error", exception);

                this.stop();
            }
        };

        this.coordinator.onLeadershipTake(() -> {
            try {
                Replicator.LOG.log(Level.INFO, "starting replicator");

                Replicator.LOG.log(Level.INFO, "starting streams applier");
                this.streamsApplier.start();

                Replicator.LOG.log(Level.INFO, "starting streams supplier");
                this.streamsSupplier.start();

                Replicator.LOG.log(Level.INFO, "starting supplier");
                this.supplier.start(this.seeker.seek(this.getCheckpoint()));

                Replicator.LOG.log(Level.INFO, "replicator started");
            } catch (IOException | InterruptedException exception) {
                exceptionHandle.accept(exception);
            }
        });

        this.coordinator.onLeadershipLose(() -> {
            try {
                Replicator.LOG.log(Level.INFO, "stopping replicator");

                Replicator.LOG.log(Level.INFO, "stopping supplier");
                this.supplier.stop();

                Replicator.LOG.log(Level.INFO, "stopping streams supplier");
                this.streamsSupplier.stop();

                Replicator.LOG.log(Level.INFO, "stopping streams applier");
                this.streamsApplier.stop();

                Replicator.LOG.log(Level.INFO, "replicator stopped");
            } catch (IOException | InterruptedException exception) {
                exceptionHandle.accept(exception);
            }
        });

        this.supplier.onEvent(this.streamsSupplier::push);
        this.supplier.onException(exceptionHandle);
        this.streamsSupplier.onException(exceptionHandle);
        this.streamsApplier.onException(exceptionHandle);
    }

    private Checkpoint getCheckpoint() throws IOException {
        Checkpoint checkpoint = this.coordinator.loadCheckpoint(this.checkpointPath);

        if (checkpoint == null && this.checkpointDefault != null) {
            checkpoint = new ObjectMapper().readValue(this.checkpointDefault, Checkpoint.class);
        }

        return checkpoint;
    }

    public void start() {
        Replicator.LOG.log(Level.INFO, "starting coordinator");
        this.coordinator.start();
    }

    public void wait(long timeout, TimeUnit unit) {
        this.coordinator.wait(timeout, unit);
    }

    public void join() {
        this.coordinator.join();
    }

    public void stop() {
        try {
            Replicator.LOG.log(Level.INFO, "stopping coordinator");
            this.coordinator.stop();

            Replicator.LOG.log(Level.INFO, "closing augmenter");
            this.augmenter.close();

            Replicator.LOG.log(Level.INFO, "closing seeker");
            this.seeker.close();

            Replicator.LOG.log(Level.INFO, "closing partitioner");
            this.partitioner.close();

            Replicator.LOG.log(Level.INFO, "closing applier");
            this.applier.close();

            Replicator.LOG.log(Level.INFO, "closing metrics applier");
            this.metricsApplier.close();

            Replicator.LOG.log(Level.INFO, "closing checkpoint applier");
            this.checkpointApplier.close();
        } catch (IOException exception) {
            Replicator.LOG.log(Level.SEVERE, "error stopping coordinator", exception);
        }
    }

    public void rewind() {
        try {
            Replicator.LOG.log(Level.INFO, "rewinding supplier");

            this.supplier.disconnect();
            this.supplier.connect(this.seeker.seek(this.getCheckpoint()));
        } catch (IOException exception) {
            Replicator.LOG.log(Level.SEVERE, "error rewinding supplier", exception);
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
                Map<String, Object> configuration = new HashMap<>();

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
            }
        } catch (Exception exception) {
            new HelpFormatter().printHelp(Replicator.COMMAND_LINE_SYNTAX, null, options, exception.getMessage());
        }
    }
}
