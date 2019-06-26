package com.booking.replication;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.applier.Seeker;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.commons.map.MapFlatter;
import com.booking.replication.commons.metrics.Metrics;
import com.booking.replication.controller.WebServer;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.streams.Streams;
import com.booking.replication.supplier.Supplier;

import com.booking.replication.supplier.model.RawEvent;
import com.booking.utils.BootstrapReplicator;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class Replicator {

    public interface Configuration {
        String CHECKPOINT_PATH = "checkpoint.path";
        String CHECKPOINT_DEFAULT = "checkpoint.default";
        String REPLICATOR_THREADS = "replicator.threads";
        String REPLICATOR_TASKS = "replicator.tasks";
        String REPLICATOR_QUEUE_SIZE = "replicator.queue.size";
        String REPLICATOR_QUEUE_TIMEOUT = "replicator.queue.timeout";
        String OVERRIDE_CHECKPOINT_START_POSITION = "override.checkpoint.start.position";
        String OVERRIDE_CHECKPOINT_BINLOG_FILENAME = "override.checkpoint.binLog.filename";
        String OVERRIDE_CHECKPOINT_BINLOG_POSITION = "override.checkpoint.binLog.position";
        String OVERRIDE_CHECKPOINT_GTID_SET = "override.checkpoint.gtidSet";
    }

    private static final Logger LOG = LogManager.getLogger(Replicator.class);
    private static final String COMMAND_LINE_SYNTAX = "java -jar mysql-replicator-<version>.jar";

    private final String checkpointPath;
    private final String checkpointDefault;
    private final Coordinator coordinator;
    private final Supplier supplier;
    private final Augmenter augmenter;
    private final AugmenterFilter augmenterFilter;
    private final Seeker seeker;
    private final Partitioner partitioner;
    private final Applier applier;
    private final Metrics<?> metrics;
    private final String errorCounter;
    private final CheckpointApplier checkpointApplier;
    private final WebServer webServer;
    private final AtomicLong checkPointDelay;

    private final Streams<Collection<AugmentedEvent>, Collection<AugmentedEvent>> destinationStream;
    private final Streams<RawEvent, Collection<AugmentedEvent>> sourceStream;

    private final String METRIC_COORDINATOR_DELAY               = MetricRegistry.name("coordinator", "delay");
    private final String METRIC_STREAM_DESTINATION_QUEUE_SIZE   = MetricRegistry.name("streams", "destination", "queue", "size");
    private final String METRIC_STREAM_SOURCE_QUEUE_SIZE        = MetricRegistry.name("streams", "source", "queue", "size");

    public Replicator(final Map<String, Object> configuration) {

        Object checkpointPath = configuration.get(Configuration.CHECKPOINT_PATH);
        Object checkpointDefault = configuration.get(Configuration.CHECKPOINT_DEFAULT);

        Objects.requireNonNull(checkpointPath, String.format("Configuration required: %s", Configuration.CHECKPOINT_PATH));

        int threads = Integer.parseInt(configuration.getOrDefault(Configuration.REPLICATOR_THREADS, "1").toString());
        int tasks = Integer.parseInt(configuration.getOrDefault(Configuration.REPLICATOR_TASKS, "1").toString());
        int queueSize = Integer.parseInt(configuration.getOrDefault(Configuration.REPLICATOR_QUEUE_SIZE, "10000").toString());
        long queueTimeout = Long.parseLong(configuration.getOrDefault(Configuration.REPLICATOR_QUEUE_TIMEOUT, "300").toString());

        boolean overrideCheckpointStartPosition = Boolean.parseBoolean(configuration.getOrDefault(Configuration.OVERRIDE_CHECKPOINT_START_POSITION, false).toString());
        String overrideCheckpointBinLogFileName = configuration.getOrDefault(Configuration.OVERRIDE_CHECKPOINT_BINLOG_FILENAME, "").toString();
        long overrideCheckpointBinlogPosition = Long.parseLong(configuration.getOrDefault(Configuration.OVERRIDE_CHECKPOINT_BINLOG_POSITION, "0").toString());
        String overrideCheckpointGtidSet = configuration.getOrDefault(Configuration.OVERRIDE_CHECKPOINT_GTID_SET, "").toString();


        this.checkpointPath = checkpointPath.toString();

        this.checkpointDefault = (checkpointDefault != null) ? (checkpointDefault.toString()) : (null);

        this.webServer = WebServer.build(configuration);

        this.metrics = Metrics.build(configuration, webServer.getServer());

        this.errorCounter = MetricRegistry.name(
                String.valueOf(configuration.getOrDefault(Metrics.Configuration.BASE_PATH, "replicator")),
                "error"
        );

        this.coordinator = Coordinator.build(configuration);

        this.supplier = Supplier.build(configuration);

        this.augmenter = Augmenter.build(configuration);

        this.augmenterFilter = AugmenterFilter.build(configuration);

        this.seeker = Seeker.build(configuration);

        this.partitioner = Partitioner.build(configuration);

        this.applier = Applier.build(configuration);

        this.checkPointDelay = new AtomicLong(0L);

        this.checkpointApplier = CheckpointApplier.build(configuration,
                this.coordinator,
                this.checkpointPath,
                safeCheckpoint -> this.checkPointDelay.set((System.currentTimeMillis() - safeCheckpoint.getTimestamp()) / 1000)
        );

        this.metrics.register(METRIC_COORDINATOR_DELAY, (Gauge<Long>) () -> this.checkPointDelay.get());

        // --------------------------------------------------------------------
        // Setup streams/pipelines:
        //
        // Notes:
        //
        //  Supplying the input data has three modes:
        //      1. Short Circuit Push:
        //          - Input data for this stream/data-pipeline will be provided by
        //            by calling the push() method of the pipeline instance, which will not
        //            use any queues but will directly pass the item to the process()
        //            method.
        //      2. Queued Push:
        //          - If pipeline has a queue then the push() method will default to
        //            adding items to that queue and then internal consumer is automatically
        //            initialized as the poller of that queue
        //      3. Pull:
        //          - There is no queue and the internal consumer is passed as a lambda
        //            function which knows hot to get data from an external data source.
        this.destinationStream = Streams.<Collection<AugmentedEvent>>builder()
                .threads(threads)
                .tasks(tasks)
                .partitioner((events, totalPartitions) -> {
                    this.metrics.getRegistry()
                            .counter("hbase.streams.destination.partitioner.event.apply.attempt").inc(1L);
                    Integer partitionNumber = this.partitioner.apply(events.iterator().next(), totalPartitions);
                    this.metrics.getRegistry()
                            .counter("hbase.streams.destination.partitioner.event.apply.success").inc(1L);
                    return partitionNumber;
                })
                .useDefaultQueueType()
                .usePushMode()
                .setSink(this.applier)
                .post((events, task) -> {
                    for (AugmentedEvent event : events) {
                        this.checkpointApplier.accept(event, task);
                    }
                }).build();

        this.metrics.register(METRIC_STREAM_DESTINATION_QUEUE_SIZE,(Gauge<Integer>) () -> this.destinationStream.size());

        this.sourceStream = Streams.<RawEvent>builder()
                .usePushMode()
                .process(this.augmenter)
                .process(this.seeker)
                .process(this.augmenterFilter)
                .setSink((events) -> {
                    Map<Integer, Collection<AugmentedEvent>> splitEventsMap = new HashMap<>();
                    for (AugmentedEvent event : events) {
                        this.metrics.getRegistry()
                                .counter("streams.partitioner.event.apply.attempt").inc(1L);
                        splitEventsMap.computeIfAbsent(
                                this.partitioner.apply(event, tasks), partition -> new ArrayList<>()
                        ).add(event);
                        metrics.getRegistry()
                                .counter("streams.partitioner.event.apply.success").inc(1L);
                    }
                    for (Collection<AugmentedEvent> splitEvents : splitEventsMap.values()) {
                        this.destinationStream.push(splitEvents);
                    }
                    return true;
                }).build();

        this.metrics.register(METRIC_STREAM_SOURCE_QUEUE_SIZE, (Gauge<Integer>) () -> this.sourceStream.size());

        Consumer<Exception> exceptionHandle = (exception) -> {

            this.metrics.incrementCounter(this.errorCounter, 1);

            if (ForceRewindException.class.isInstance(exception)) {

                Replicator.LOG.warn(exception.getMessage(), exception);
                this.rewind();

            } else {

                Replicator.LOG.error(exception.getMessage(), exception);
                this.stop();

            }
        };

        this.coordinator.onLeadershipTake(() -> {
            try {
                Replicator.LOG.info("starting replicator");

                Replicator.LOG.info("starting streams applier");
                this.destinationStream.start();

                Replicator.LOG.info("starting streams supplier");
                this.sourceStream.start();

                Replicator.LOG.info("starting supplier");

                Checkpoint from;

                if(overrideCheckpointStartPosition){

                    if (overrideCheckpointBinLogFileName != null && !overrideCheckpointBinLogFileName.equals("")) {

                        LOG.info("Checkpoint startup mode: override Binlog filename and position:" +
                                overrideCheckpointBinLogFileName +
                                ":" +
                                overrideCheckpointBinlogPosition);

                        from = this.seeker.seek(
                                new Checkpoint(new Binlog(overrideCheckpointBinLogFileName, overrideCheckpointBinlogPosition))
                        );

                    } else if (overrideCheckpointGtidSet != null && !overrideCheckpointGtidSet.equals("")) {

                       LOG.info("Checkpoint startup mode: override gtidSet: " + overrideCheckpointGtidSet);
                       from = this.seeker.seek(
                               new Checkpoint(overrideCheckpointGtidSet)
                       );

                    } else {
                        throw new RuntimeException("Impossible case!");
                    }

                } else {
                    LOG.info("Checkpoint startup mode: loading safe checkpoint from zookeeper");

                    from = this.seeker.seek(
                        this.loadSafeCheckpoint()
                    );
                }

                this.supplier.start(from);

                Replicator.LOG.info("replicator started");
            } catch (IOException | InterruptedException exception) {
                exceptionHandle.accept(exception);
            }
        });

        this.coordinator.onLeadershipLose(() -> {
            try {
                Replicator.LOG.info("stopping replicator");

                Replicator.LOG.info("stopping supplier");
                this.supplier.stop();

                Replicator.LOG.info("stopping streams supplier");
                this.sourceStream.stop();

                Replicator.LOG.info("stopping streams applier");
                this.destinationStream.stop();

                Replicator.LOG.info("stopping web server");
                this.webServer.stop();

                Replicator.LOG.info("replicator stopped");
            } catch (IOException | InterruptedException exception) {
                exceptionHandle.accept(exception);
            }
        });

        this.supplier.onEvent((event) -> {
            this.sourceStream.push(event);
        });

        this.supplier.onException(exceptionHandle);
        this.sourceStream.onException(exceptionHandle);
        this.destinationStream.onException(exceptionHandle);

    }

    public Applier getApplier() {
        return this.applier;
    }

    private Checkpoint loadSafeCheckpoint() throws IOException {
        Checkpoint checkpoint = this.coordinator.loadCheckpoint(this.checkpointPath);

        if (checkpoint == null && this.checkpointDefault != null) {
            checkpoint = new ObjectMapper().readValue(this.checkpointDefault, Checkpoint.class);
        }

        return checkpoint;
    }

    public void start() {

        Replicator.LOG.info("starting webserver");
        try {
            this.webServer.start();
        } catch (IOException e) {
            Replicator.LOG.error("error starting webserver", e);
        }

        Replicator.LOG.info("starting coordinator");
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
            Replicator.LOG.info("stopping coordinator");
            this.coordinator.stop();

            Replicator.LOG.info("closing augmenter");
            this.augmenter.close();

            Replicator.LOG.info("closing seeker");
            this.seeker.close();

            Replicator.LOG.info("closing partitioner");
            this.partitioner.close();

            Replicator.LOG.info("closing applier");
            this.applier.close();

            Replicator.LOG.info("stopping web server");
            this.webServer.stop();

            Replicator.LOG.info("closing metrics applier");
            this.metrics.close();

            Replicator.LOG.info("closing checkpoint applier");
            this.checkpointApplier.close();
        } catch (IOException exception) {
            Replicator.LOG.error("error stopping coordinator", exception);
        }
    }

    public void rewind() {
        try {
            Replicator.LOG.info("rewinding supplier");

            this.supplier.disconnect();
            this.supplier.connect(this.seeker.seek(this.loadSafeCheckpoint()));
        } catch (IOException exception) {
            Replicator.LOG.error("error rewinding supplier", exception);
        }
    }

    /*
     * Start the JVM with the argument -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
     */
    public static void main(String[] arguments) {
        Options options = new Options();

        options.addOption(Option.builder().longOpt("help").desc("print the help message").build());
        options.addOption(Option.builder().longOpt("config").argName("key-value").desc("the configuration setSink be used with the format <key>=<value>").hasArgs().build());
        options.addOption(Option.builder().longOpt("config-file").argName("filename").desc("the configuration file setSink be used (YAML)").hasArg().build());
        options.addOption(Option.builder().longOpt("supplier").argName("supplier").desc("the supplier setSink be used").hasArg().build());
        options.addOption(Option.builder().longOpt("applier").argName("applier").desc("the applier setSink be used").hasArg().build());
        options.addOption(Option.builder().longOpt("secret-file").argName("filename").desc("the secret file which has Mysql user/password config (JSON)").hasArg().build());


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

                if (line.hasOption("secret-file")) {
                    configuration.putAll(new ObjectMapper().readValue(
                            new File(line.getOptionValue("secret-file")),
                            new TypeReference<Map<String, String>>() {

                    }));
                }

                if (line.hasOption("supplier")) {
                    configuration.put(Supplier.Configuration.TYPE, line.getOptionValue("supplier").toUpperCase());
                }

                if (line.hasOption("applier")) {
                    configuration.put(Applier.Configuration.TYPE, line.getOptionValue("applier").toUpperCase());
                }


                new BootstrapReplicator(configuration).run();

                Replicator replicator = new Replicator(configuration);

                Runtime.getRuntime().addShutdownHook(new Thread(replicator::stop));

                replicator.start();
            }
        } catch (Exception exception) {
            LOG.error("Error in replicator", exception);
            new HelpFormatter().printHelp(Replicator.COMMAND_LINE_SYNTAX, null, options, exception.getMessage());
        }
    }
}
