package com.booking.replication.runtime.standalone;
import com.booking.replication.Replicator;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.BinlogEventPartitioner;
import com.booking.replication.applier.Seeker;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.checkpoint.CheckpointApplier;
import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.commons.metrics.Metrics;
import com.booking.replication.controller.WebServer;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.streams.Streams;
import com.booking.replication.supplier.Supplier;

import com.booking.replication.supplier.model.RawEvent;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ReplicatorStandaloneApplication {

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
        private final BinlogEventPartitioner partitioner;
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

        public ReplicatorStandaloneApplication(final Map<String, Object> configuration) {

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

            this.partitioner = BinlogEventPartitioner.build(configuration);

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

                    ReplicatorStandaloneApplication.LOG.warn(exception.getMessage(), exception);
                    this.rewind();

                } else {

                    ReplicatorStandaloneApplication.LOG.error(exception.getMessage(), exception);
                    this.stop();

                }
            };

            this.coordinator.onLeadershipTake(() -> {
                try {
                    ReplicatorStandaloneApplication.LOG.info("starting replicator");

                    ReplicatorStandaloneApplication.LOG.info("starting streams applier");
                    this.destinationStream.start();

                    ReplicatorStandaloneApplication.LOG.info("starting streams supplier");
                    this.sourceStream.start();

                    ReplicatorStandaloneApplication.LOG.info("starting supplier");

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

                    ReplicatorStandaloneApplication.LOG.info("replicator started");
                } catch (IOException | InterruptedException exception) {
                    exceptionHandle.accept(exception);
                }
            });

            this.coordinator.onLeadershipLose(() -> {
                try {
                    ReplicatorStandaloneApplication.LOG.info("stopping replicator");

                    ReplicatorStandaloneApplication.LOG.info("stopping supplier");
                    this.supplier.stop();

                    ReplicatorStandaloneApplication.LOG.info("stopping streams supplier");
                    this.sourceStream.stop();

                    ReplicatorStandaloneApplication.LOG.info("stopping streams applier");
                    this.destinationStream.stop();

                    ReplicatorStandaloneApplication.LOG.info("stopping web server");
                    this.webServer.stop();

                    ReplicatorStandaloneApplication.LOG.info("replicator stopped");
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

            ReplicatorStandaloneApplication.LOG.info("starting webserver");
            try {
                this.webServer.start();
            } catch (IOException e) {
                ReplicatorStandaloneApplication.LOG.error("error starting webserver", e);
            }

            ReplicatorStandaloneApplication.LOG.info("starting coordinator");
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
                ReplicatorStandaloneApplication.LOG.info("stopping coordinator");
                this.coordinator.stop();

                ReplicatorStandaloneApplication.LOG.info("closing augmenter");
                this.augmenter.close();

                ReplicatorStandaloneApplication.LOG.info("closing seeker");
                this.seeker.close();

                ReplicatorStandaloneApplication.LOG.info("closing partitioner");
                this.partitioner.close();

                ReplicatorStandaloneApplication.LOG.info("closing applier");
                this.applier.close();

                ReplicatorStandaloneApplication.LOG.info("stopping web server");
                this.webServer.stop();

                ReplicatorStandaloneApplication.LOG.info("closing metrics applier");
                this.metrics.close();

                ReplicatorStandaloneApplication.LOG.info("closing checkpoint applier");
                this.checkpointApplier.close();
            } catch (IOException exception) {
                ReplicatorStandaloneApplication.LOG.error("error stopping coordinator", exception);
            }
        }

        public void rewind() {
            try {
                ReplicatorStandaloneApplication.LOG.info("rewinding supplier");

                this.supplier.disconnect();
                this.supplier.connect(this.seeker.seek(this.loadSafeCheckpoint()));
            } catch (IOException exception) {
                ReplicatorStandaloneApplication.LOG.error("error rewinding supplier", exception);
            }
        }

    }
