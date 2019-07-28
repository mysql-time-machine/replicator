package com.booking.replication.flink;

import com.booking.replication.Replicator;
import com.booking.replication.applier.Seeker;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.supplier.Supplier;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class BinlogSource extends RichSourceFunction<AugmentedEvent> implements CheckpointedFunction {

    private static final Logger LOG = LogManager.getLogger(BinlogSource.class);

    // control variables
    private long count = 0L;
    private volatile boolean isRunning = true;
    private Map<String, Object> configuration;
    private final String checkpointPath;

    private transient ListState<Long> checkpointedCount;

    public BinlogSource(Map<String, Object> configuration) throws IOException {
        this.configuration = configuration;
        this.checkpointPath = configuration.get(Replicator.Configuration.CHECKPOINT_PATH).toString();
    }

    @Override
    public void open(Configuration parameters) {

    }

    @Override
    public void run(SourceContext<AugmentedEvent> sourceContext) throws Exception {

        // vars
        boolean overrideCheckpointStartPosition = Boolean.parseBoolean(configuration.getOrDefault(Replicator.Configuration.OVERRIDE_CHECKPOINT_START_POSITION, false).toString());
        String overrideCheckpointBinLogFileName = configuration.getOrDefault(Replicator.Configuration.OVERRIDE_CHECKPOINT_BINLOG_FILENAME, "").toString();
        long overrideCheckpointBinlogPosition   = Long.parseLong(configuration.getOrDefault(Replicator.Configuration.OVERRIDE_CHECKPOINT_BINLOG_POSITION, "0").toString());
        String overrideCheckpointGtidSet        = configuration.getOrDefault(Replicator.Configuration.OVERRIDE_CHECKPOINT_GTID_SET, "").toString();

        LinkedBlockingQueue<AugmentedEvent> incomingAugmentedEvents = new LinkedBlockingQueue<>();

        // augmenter
        Augmenter augmenter = Augmenter.build(configuration);

        // augmenterFilter
        AugmenterFilter augmenterFilter = AugmenterFilter.build(configuration);

        // supplier
        Supplier supplier = Supplier.build(configuration);

        supplier.onEvent((event) -> {

            Collection<AugmentedEvent> augmentedEvents = augmenter.apply(event);

            Collection<AugmentedEvent> filteredEvents = augmenterFilter.apply(augmentedEvents);

            if (augmentedEvents != null) {
                for (AugmentedEvent filteredEvent : filteredEvents) {
                    sourceContext.collectWithTimestamp(
                            filteredEvent,
                            filteredEvent.getHeader().getTimestamp()
                    );
                }
            }

        });

        // coordinator
        Coordinator coordinator = Coordinator.build(configuration);

        coordinator.onLeadershipTake(() -> {

            try {
                LOG.info("Acquired leadership. Loading checkpoint.");

                Checkpoint binlogCheckpoint = null;

                binlogCheckpoint = getCheckpoint(
                        overrideCheckpointStartPosition,
                        overrideCheckpointBinLogFileName,
                        overrideCheckpointBinlogPosition,
                        overrideCheckpointGtidSet,
                        coordinator
                );

                LOG.info("Loaded checkpoint. Starting supplier.");

                supplier.start(binlogCheckpoint);

                LOG.info("Supplier started.");

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        coordinator.onLeadershipLose(() -> {

            try {

                LOG.info("Stopping supplier");

                supplier.stop();

                // TODO
                // Replicator.LOG.info("stopping web server");
                // this.webServer.stop();

                LOG.info("Supplier stopped");

            } catch (IOException e) {
                e.printStackTrace();
//                exceptionHandle.accept(exception);
            }
        });

        LOG.info("starting coordinator");

        coordinator.start();

        // main loop
        while (isRunning) {

            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (sourceContext.getCheckpointLock()) {

                System.out.println("Source: main loop count #" + count); // this is ordered;

                Thread.sleep(100);

                count++;

            }
        }

        LOG.info("closing augmenter");
        augmenter.close();

        LOG.info("stopping supplier");
        supplier.stop();

        LOG.info("stopping coordinator");
        coordinator.stop();

    }

    public void cancel() {
        isRunning = false;
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {

        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("count", Long.class));

        if (context.isRestored()) {
            for (Long count : this.checkpointedCount.get()) {
                this.count = count;
            }
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedCount.clear();
        this.checkpointedCount.add(count);
    }

    private Checkpoint getCheckpoint(
            boolean overrideCheckpointStartPosition,
            String overrideCheckpointBinLogFileName,
            long overrideCheckpointBinlogPosition,
            String overrideCheckpointGtidSet,
            Coordinator coordinator) throws IOException {

        Checkpoint from;

        Seeker seeker = Seeker.build(configuration);

        if(overrideCheckpointStartPosition){

            if (overrideCheckpointBinLogFileName != null && !overrideCheckpointBinLogFileName.equals("")) {

                LOG.info("Checkpoint startup mode: override Binlog filename and position:" +
                        overrideCheckpointBinLogFileName +
                        ":" +
                        overrideCheckpointBinlogPosition);

                from = seeker.seek(
                        new Checkpoint(new Binlog(overrideCheckpointBinLogFileName, overrideCheckpointBinlogPosition))
                );

            } else if (overrideCheckpointGtidSet != null && !overrideCheckpointGtidSet.equals("")) {

                LOG.info("Checkpoint startup mode: override gtidSet: " + overrideCheckpointGtidSet);
                from = seeker.seek(
                        new Checkpoint(overrideCheckpointGtidSet)
                );

            } else {
                throw new RuntimeException("Impossible case!");
            }

        } else {
            LOG.info("Checkpoint startup mode: loading safe checkpoint from zookeeper");

            from = seeker.seek(
                    this.loadSafeCheckpoint(coordinator)
            );
        }

        seeker.close();

        return from;
    }

    private Checkpoint loadSafeCheckpoint(Coordinator coordinator) throws IOException {

        Checkpoint checkpoint = coordinator.loadCheckpoint(this.checkpointPath);

        Object checkpointDefault = configuration.get(Replicator.Configuration.CHECKPOINT_DEFAULT);

        String checkpointDefaultString = (checkpointDefault != null) ? (checkpointDefault.toString()) : (null);

        if (checkpoint == null && checkpointDefaultString != null) {
            checkpoint = new ObjectMapper().readValue(checkpointDefaultString, Checkpoint.class);
        }

        return checkpoint;
    }

    public synchronized boolean isRunning() {
        return isRunning;
    }

}
