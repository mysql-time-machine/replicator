package com.booking.replication.flink.sources.binlog;

import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.Supplier;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class BinlogSource extends RichSourceFunction<List<AugmentedEvent>> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogSource.class);

    // serializable state
    private volatile boolean isRunning;

    private Map<String, Object> configuration;
    private ExecutorService executorService;

    private ListState<Checkpoint> checkpointListState;
    private Checkpoint currentCheckpoint;

    // augmenter
    private transient Augmenter augmenter;
    // augmenterFilter
    private transient AugmenterFilter augmenterFilter;
    // supplier
    private transient Supplier supplier;
    // coordinator

    public BinlogSource(Map<String, Object> configuration) {
        LOG.info("Binlog Source constructor");
        this.isRunning = true;
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) {
        LOG.info("Open");
        this.init();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        this.checkpointListState.clear();
        LOG.info("Checkpointing now: {}", currentCheckpoint);
        this.checkpointListState.add(currentCheckpoint);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointListState = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("binlogCheckpoints", Checkpoint.class));

        LOG.info("Initializing state");

        if (context.isRestored()) {
            for (Checkpoint checkpoint : checkpointListState.get()) {
                if (checkpoint != null && checkpoint.getGtidSet() != null) {
                    this.currentCheckpoint = checkpoint;
                    this.init();
                    LOG.info("restored context in binlogSource, checkpoint => #" + checkpoint.getGtidSet());
                }
            }
        }
    }

    private void init() {
        LOG.info("Initializing state");
        try {
            // augmenter
            this.augmenter = Augmenter.build(configuration);

            // augmenterFilter
            this.augmenterFilter = AugmenterFilter.build(configuration);

            // supplier
            this.supplier = Supplier.build(configuration);

            // callbacks
            this.executorService = Executors.newSingleThreadExecutor();
        } catch (Exception e) {
            LOG.error("Error in init ", e);
        }
    }

    // TODO leaving this as a bootstrap mechanism from a GTID checkpoint.
    //  We need to figure how to bootstrap flink checkpoints
    private Checkpoint getBootstrapCheckPoint() {
        // Bootstrap checkpoint from config for first time run
        String checkpointJson = configuration.getOrDefault("checkpoint.bootstrap", "{}").toString();

        Checkpoint checkpoint = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            checkpoint = mapper.readValue(checkpointJson, Checkpoint.class);
        } catch (IOException e) {
            LOG.error("Error in converting JSON to checkpoint " + checkpointJson, e);
        }

        return checkpoint;
    }

    @Override
    public void run(SourceContext<List<AugmentedEvent>> sourceContext) {

        // Register callbacks to push to sourceContext
        this.supplier.onEvent((event) -> {
            Collection<AugmentedEvent> augmentedEvents = this.augmenter.apply(event);

            if (augmentedEvents != null) { // <- null if event buffered, but commit not reached yet
                Collection<AugmentedEvent> filteredEvents = this.augmenterFilter.apply(augmentedEvents);

                augmentedEvents.stream().findFirst()
                        .ifPresent(cp -> this.currentCheckpoint = cp.getHeader().getCheckpoint());

                if (this.isRunning && filteredEvents != null) {
                    sourceContext.collect((List<AugmentedEvent>) filteredEvents);
                }
            }
        });

        this.supplier.onException(Throwable::printStackTrace);
        this.executorService.submit((Callable<Void>) () -> {
            LOG.info("String binlog supplier");
            try {
                this.supplier.start(this.currentCheckpoint != null ?
                        this.currentCheckpoint : getBootstrapCheckPoint());
            } catch (IOException e) {
                LOG.error("Error while starting supplier ", e);
            }
            return null;
        });

        // Have the run loop running while the source is running.
        while (this.isRunning) {
        }
    }

    @Override
    public void cancel() {
        LOG.info("cancel called, stopping BinlogSource");

        try {
            if (this.supplier != null) {
                this.supplier.stop();
            }
            if (this.augmenter != null) {
                this.augmenter.close();
            }
            if (this.executorService != null) {
                this.executorService.shutdown();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.isRunning = false;
    }
}
