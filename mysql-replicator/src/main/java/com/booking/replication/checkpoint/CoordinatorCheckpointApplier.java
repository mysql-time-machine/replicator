package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.CheckpointStorage;
import com.booking.replication.supplier.mysql.binlog.gtid.GtidSetAlgebra;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CoordinatorCheckpointApplier implements CheckpointApplier {

    private static final Logger LOG = LogManager.getLogger(CoordinatorCheckpointApplier.class);

    private final CheckpointStorage storage;
    private final String path;
    private final AtomicLong lastExecution;
    private final ScheduledExecutorService executor;
    private final GtidSetAlgebra gtidSetAlgebra;

    private final Map<Integer, CheckpointBuffer> taskCheckpointBuffer;

    public CoordinatorCheckpointApplier(CheckpointStorage storage, String path, long period,  boolean transactionEnabled, Consumer<Checkpoint> safeCheckpointCallback) {

        this.storage = storage;
        this.path = path;
        this.lastExecution = new AtomicLong();

        this.gtidSetAlgebra = new GtidSetAlgebra();

        this.taskCheckpointBuffer = new ConcurrentHashMap<>();

        this.executor = Executors.newSingleThreadScheduledExecutor();

        this.executor.scheduleAtFixedRate(() -> {

            List<Checkpoint> checkpointsSeenSoFar = new ArrayList<>();
            for (CheckpointBuffer checkpointBuffer: taskCheckpointBuffer.values()) {
                checkpointsSeenSoFar.addAll(checkpointBuffer.getBufferedSoFar());
            }

            List<Checkpoint> checkpointsSeenWithGtidSet = new ArrayList<>();
            for (Checkpoint c: checkpointsSeenSoFar) {
                if ((c.getGtidSet() != null) && !c.getGtidSet().equals("") && c.getTimestamp() > 0) {
                    checkpointsSeenWithGtidSet.add(c);
                }
            }

            int currentSize = checkpointsSeenWithGtidSet.size();

            if ( checkpointsSeenSoFar.size() > 0 ) {
                // Unnecessary to write this out in the event that no checkpoints were seen, it just creates noise
                LOG.info("Checkpoints seen in last " + period + "ms, [total/valid]: " + checkpointsSeenSoFar.size() + "/" + checkpointsSeenWithGtidSet.size());
            }

            if (currentSize > 0) {

                Checkpoint safeCheckpoint = gtidSetAlgebra.getSafeCheckpoint(checkpointsSeenWithGtidSet);

                LOG.info("CheckpointApplier, calculated safe checkpoint from checkpointsSeenSoFar.");

                if (safeCheckpoint != null && !safeCheckpoint.getGtidSet().equals("")) {

                    LOG.info("CheckpointApplier, storing safe checkpoint: " + safeCheckpoint.getGtidSet());

                    try {

                        this.storage.saveCheckpoint(this.path, safeCheckpoint);
                        CoordinatorCheckpointApplier.LOG.info("CheckpointApplier, stored checkpoint: " + safeCheckpoint.toString());

                        this.lastExecution.set(System.currentTimeMillis());

                        safeCheckpointCallback.accept(safeCheckpoint);

                    } catch (IOException exception) {
                        CoordinatorCheckpointApplier.LOG.info( "error saving checkpoint", exception);
                    }
                } else {
                    throw new RuntimeException("Could not find safe checkpoint. Not safe to continue running!");
                }

            } else {
                LOG.info("CheckpointApplier: No checkpoints observed since last checkpointStore");
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public  void accept(AugmentedEvent event, Integer task) {

        synchronized (taskCheckpointBuffer) {
            if (taskCheckpointBuffer.get(task) == null) {
                taskCheckpointBuffer.put(task, new CheckpointBuffer());
            }
        }

        Checkpoint checkpoint = event.getHeader().getCheckpoint();

        taskCheckpointBuffer.get(task).writeToBuffer(checkpoint);

    }

    @Override
    public void close() {
        try {
            this.executor.shutdown();
            this.executor.awaitTermination(5L, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            throw new RuntimeException(exception);
        } finally {
            this.executor.shutdownNow();
        }
    }

}
