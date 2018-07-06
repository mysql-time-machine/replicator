package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventTransaction;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.CheckpointStorage;
import com.booking.replication.streams.Streams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CoordinatorCheckpointApplier implements CheckpointApplier {
    private static final Logger LOG = Logger.getLogger(CoordinatorCheckpointApplier.class.getName());

    private final CheckpointStorage storage;
    private final String path;
    private final Map<Integer, AugmentedEventTransaction> taskTransactionMap;
    private final Map<Integer, Checkpoint> taskCheckpointMap;
    private final AtomicInteger totalTasks;
    private final ScheduledExecutorService executor;

    public CoordinatorCheckpointApplier(CheckpointStorage storage, String path, long period) {
        this.storage = storage;
        this.path = path;
        this.taskTransactionMap = new ConcurrentHashMap<>();
        this.taskCheckpointMap = new ConcurrentHashMap<>();
        this.totalTasks = new AtomicInteger();
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.executor.scheduleAtFixedRate(() -> {
            if (this.totalTasks.get() > 0 && this.totalTasks.get() == this.taskCheckpointMap.size()) {
                Checkpoint minimumCheckpoint = null;

                for (Checkpoint taskCheckpoint : this.taskCheckpointMap.values()) {
                    if (minimumCheckpoint == null || minimumCheckpoint.compareTo(taskCheckpoint) < 0) {
                        minimumCheckpoint = taskCheckpoint;
                    }
                }

                try {
                    this.storage.saveCheckpoint(this.path, minimumCheckpoint);
                } catch (IOException exception) {
                    CoordinatorCheckpointApplier.LOG.log(Level.WARNING, "error saving checkpoint", exception);
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void accept(AugmentedEvent augmentedEvent, Streams.Task task) {
        Checkpoint checkpoint = augmentedEvent.getHeader().getCheckpoint();
        AugmentedEventTransaction transaction = augmentedEvent.getHeader().getEventTransaction();

        int currentTask = task.getCurrent();
        int totalTasks = task.getTotal();

        if (transaction != null && transaction.compareTo(this.taskTransactionMap.get(currentTask)) > 0) {
            this.taskCheckpointMap.put(currentTask, checkpoint);
            this.taskTransactionMap.put(currentTask, transaction);
            this.totalTasks.set(totalTasks);
        }
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
