package com.booking.replication.checkpoint;

import com.codahale.metrics.Gauge;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.CheckpointStorage;
import com.booking.replication.supplier.mysql.binlog.gtid.GtidSetAlgebra;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.MetricRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.stream.Collectors;


public class CoordinatorCheckpointApplier implements CheckpointApplier {

    private static final Logger LOG = LogManager.getLogger(CoordinatorCheckpointApplier.class);

    private final CheckpointStorage storage;
    private final String path;
    private final AtomicLong lastExecution;
    private final AtomicLong lastCheckpointTime;
    private final ScheduledExecutorService executor;
    private final GtidSetAlgebra gtidSetAlgebra;

    private final Map<Integer, CheckpointBuffer> taskCheckpointBuffer;

    private final String METRIC_KEY = MetricRegistry.name("coordinator", "timediff");

    public CoordinatorCheckpointApplier(CheckpointStorage storage, String path, long period,  boolean transactionEnabled, MetricRegistry metricRegistry) {

        this.storage = storage;
        this.path = path;
        this.lastExecution      = new AtomicLong();
        this.lastCheckpointTime = new AtomicLong();

        this.gtidSetAlgebra = new GtidSetAlgebra();

        this.taskCheckpointBuffer = new ConcurrentHashMap<>();

        this.executor = Executors.newSingleThreadScheduledExecutor();

        metricRegistry.register(METRIC_KEY, (Gauge<Long>)() -> (System.currentTimeMillis() - this.lastCheckpointTime.get()) /1000 );

        this.executor.scheduleAtFixedRate(() -> {

            List<Checkpoint> checkpointsSeenSoFar = new ArrayList<>();
            for (CheckpointBuffer checkpointBuffer: taskCheckpointBuffer.values()) {
                    checkpointsSeenSoFar.addAll(checkpointBuffer.getBufferedSoFar());
            }

            List<Checkpoint> checkpointsSeenWithGtidSet = checkpointsSeenSoFar
                    .stream()
                    .filter(c -> (c.getGtidSet() != null && !c.getGtidSet().equals(""))).collect(Collectors.toList());

            int currentSize = checkpointsSeenWithGtidSet.size();

            LOG.info("Checkpoints seen in last " + period + "ms, [total/withGTIDSet]: " + checkpointsSeenSoFar.size() + "/" + checkpointsSeenWithGtidSet.size());

            if (currentSize > 0) {

                Checkpoint safeCheckpoint = gtidSetAlgebra.getSafeCheckpoint(checkpointsSeenSoFar);

                if (safeCheckpoint != null && !safeCheckpoint.getGtidSet().equals("")) {

                    LOG.info("CheckpointApplier, storing safe checkpoint: " + safeCheckpoint.getGtidSet());
                    try {
                        this.storage.saveCheckpoint(this.path, safeCheckpoint);
                        CoordinatorCheckpointApplier.LOG.info("CheckpointApplier, stored checkpoint: " + safeCheckpoint.toString());
                        this.lastCheckpointTime.set(safeCheckpoint.getTimestamp());
                        this.lastExecution.set(System.currentTimeMillis());
                    } catch (IOException exception) {
                        CoordinatorCheckpointApplier.LOG.info( "error saving checkpoint", exception);
                    }
                } else {
                    throw new RuntimeException("Could not find safe checkpoint. Not safe to continue running!");
                }

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
