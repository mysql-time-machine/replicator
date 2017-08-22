package com.booking.replication.monitor;

import com.booking.replication.pipeline.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bdevetak on 26/11/15.
 */
public class Overseer extends Thread {

    private PipelineOrchestrator pipelineOrchestrator;
    private BinlogEventProducer producer;
    private final PipelinePosition pipelinePosition;

    private volatile boolean doMonitor = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(Overseer.class);

    /**
     * Watchdog for various replicator threads.
     *
     * @param producer         Producer thread
     * @param orchestrator     Orchestrator thread
     * @param pipelinePosition Binlog position information
     */
    public Overseer(
            BinlogEventProducer producer,
            PipelineOrchestrator orchestrator,
            PipelinePosition pipelinePosition
    ) {
        this.producer = producer;
        pipelineOrchestrator = orchestrator;
        this.pipelinePosition = pipelinePosition;
    }

    @Override
    public void run() {
        while (doMonitor) {
            try {
                // make sure that producer is running every 1s
                Thread.sleep(1000);
                makeSureProducerIsRunning();

                // TODO: add status/health checks for pipelineOrchestrator and applier
                // makeSurePipelineIsRunning();

            } catch (InterruptedException e) {
                LOGGER.error("Overseer thread interrupted", e);
                doMonitor = false;
            }
        }
    }

    public void stopMonitoring() {
        doMonitor = false;
    }

    public void startMonitoring() {
        doMonitor = true;
    }

    private void makeSureProducerIsRunning() {
        // TODO: merge into health-checker class
        if (!producer.getOpenReplicator().isRunning()) {
            CurrentTransaction currentTransaction = pipelineOrchestrator.getCurrentTransaction();
            if (currentTransaction == null || !currentTransaction.isRewinded()) {
                LOGGER.error("Producer stopped running at pipeline position: "
                        + pipelinePosition.getCurrentPosition().getBinlogFilename()
                        + ":"
                        + pipelinePosition.getCurrentPosition().getBinlogPosition()
                        + ". Requesting pipeline shutdown...");
                stopMonitoring();
                pipelineOrchestrator.requestReplicatorShutdown();
            }
        }
    }
}