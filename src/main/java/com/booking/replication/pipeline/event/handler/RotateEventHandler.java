package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Coordinator;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.RotateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class RotateEventHandler implements BinlogEventV4Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RotateEventHandler.class);

    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final PipelinePosition pipelinePosition;
    private final String lastBinlogFileName;


    public RotateEventHandler(EventHandlerConfiguration eventHandlerConfiguration, PipelinePosition pipelinePosition, String lastBinlogFileName) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelinePosition = pipelinePosition;
        this.lastBinlogFileName = lastBinlogFileName;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }

    @Override
    public void apply(BinlogEventV4 binlogEventV4, CurrentTransaction currentTransaction) throws EventHandlerApplyException, ApplierException, IOException {
        final RotateEvent event = (RotateEvent) binlogEventV4;
        try {
            eventHandlerConfiguration.getApplier().applyRotateEvent(event);
        } catch (IOException e) {
            throw new EventHandlerApplyException("Failed to apply event", e);
        }
        LOGGER.info("End of binlog file. Waiting for all tasks to finish before moving forward...");

        //TODO: Investigate if this is the right thing to do.

        eventHandlerConfiguration.getApplier().waitUntilAllRowsAreCommitted(event);


        String currentBinlogFileName = pipelinePosition.getCurrentPosition().getBinlogFilename();
        long currentBinlogPosition = pipelinePosition.getCurrentPosition().getBinlogPosition();
        // binlog begins on position 4
        if (currentBinlogPosition <= 0L) currentBinlogPosition = 4;

        String nextBinlogFileName = event.getBinlogFileName().toString();

        LOGGER.info("All rows committed, moving to next binlog " + nextBinlogFileName);

        String pseudoGTID = pipelinePosition.getCurrentPseudoGTID();
        String pseudoGTIDFullQuery = pipelinePosition.getCurrentPseudoGTIDFullQuery();
        int currentSlaveId = pipelinePosition.getCurrentPosition().getServerID();

        PseudoGTIDCheckpoint marker = new PseudoGTIDCheckpoint(
                pipelinePosition.getCurrentPosition().getHost(),
                currentSlaveId,
                currentBinlogFileName,
                currentBinlogPosition,
                pseudoGTID,
                pseudoGTIDFullQuery,
                pipelineOrchestrator.getFakeMicrosecondCounter()
        );

        try {
            Coordinator.saveCheckpointMarker(marker);
        } catch (Exception e) {
            LOGGER.error("Failed to save Checkpoint!", e);
            pipelineOrchestrator.requestShutdown();
        }

        if (currentBinlogFileName.equals(lastBinlogFileName)) {
            LOGGER.info("processed the last binlog file " + lastBinlogFileName);
            pipelineOrchestrator.requestShutdown();
        }
    }

    @Override
    public void handle(BinlogEventV4 binlogEventV4) throws TransactionException, TransactionSizeLimitException {
        final RotateEvent event = (RotateEvent) binlogEventV4;
        if (pipelineOrchestrator.isInTransaction()) {
            pipelineOrchestrator.addEventIntoTransaction(event);
        } else {
            pipelineOrchestrator.beginTransaction();
            pipelineOrchestrator.addEventIntoTransaction(event);
            pipelineOrchestrator.commitTransaction(event.getHeader().getTimestamp(), CurrentTransaction.FAKEXID);
        }
    }
}
