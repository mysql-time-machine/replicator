package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.ApplierException;
import com.booking.replication.binlog.event.impl.BinlogEventRotate;
import com.booking.replication.binlog.event.IBinlogEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class RotateEventHandler implements RawBinlogEventHandler {
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
    public void apply(IBinlogEvent binlogEvent, CurrentTransaction currentTransaction) throws EventHandlerApplyException, ApplierException, IOException {
        final BinlogEventRotate event = (BinlogEventRotate) binlogEvent;
        try {
            eventHandlerConfiguration.getApplier().applyRotateEvent(event);
        } catch (IOException e) {
            throw new EventHandlerApplyException("Failed to apply event", e);
        }
        LOGGER.info("End of binlog file. Waiting for all tasks to finish before moving forward...");

        eventHandlerConfiguration.getApplier().waitUntilAllRowsAreCommitted();

        String currentBinlogFileName = pipelinePosition.getCurrentPosition().getBinlogFilename();
        long currentBinlogPosition = pipelinePosition.getCurrentPosition().getBinlogPosition();

        // binlog begins on position 4
        if (currentBinlogPosition <= 0L) currentBinlogPosition = 4;

        String nextBinlogFileName = event.getBinlogFileName().toString();

        LOGGER.info("Rotate Event: moving to the processing of the next binlog file" + nextBinlogFileName);

        if (currentBinlogFileName.equals(lastBinlogFileName)) {
            LOGGER.info("Processed the last binlog file " + lastBinlogFileName);
            pipelineOrchestrator.requestShutdown();
        }
    }

    @Override
    public void handle(IBinlogEvent binlogEvent) throws TransactionException, TransactionSizeLimitException {
        final BinlogEventRotate event = (BinlogEventRotate) binlogEvent;
        if (pipelineOrchestrator.isInTransaction()) {
            pipelineOrchestrator.addEventIntoTransaction(event);
        } else {
            pipelineOrchestrator.beginTransaction();
            pipelineOrchestrator.addEventIntoTransaction(event);
            pipelineOrchestrator.commitTransaction(event.getTimestamp(), CurrentTransaction.FAKEXID);
        }
    }
}
