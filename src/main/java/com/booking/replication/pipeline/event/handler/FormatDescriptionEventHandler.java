package com.booking.replication.pipeline.event.handler;

import com.booking.replication.binlog.event.RawBinlogEvent;
import com.booking.replication.binlog.event.RawBinlogEventFormatDescription;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by edmitriev on 7/12/17.
 */
public class FormatDescriptionEventHandler implements RawBinlogEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FormatDescriptionEventHandler.class);

    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final PipelineOrchestrator pipelineOrchestrator;

    public FormatDescriptionEventHandler(EventHandlerConfiguration eventHandlerConfiguration) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }


    @Override
    public void apply(RawBinlogEvent rawBinlogEvent, CurrentTransaction currentTransaction) {
        final RawBinlogEventFormatDescription event = (RawBinlogEventFormatDescription) rawBinlogEvent;
        eventHandlerConfiguration.getApplier().applyFormatDescriptionEvent(event);
    }

    @Override
    public void handle(RawBinlogEvent rawBinlogEvent) throws TransactionException, TransactionSizeLimitException {
        final RawBinlogEventFormatDescription event = (RawBinlogEventFormatDescription) rawBinlogEvent;
        if (pipelineOrchestrator.isInTransaction()) {
            pipelineOrchestrator.addEventIntoTransaction(event);
        } else {
            pipelineOrchestrator.beginTransaction();
            pipelineOrchestrator.addEventIntoTransaction(event);
            pipelineOrchestrator.commitTransaction(event.getTimestamp(), CurrentTransaction.FAKEXID);
        }
    }
}
