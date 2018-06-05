package com.booking.replication.pipeline.event.handler;

import com.booking.replication.binlog.event.impl.BinlogEventFormatDescription;
import com.booking.replication.binlog.event.IBinlogEvent;
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
    public void apply(IBinlogEvent binlogEvent, CurrentTransaction currentTransaction) {
        final BinlogEventFormatDescription event = (BinlogEventFormatDescription) binlogEvent;
        eventHandlerConfiguration.getApplier().applyFormatDescriptionEvent(event);
    }

    @Override
    public void handle(IBinlogEvent binlogEvent) throws TransactionException, TransactionSizeLimitException {
        final BinlogEventFormatDescription event = (BinlogEventFormatDescription) binlogEvent;
        if (pipelineOrchestrator.isInTransaction()) {
            pipelineOrchestrator.addEventIntoTransaction(event);
        } else {
            pipelineOrchestrator.beginTransaction();
            pipelineOrchestrator.addEventIntoTransaction(event);
            pipelineOrchestrator.commitTransaction(event.getTimestamp(), CurrentTransaction.FAKEXID);
        }
    }
}
