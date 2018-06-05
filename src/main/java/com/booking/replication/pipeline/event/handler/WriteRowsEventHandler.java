package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Metrics;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.binlog.event.impl.BinlogEventRows;
import com.booking.replication.binlog.event.IBinlogEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.exception.TableMapException;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by edmitriev on 7/12/17.
 */
public class WriteRowsEventHandler implements RawBinlogEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteRowsEventHandler.class);
    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final Meter counter = Metrics.registry.meter(name("events", "insertEventCounter"));;
    private final PipelineOrchestrator pipelineOrchestrator;


    public WriteRowsEventHandler(EventHandlerConfiguration eventHandlerConfiguration) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }

    @Override
    public void apply(IBinlogEvent binlogEvent, CurrentTransaction currentTransaction) throws TableMapException, ApplierException, IOException {
        final BinlogEventRows event = (BinlogEventRows) binlogEvent;
        AugmentedRowsEvent augmentedRowsEvent =
                eventHandlerConfiguration.getEventAugmenter().mapDataEventToSchema(
                        event,
                        currentTransaction
                );
        eventHandlerConfiguration.getApplier().applyAugmentedRowsEvent(augmentedRowsEvent, currentTransaction);
        counter.mark();
    }

    @Override
    public void handle(IBinlogEvent binlogEvent) throws TransactionException, TransactionSizeLimitException {
        final BinlogEventRows event = (BinlogEventRows) binlogEvent;
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
