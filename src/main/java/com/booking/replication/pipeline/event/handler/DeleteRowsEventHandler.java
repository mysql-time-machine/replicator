package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Metrics;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.exception.TableMapException;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by edmitriev on 7/12/17.
 */
public class DeleteRowsEventHandler implements BinlogEventV4Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteRowsEventHandler.class);
    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final Meter counter = Metrics.registry.meter(name("events", "deleteEventCounter"));;
    private final PipelineOrchestrator pipelineOrchestrator;


    public DeleteRowsEventHandler(EventHandlerConfiguration eventHandlerConfiguration) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }

    @Override
    public void apply(BinlogEventV4 binlogEventV4, CurrentTransaction currentTransaction)
            throws EventHandlerApplyException, TableMapException, ApplierException, IOException {

        final AbstractRowEvent event = (AbstractRowEvent) binlogEventV4;

        AugmentedRowsEvent augmentedRowsEvent =
                eventHandlerConfiguration
                    .getEventAugmenter()
                    .mapDataEventToSchema(event, currentTransaction)
                    .removeRowsWithoutPrimaryKey();

        if (!augmentedRowsEvent.getSingleRowEvents().isEmpty()) {
            eventHandlerConfiguration.getApplier().applyAugmentedRowsEvent(augmentedRowsEvent, currentTransaction);
            counter.mark();
        }
    }

    @Override
    public void handle(BinlogEventV4 binlogEventV4) throws TransactionException, TransactionSizeLimitException {
        final AbstractRowEvent event = (AbstractRowEvent) binlogEventV4;
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
