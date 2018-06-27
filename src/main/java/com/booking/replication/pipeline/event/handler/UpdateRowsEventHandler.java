package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Metrics;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.applier.kafka.KafkaMessageBufferException;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.binlog.event.impl.BinlogEventRows;
import com.booking.replication.binlog.event.IBinlogEvent;
import com.booking.replication.exceptions.RowListMessageSerializationException;
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
public class UpdateRowsEventHandler implements RawBinlogEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateRowsEventHandler.class);
    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final Meter updateEventCounter = Metrics.registry.meter(name("events", "updateEventCounter"));;
    private final PipelineOrchestrator pipelineOrchestrator;


    public UpdateRowsEventHandler(EventHandlerConfiguration eventHandlerConfiguration) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }

    @Override
    public void apply(IBinlogEvent binlogEvent, CurrentTransaction currentTransaction) throws TableMapException, ApplierException, IOException, RowListMessageSerializationException, KafkaMessageBufferException {
        final BinlogEventRows event = (BinlogEventRows) binlogEvent;
        AugmentedRowsEvent augmentedRowsEvent = eventHandlerConfiguration.getEventAugmenter().mapDataEventToSchema(event, currentTransaction);
        eventHandlerConfiguration.getApplier().applyAugmentedRowsEvent(augmentedRowsEvent, currentTransaction);
        updateEventCounter.mark();
    }

    @Override
    public void handle(IBinlogEvent binlogEvent) throws TransactionException, TransactionSizeLimitException {
        final BinlogEventRows event = (BinlogEventRows) binlogEvent;
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
