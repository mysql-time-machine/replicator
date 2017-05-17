package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Metrics;
import com.booking.replication.binlog.event.RawBinlogEvent;
import com.booking.replication.binlog.event.RawBinlogEventXid;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by edmitriev on 7/12/17.
 */
public class XidEventHandler implements RawBinlogEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(XidEventHandler.class);

    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final Meter counter = Metrics.registry.meter(name("events", "XIDCounter"));;
    private final PipelineOrchestrator pipelineOrchestrator;


    public XidEventHandler(EventHandlerConfiguration eventHandlerConfiguration) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }


    @Override
    public void apply(RawBinlogEvent rawBinlogEvent, CurrentTransaction currentTransaction) throws EventHandlerApplyException {
        if (rawBinlogEvent instanceof  RawBinlogEventXid) {
            final RawBinlogEventXid event = (RawBinlogEventXid) rawBinlogEvent;
            if (currentTransaction.getXid() != event.getXid()) {
                throw new EventHandlerApplyException("Xid of transaction doesn't match the current event xid: " + currentTransaction + ", " + event);
            }
            eventHandlerConfiguration.getApplier().applyXidEvent(event, currentTransaction);
            counter.mark();
        } else {
            throw new EventHandlerApplyException("Expected event of type RawBinlogEventXid, insted got " + rawBinlogEvent.getClass().toString());
        }
    }

    @Override
    public void handle(RawBinlogEvent rawBinlogEvent) throws TransactionException, TransactionSizeLimitException {
        if (rawBinlogEvent instanceof  RawBinlogEventXid) {
            final RawBinlogEventXid event = (RawBinlogEventXid) rawBinlogEvent;
            // prepare trans data
            pipelineOrchestrator.commitTransaction(event);
        } else {
            throw new TransactionException("Expected RawBinlogEventXid, but received " + rawBinlogEvent.getClass().toString());
        }
    }
}
