package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Metrics;
import com.booking.replication.binlog.event.impl.BinlogEventXid;
import com.booking.replication.binlog.event.IBinlogEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.codahale.metrics.Meter;
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
    public void apply(IBinlogEvent binlogEvent, CurrentTransaction currentTransaction) throws EventHandlerApplyException {
        if (binlogEvent instanceof BinlogEventXid) {
            final BinlogEventXid event = (BinlogEventXid) binlogEvent;
            if (currentTransaction.getXid() != event.getXid()) {
                throw new EventHandlerApplyException("Xid of transaction doesn't match the current event xid: " + currentTransaction + ", " + event);
            }
            eventHandlerConfiguration.getApplier().applyXidEvent(event, currentTransaction);
            counter.mark();
        } else {
            throw new EventHandlerApplyException("Expected event of type BinlogEventXid, insted got " + binlogEvent.getClass().toString());
        }
    }

    @Override
    public void handle(IBinlogEvent binlogEvent) throws TransactionException, TransactionSizeLimitException {
        if (binlogEvent instanceof BinlogEventXid) {
            final BinlogEventXid event = (BinlogEventXid) binlogEvent;
            // prepare trans data
            pipelineOrchestrator.commitTransaction(event);
        } else {
            throw new TransactionException("Expected BinlogEventXid, but received " + binlogEvent.getClass().toString());
        }
    }
}
