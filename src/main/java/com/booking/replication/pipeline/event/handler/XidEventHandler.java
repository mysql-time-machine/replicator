package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Metrics;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by edmitriev on 7/12/17.
 */
public class XidEventHandler implements BinlogEventV4Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(XidEventHandler.class);

    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final Meter counter = Metrics.registry.meter(name("events", "XIDCounter"));;
    private final PipelineOrchestrator pipelineOrchestrator;


    public XidEventHandler(EventHandlerConfiguration eventHandlerConfiguration) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }


    @Override
    public void apply(BinlogEventV4 binlogEventV4, CurrentTransaction currentTransaction) throws EventHandlerApplyException {
        final XidEvent event = (XidEvent) binlogEventV4;
        if (currentTransaction.getXid() != event.getXid()) {
            throw new EventHandlerApplyException("Xid of transaction doesn't match the current event xid: " + currentTransaction + ", " + event);
        }
        eventHandlerConfiguration.getApplier().applyXidEvent(event, currentTransaction);
        counter.mark();
    }

    @Override
    public void handle(BinlogEventV4 binlogEventV4) throws TransactionException, TransactionSizeLimitException {
        final XidEvent event = (XidEvent) binlogEventV4;
        // prepare trans data
        pipelineOrchestrator.commitTransaction(event);
    }
}
