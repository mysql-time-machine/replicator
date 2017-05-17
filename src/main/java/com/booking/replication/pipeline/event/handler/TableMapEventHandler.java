package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Constants;
import com.booking.replication.Metrics;
import com.booking.replication.binlog.event.RawBinlogEvent;
import com.booking.replication.binlog.event.RawBinlogEventTableMap;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.booking.replication.replicant.ReplicantPool;
import com.booking.replication.schema.exception.TableMapException;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by edmitriev on 7/13/17.
 */
public class TableMapEventHandler implements RawBinlogEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMapEventHandler.class);

    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final Meter heartBeatCounter = Metrics.registry.meter(name("events", "heartBeatCounter"));;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final PipelinePosition pipelinePosition;
    private final ReplicantPool replicantPool;


    public TableMapEventHandler(EventHandlerConfiguration eventHandlerConfiguration,
                                PipelinePosition pipelinePosition, ReplicantPool replicantPool) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelinePosition = pipelinePosition;
        this.replicantPool = replicantPool;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }

    @Override
    public void apply(RawBinlogEvent rawBinlogEvent, CurrentTransaction currentTransaction) throws EventHandlerApplyException, TableMapException {
        final RawBinlogEventTableMap event = (RawBinlogEventTableMap) rawBinlogEvent;
        String tableName = event.getTableName().toString();

        if (tableName.equals(Constants.HEART_BEAT_TABLE)) {
            heartBeatCounter.mark();
        }

        long tableID = event.getTableId();
        String dbName = pipelineOrchestrator.currentTransaction.getDBNameFromTableID(tableID);

        LOGGER.debug("processing events for { db => " + dbName + " table => " + event.getTableName() + " } ");
        LOGGER.debug("fakeMicrosecondCounter at tableMap event => " + pipelineOrchestrator.getFakeMicrosecondCounter());

        eventHandlerConfiguration.getApplier().applyTableMapEvent(event);
    }

    @Override
    public void handle(RawBinlogEvent rawBinlogEvent) throws TransactionException, TransactionSizeLimitException {
        final RawBinlogEventTableMap event = (RawBinlogEventTableMap) rawBinlogEvent;
        pipelineOrchestrator.currentTransaction.updateCache(event);
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
