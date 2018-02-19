package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Coordinator;
import com.booking.replication.Metrics;
import com.booking.replication.applier.*;
import com.booking.replication.applier.hbase.TaskBufferInconsistencyException;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.EventPosition;
import com.booking.replication.binlog.event.QueryEventType;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;
import com.booking.replication.pipeline.BinlogEventProducerException;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.applier.SupportedAppliers.ApplierName;
import com.booking.replication.sql.QueryInspector;
import com.booking.replication.sql.exception.QueryInspectorException;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.QueryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by edmitriev on 7/12/17.
 */
public class QueryEventHandler implements BinlogEventV4Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryEventHandler.class);

    private final ActiveSchemaVersion activeSchemaVersion;
    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final Meter pgtidCounter = Metrics.registry.meter(name("events", "pgtidCounter"));;
    private final Meter commitQueryCounter = Metrics.registry.meter(name("events", "commitQueryCounter"));;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final PipelinePosition pipelinePosition;


    public QueryEventHandler(EventHandlerConfiguration eventHandlerConfiguration, ActiveSchemaVersion activeSchemaVersion,
                             PipelinePosition pipelinePosition) {
        this.activeSchemaVersion = activeSchemaVersion;
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelinePosition = pipelinePosition;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }

    @Override
    public void apply(BinlogEventV4 binlogEventV4, CurrentTransaction currentTransaction) throws EventHandlerApplyException, ApplierException, IOException {
        final QueryEvent event = (QueryEvent) binlogEventV4;
        String querySQL = event.getSql().toString();
        QueryEventType queryEventType = QueryInspector.getQueryEventType(event);
        LOGGER.debug("Applying event: " + event + ", type: " + queryEventType);

        switch (queryEventType) {
            case COMMIT:
                commitQueryCounter.mark();
                eventHandlerConfiguration.getApplier().applyCommitQueryEvent(event, currentTransaction);
                break;
            case BEGIN:
                eventHandlerConfiguration.getApplier().applyBeginQueryEvent(event, currentTransaction);
                break;
            case DDLTABLE:
                // Sync all the things here.
                eventHandlerConfiguration.getApplier().forceFlush();
                eventHandlerConfiguration.getApplier().waitUntilAllRowsAreCommitted(event);

                try {
                    AugmentedSchemaChangeEvent augmentedSchemaChangeEvent = activeSchemaVersion.transitionSchemaToNextVersion(
                            eventHandlerConfiguration.getEventAugmenter().getSchemaTransitionSequence(event),
                            event.getHeader().getTimestamp()
                    );

                    String pseudoGTID = pipelinePosition.getCurrentPseudoGTID();
                    String pseudoGTIDFullQuery = pipelinePosition.getCurrentPseudoGTIDFullQuery();
                    int currentSlaveId = pipelinePosition.getCurrentPosition().getServerID();

                    PseudoGTIDCheckpoint marker = new PseudoGTIDCheckpoint(
                            pipelinePosition.getCurrentPosition().getHost(),
                            currentSlaveId,
                            EventPosition.getEventBinlogFileName(event),
                            EventPosition.getEventBinlogPosition(event),
                            pseudoGTID,
                            pseudoGTIDFullQuery,
                            pipelineOrchestrator.getFakeMicrosecondCounter()
                    );

                    LOGGER.info("Save new marker: " + marker.toJson());
                    Coordinator.saveCheckpointMarker(marker);
                    eventHandlerConfiguration.getApplier().applyAugmentedSchemaChangeEvent(augmentedSchemaChangeEvent, pipelineOrchestrator);
                } catch (SchemaTransitionException e) {
                    LOGGER.error("Failed to apply query", e);
                    throw new EventHandlerApplyException("Failed to apply event", e);
                } catch (Exception e) {
                    throw new EventHandlerApplyException("Failed to apply event", e);
                }
                break;
            case PSEUDOGTID:
                pgtidCounter.mark();
                try {
                    String pseudoGTID = QueryInspector.extractPseudoGTID(querySQL);

                    LOGGER.debug("PGTID: " + pseudoGTID);

                    // THIS IS EXECUTED

                    pipelinePosition.setCurrentPseudoGTID(pseudoGTID);
                    pipelinePosition.setCurrentPseudoGTIDFullQuery(querySQL);

                    LOGGER.debug("applier type: " + eventHandlerConfiguration.getApplier().toString());

                    // All appliers are wrapped into EventCountingApplier so we need to check which one
                    // we have.
                    // TODO: Do we need a wrapper class just for the event counting?
                    // TODO: Make the codebase more consistent in terms of inheritance vs composition.

                    if (eventHandlerConfiguration.getApplier().getApplierName() == ApplierName.HBaseApplier) {

                        try {
                            ((HBaseApplier) ((EventCountingApplier) eventHandlerConfiguration.getApplier()).getWrapped()).applyPseudoGTIDEvent(
                                    new PseudoGTIDCheckpoint(
                                        pipelinePosition.getCurrentPosition().getHost(),
                                        pipelinePosition.getCurrentPosition().getServerID(),
                                        pipelinePosition.getCurrentPosition().getBinlogFilename(),
                                        pipelinePosition.getCurrentPosition().getBinlogPosition(),
                                        pseudoGTID,
                                        querySQL,
                                        pipelineOrchestrator.getFakeMicrosecondCounter()
                                )
                            );
                        } catch (TaskBufferInconsistencyException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (QueryInspectorException e) {
                    LOGGER.error("Failed to update pipelinePosition with new pGTID!", e);
                    throw new EventHandlerApplyException("Failed to apply event", e);
                }
                break;
            case ANALYZE:
            case DDLDEFINER:
            case DDLTEMPORARYTABLE:
            case DDLVIEW:
                // TODO: add view schema changes to view schema history
                LOGGER.debug("Dropping an event of type: " + queryEventType);
                break;
            default:
                LOGGER.warn("Unexpected query event: " + querySQL);
                break;
        }
    }

    @Override
    public void handle(BinlogEventV4 binlogEventV4) throws TransactionException, BinlogEventProducerException, TransactionSizeLimitException {
        final QueryEvent event = (QueryEvent) binlogEventV4;
        QueryEventType queryEventType = QueryInspector.getQueryEventType(event);
        switch (queryEventType) {
            case COMMIT:
                pipelineOrchestrator.commitTransaction(event);
                break;
            case BEGIN:
                if (!pipelineOrchestrator.beginTransaction(event)) {
                    throw new TransactionException("Failed to begin new transaction. Already have one: " + pipelineOrchestrator.getCurrentTransaction());
                }
                break;
            case DDLTEMPORARYTABLE:
            case DDLTABLE:
            case DDLVIEW:
                if (pipelineOrchestrator.isInTransaction()) {
                    pipelineOrchestrator.addEventIntoTransaction(event);
                } else {
                    pipelineOrchestrator.beginTransaction();
                    pipelineOrchestrator.addEventIntoTransaction(event);
                    pipelineOrchestrator.commitTransaction(event.getHeader().getTimestamp(), CurrentTransaction.FAKEXID);
                }
                break;
            case PSEUDOGTID:
                // apply event right away through a fake transaction
                if (!pipelineOrchestrator.beginTransaction()) {
                    throw new TransactionException("Failed to begin new transaction. Already have one: " + pipelineOrchestrator.getCurrentTransaction());
                }
                pipelineOrchestrator.addEventIntoTransaction(event);
                pipelineOrchestrator.commitTransaction(event.getHeader().getTimestamp(), CurrentTransaction.FAKEXID);
                break;
            case ANALYZE:
            case DDLDEFINER:
                LOGGER.debug("Dropping an event of type: " + queryEventType);
                break;
            default:
                LOGGER.warn("Unexpected query event: " + event.getSql());
                if (pipelineOrchestrator.isInTransaction()) {
                    pipelineOrchestrator.addEventIntoTransaction(event);
                } else {
                    pipelineOrchestrator.beginTransaction();
                    pipelineOrchestrator.addEventIntoTransaction(event);
                    pipelineOrchestrator.commitTransaction(event.getHeader().getTimestamp(), CurrentTransaction.FAKEXID);
                }
        }
    }
}
