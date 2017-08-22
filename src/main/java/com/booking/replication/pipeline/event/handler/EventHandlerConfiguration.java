package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.EventAugmenter;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.sql.QueryInspector;

/**
 * Created by edmitriev on 7/19/17.
 */
public class EventHandlerConfiguration {
    private Applier applier;
    private EventAugmenter eventAugmenter;
    private PipelineOrchestrator pipelineOrchestrator;

    public EventHandlerConfiguration(Applier applier, EventAugmenter eventAugmenter, PipelineOrchestrator pipelineOrchestrator) {
        this.applier = applier;
        this.eventAugmenter = eventAugmenter;
        this.pipelineOrchestrator = pipelineOrchestrator;
    }

    public Applier getApplier() {
        return applier;
    }

    public EventAugmenter getEventAugmenter() {
        return eventAugmenter;
    }

    public PipelineOrchestrator getPipelineOrchestrator() { return  pipelineOrchestrator; }

}
