package com.booking.replication.flink;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.*;

import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import scala.App;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Simple POC sink { Experimental }
 * */
public class ReplicatorFlinkSink
        extends RichSinkFunction<AugmentedEvent>
        implements CheckpointedFunction {

    private Map<String, Object> configuration;

    private transient Applier applier;

    public ReplicatorFlinkSink(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void close() {
    }

    @Override
    public void invoke(AugmentedEvent augmentedEvent) throws Exception {

        // POC hack
        List<AugmentedEvent> events = new ArrayList<>();
        events.add(augmentedEvent);
        applier.apply(events);

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        this.applier = Applier.build(configuration);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        boolean result = applier.forceFlush();
    }
}
