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

public class ReplicatorGenericFlinkDummySink
        extends RichSinkFunction<AugmentedEvent>
        implements CheckpointedFunction {

    private Map<String, Object> configuration;

    private transient Applier applier;
    private transient Checkpoint binlogCheckpoint;

    public ReplicatorGenericFlinkDummySink(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void close() {
        System.out.println("SINK CLOSE _____!!!!");
    }

    @Override
    public void invoke(AugmentedEvent augmentedEvent) throws Exception {

        // ugly poc hack
        List<AugmentedEvent> e = new ArrayList<>();
        e.add(augmentedEvent);

        if (applier == null) {
            System.out.println("Lost applier");
            applier = Applier.build(configuration);
        }

        applier.apply(e);

        Checkpoint committedCheckpoint =
                augmentedEvent.getHeader().getCheckpoint();

        this.binlogCheckpoint = committedCheckpoint;

    }



    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        this.applier = Applier.build(configuration);
        this.binlogCheckpoint = new Checkpoint();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if ( binlogCheckpoint != null) {
            if (binlogCheckpoint.getGtidSet() != null) {
                System.out.println("BinlogSink: snapshotting state, gtidSet #" + binlogCheckpoint.getGtidSet());
            }
        }
    }

}
