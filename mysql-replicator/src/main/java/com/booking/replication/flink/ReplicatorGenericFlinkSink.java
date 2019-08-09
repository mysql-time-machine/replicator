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

import java.util.Collection;
import java.util.Map;

import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import scala.App;

import java.util.Collection;
import java.util.Map;

public class ReplicatorGenericFlinkSink
        extends RichSinkFunction<Collection<AugmentedEvent>>
        implements CheckpointedFunction {

        private transient Applier applier;
        private transient Map<String, Object> configuration;

        private transient Checkpoint binlogCheckpoint = new Checkpoint();

        private transient ListState<Checkpoint> binlogCheckpoints;

        public ReplicatorGenericFlinkSink(Map<String, Object> configuration) {
            System.out.println("Created applier");
            this.applier = Applier.build(configuration);
            this.configuration = configuration;
        }

        @Override
        public void invoke(Collection<AugmentedEvent> augmentedEvents) throws Exception {
            if (applier == null) {
                System.out.println("Lost applier");
                applier = Applier.build(configuration);
            }
            applier.apply(augmentedEvents);

            Checkpoint committedCheckpoint =
                    augmentedEvents.stream().findFirst().get().getHeader().getCheckpoint();

            this.binlogCheckpoint = committedCheckpoint;

            // TODO: move to snapshotState
            if ( committedCheckpoint.getGtidSet() != null) {
                System.out.println("BinlogSink: snapshotting state, gtidSet #" + committedCheckpoint.getGtidSet());
                this.binlogCheckpoints.clear();
                this.binlogCheckpoints.add(committedCheckpoint);
            }

        }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
