package com.booking.replication.flink;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Collection;
import java.util.Map;

public class ReplicatorFlinkSink
        extends RichSinkFunction<Collection<AugmentedEvent>>
        implements CheckpointedFunction, CheckpointListener {

    private transient Applier applier;
    private Map<String,Object> configuration;

    private transient Checkpoint binlogCheckpoint = new Checkpoint();
    private transient ListState<Checkpoint> binlogCheckpoints;

    public ReplicatorFlinkSink(Map<String,Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void invoke(Collection<AugmentedEvent> augmentedEvents, Context context) throws Exception {

        System.out.println("applier collection size => " + augmentedEvents.size());

        if (applier == null) {
            System.out.println("Lost applier");
            applier = Applier.build(configuration);
        }
        applier.apply(augmentedEvents);

        this.binlogCheckpoint =
                augmentedEvents
                        .stream()
                        .findFirst()
                        .get()
                        .getHeader()
                        .getCheckpoint();

        if (this.binlogCheckpoint != null && this.binlogCheckpoint.getGtidSet() != null) {
            System.out.println("applier -> " + this.binlogCheckpoint.getGtidSet());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        System.out.println("sink snapshotState, checkpointID => " + functionSnapshotContext.getCheckpointId());

        if (binlogCheckpoint != null) {
            if ( binlogCheckpoint.getGtidSet() != null) {
                System.out.println("BinlogSink: snapshotting state, gtidSet #" + binlogCheckpoint.getGtidSet());
                this.binlogCheckpoints.clear();
                this.binlogCheckpoints.add(binlogCheckpoint);
            }
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        this.applier = Applier.build(configuration);
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        System.out.println("sink checkpointComplete -> " + l);
    }
}
