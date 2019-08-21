package com.booking.replication.flink.sources.binlog;

import com.booking.replication.applier.Seeker;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Binlog;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.flink.sources.config.BinlogConfiguration;
import com.booking.replication.supplier.Supplier;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class BinlogSource
        extends RichSourceFunction<List<AugmentedEvent>>
        implements CheckpointedFunction, CheckpointListener {

    private static final Logger LOG = LogManager.getLogger(BinlogSource.class);

    private static final boolean USE_FLINK_CHECKPOINT_STORE = true;

    // serializable state
    private volatile boolean isRunning = true;
    private boolean isLeader = false;

    private Map<String, Object> configuration;
    private final String checkpointPath;

    // non-serializable
    private transient BlockingDeque<List<AugmentedEvent>> incomingEvents;
    private transient TreeMap<Long, Tuple2<Boolean, List<Checkpoint>>> gtidsSeenByFlinkCheckpointID;
    private transient List<Checkpoint> gtidsSeen;
    private transient List<Tuple2<Long,Checkpoint>> gtidsConfirmed;

    private transient boolean chaosMonkeySwitch = false;
    private transient ListState<Boolean> chaosMonkeySwitches;

    private transient long            count = 0L;
    private transient ListState<Long> checkpointedCount;

    private transient Checkpoint currentBinlogCheckpoint = new Checkpoint();
    private transient ListState<Checkpoint> binlogCheckpointsConfirmed;

    // augmenter
    private transient Augmenter augmenter;
    // augmenterFilter
    private transient  AugmenterFilter augmenterFilter;
    // supplier
    private transient Supplier supplier;
    // coordinator
    private transient Coordinator coordinator;

    private transient long lastConfirmedFlinkCheckpoint;

    public BinlogSource(Map<String, Object> configuration) throws IOException {

        this.configuration = configuration;

        this.checkpointPath = configuration.get(BinlogConfiguration.CHECKPOINT_PATH).toString();

        this.gtidsSeenByFlinkCheckpointID = new TreeMap<>();
        this.gtidsSeen = new ArrayList<>();
        this.gtidsConfirmed = new ArrayList<>();

        this.lastConfirmedFlinkCheckpoint = 0;
    }

    @Override
    public void open(Configuration parameters) {
        LOG.info("BinlogSource Open.");
    }

    @Override
    public void run(SourceContext<List<AugmentedEvent>> sourceContext) throws Exception {

        while (isRunning) {

            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (sourceContext.getCheckpointLock()) {

                if (isLeader) {

                    List<AugmentedEvent> events = incomingEvents.take();

                    Optional<AugmentedEvent> atLeastOne = events
                            .stream()
                            .filter(event -> event.getHeader().getEventTransaction() != null)
                            .findFirst();

                    if (atLeastOne.isPresent()) {

                        long transactionTimestamp = events
                                .stream()
                                .filter(event -> event.getHeader().getEventTransaction() != null)
                                .findFirst()
                                .get().getHeader().getEventTransaction().getCommitTimestamp();

                        // all events in the list are part of the same transaction
                        // TODO: make this more obvious
                        sourceContext.collectWithTimestamp(
                                events,
                                transactionTimestamp
                        );

                        currentBinlogCheckpoint = events.stream().findFirst().get().getHeader().getCheckpoint();

                        gtidsSeen.add(currentBinlogCheckpoint);

                        if (!gtidsSeenByFlinkCheckpointID.containsKey(lastConfirmedFlinkCheckpoint)) {
                            gtidsSeenByFlinkCheckpointID.put(
                                    lastConfirmedFlinkCheckpoint,
                                    Tuple2.of(false, new ArrayList<>())
                            );
                        }

                        gtidsSeenByFlinkCheckpointID.get(lastConfirmedFlinkCheckpoint).f0 = false;
                        gtidsSeenByFlinkCheckpointID.get(lastConfirmedFlinkCheckpoint).f1.add(
                                new Checkpoint(
                                        currentBinlogCheckpoint.getTimestamp(),
                                        currentBinlogCheckpoint.getServerId(),
                                        currentBinlogCheckpoint.getGtidSet()
                                )
                        );

                        count++;

                        if (chaosMonkeySwitch) {
                            System.out.println("Chaos Monkey is awake!!!");
                            throw new RuntimeException();
                        }
                    } else {
                        System.out.println("Events missing transaction information => ");
                        events.stream().forEach(e -> {
                            try {
                                System.out.println(e.toJSONString());
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                        });
                    }

                } else {
                    System.out.println("not leader, thread " + Thread.currentThread().getId());
                }

            }
            Thread.sleep(1000);
        }
    }


    public void cancel() {

        LOG.info("cancel called, stopping BinlogSource");

        try {
            if (supplier != null) {
                supplier.stop();
            }
            if (augmenter != null) {
                augmenter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (coordinator != null) {
            coordinator.stop();
        }

        isRunning = false;
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {

        System.out.println("Initializing Flink source state");

        this.lastConfirmedFlinkCheckpoint = 0;

        try {

            // augmenter
            this.augmenter = Augmenter.build(configuration);

            // augmenterFilter
            this.augmenterFilter = AugmenterFilter.build(configuration);

            // supplier
            this.supplier = Supplier.build(configuration);

            // coordinator
            this.coordinator = Coordinator.build(configuration);

            this.incomingEvents = new LinkedBlockingDeque<>();

            // helper structures
            this.gtidsSeen = new ArrayList<>();
            this.gtidsConfirmed = new ArrayList<>();
            this.gtidsSeenByFlinkCheckpointID = new TreeMap<>();

            // callbacks
            supplier.onEvent((event) -> {

                synchronized (this) {

                    Collection<AugmentedEvent> augmentedEvents = augmenter.apply(event);

                    if (augmentedEvents != null) { // <- null if event buffered, but commit not reached yet

                        Collection<AugmentedEvent> filteredEvents = augmenterFilter.apply(augmentedEvents);

                        if (filteredEvents != null) {
                            try {
                                incomingEvents.put((List<AugmentedEvent>) filteredEvents);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });

            supplier.onException(e -> {
                e.printStackTrace();
            });

            coordinator.onLeadershipTake(() -> {

                // vars
                boolean overrideCheckpointStartPosition = Boolean.parseBoolean(configuration.getOrDefault(BinlogConfiguration.OVERRIDE_CHECKPOINT_START_POSITION, false).toString());
                String overrideCheckpointBinLogFileName = configuration.getOrDefault(BinlogConfiguration.OVERRIDE_CHECKPOINT_BINLOG_FILENAME, "").toString();
                long overrideCheckpointBinlogPosition = Long.parseLong(configuration.getOrDefault(BinlogConfiguration.OVERRIDE_CHECKPOINT_BINLOG_POSITION, "0").toString());
                String overrideCheckpointGtidSet = configuration.getOrDefault(BinlogConfiguration.OVERRIDE_CHECKPOINT_GTID_SET, "").toString();

                try {

                    LOG.info("Acquired leadership. Loading checkpoint.");

                    restoreState(context); // will set binlogCheckpoint

                    if (!USE_FLINK_CHECKPOINT_STORE) {
                        currentBinlogCheckpoint = getCheckpoint(
                                overrideCheckpointStartPosition,
                                overrideCheckpointBinLogFileName,
                                overrideCheckpointBinlogPosition,
                                overrideCheckpointGtidSet,
                                coordinator
                        );
                    }

                    LOG.info("Loaded checkpoint: "+ currentBinlogCheckpoint.getGtidSet() + " Starting supplier.");

                    synchronized (this) {
                        if (!isLeader) {
                            isLeader = true;
                            LOG.info("isLeader = true");
                        } else {
                            LOG.info("Re-acquired leadership");
                        }
                    }

                    // TODO: make more explicit visible the case when gtidPurgedFallback mode
                    //       is used, since in that case any checkpoint we pass to start() is bypassed
                    supplier.start(currentBinlogCheckpoint);

                    LOG.info("Supplier started.");


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            coordinator.onLeadershipLose(() -> {

                try {

                    synchronized (this) {
                        isLeader = false;
                    }

                    LOG.info("Stopping supplier");

                    supplier.stop();

                    LOG.info("Supplier stopped");

                    LOG.info("closing augmenter");

                    augmenter.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            LOG.info("starting coordinator");

            coordinator.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void restoreState(FunctionInitializationContext context) throws Exception {

        // restore state
        this.chaosMonkeySwitches = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<Boolean>("chaosMonkeySwitchState", Boolean.class));
        if (context.isRestored()) {
            for (Boolean chaosMonkeySwitchRestored : this.chaosMonkeySwitches.get()) {
                if (chaosMonkeySwitchRestored != null) {
                    this.chaosMonkeySwitch = chaosMonkeySwitchRestored;
                    System.out.println("restored context, chaosMonkeySwitch => #" + chaosMonkeySwitchRestored);
                }
            }
        }

        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("count", Long.class));
        if (context.isRestored()) {
            for (Long countRestored : this.checkpointedCount.get()) {
                System.out.println("Recovered from failure, restored context, count => #" + countRestored);
                this.count = countRestored;
            }
        }

        this.binlogCheckpointsConfirmed = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("binlogCheckpointsConfirmed", Checkpoint.class));

        context.getOperatorStateStore().getRegisteredStateNames().stream().forEach( name -> {
            LOG.info("BinlogSource restoreState: found registered state name => " + name);
        });

        if (context.isRestored()) {
            // TODO: implement gtid merge logic for case of gaps; taking the last one is just POC that assumes no gaps.
            for (Checkpoint checkpoint : this.binlogCheckpointsConfirmed.get()) {
                if (checkpoint != null && checkpoint.getGtidSet() != null) {
                    this.currentBinlogCheckpoint = checkpoint;
                    LOG.info("restored context in binlogSource, checkpoint => #" + checkpoint.getGtidSet());
                }
            }
        }

        if (this.currentBinlogCheckpoint == null) {
            Object checkpointDefault = configuration.get(BinlogConfiguration.CHECKPOINT_DEFAULT);
            String checkpointDefaultString = (checkpointDefault != null) ? (checkpointDefault.toString()) : (null);
            if (checkpointDefaultString != null) {
                this.currentBinlogCheckpoint = new ObjectMapper().readValue(checkpointDefaultString, Checkpoint.class);
            }
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        lastConfirmedFlinkCheckpoint = context.getCheckpointId();

        if (isLeader) {

            this.checkpointedCount.clear();
            this.checkpointedCount.add(count);

            // TODO: ugly hack to make this die only once.
            //       Make chaos conditions configurable, with random % as default policy
            if (count == 3 && !chaosMonkeySwitch) {
                System.out.println("BinlogSourceOnSnapshot: chaos condition true -> activate chaos monkey");
                chaosMonkeySwitch = true;
            } else {
                chaosMonkeySwitch = false;
            }

            System.out.println("BinlogSourceOnSnapshot: chaosMonkeySwitch => #" + chaosMonkeySwitch);
            this.chaosMonkeySwitches.clear();
            this.chaosMonkeySwitches.add(chaosMonkeySwitch);

            this.binlogCheckpointsConfirmed.clear();
            this.binlogCheckpointsConfirmed.addAll(gtidsConfirmed.stream().map(t -> t.f1).collect(Collectors.toList()));

            gtidsConfirmed.stream().forEach(x -> System.out.println("BinlogSourceOnSnapshot: { checkpointId => " +
                    x.f0 +
                    ", gtidSet => " +
                    x.f1.getGtidSet() +
                    " }"
            ));

            // mark snapshotted for cleanup - will need another helper to track the checkpointIDs
            List<Long> checkpointIDsSnapshotted = gtidsConfirmed.stream().map(t -> t.f0).collect(Collectors.toList());
            checkpointIDsSnapshotted.stream().forEach(
                    checkpointId -> {
                        System.out.println("BinlogSourceOnSnapshot: marking for cleanup: { checkpointId => " +
                                checkpointId +
                                ", gtidSet => " +
                                gtidsSeenByFlinkCheckpointID
                                        .get(checkpointId)
                                        .f1
                                        .stream()
                                        .map(c -> c.getGtidSet().toString())
                                        .collect(Collectors.toList()).stream().collect(Collectors.joining(",")) +
                                " }"
                        );
                        gtidsSeenByFlinkCheckpointID.get(checkpointId).f0 = true;
                    }
            );

        }
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {

        System.out.println("BinlogSourceOnConfirmed: checkpoint complete => " + l);

        System.out.println("BinlogSourceOnConfirmed: Seen ");
        gtidsSeen.stream().forEach(x -> System.out.println("\t " + x.getGtidSet()));

        gtidsConfirmed.clear();

        System.out.println("BinlogSourceOnConfirmed: Seen by previously confirmed checkpointIDs");
        gtidsSeenByFlinkCheckpointID.keySet()
                .stream()
                .filter(k -> k < l)
                .filter(k -> gtidsSeenByFlinkCheckpointID.get(k).f0 == false) // not snapshotted yet
                .forEach(
                        flinkCheckpointId -> {
                            gtidsSeenByFlinkCheckpointID.get(flinkCheckpointId).f1.forEach(
                                binlogCheckpoint -> {
                                    System.out.println("\tflinkCheckpointID => " + flinkCheckpointId + ", gtidSet => " + binlogCheckpoint.getGtidSet() + " }");
                                    gtidsConfirmed.add(
                                            Tuple2.of(
                                                    flinkCheckpointId,
                                                    new Checkpoint(
                                                        binlogCheckpoint.getTimestamp(),
                                                        binlogCheckpoint.getServerId(),
                                                        binlogCheckpoint.getGtidSet()
                                                    )
                                            )
                                    );
                                }
                       );
                }
        );

        this.lastConfirmedFlinkCheckpoint = l;

        // cleanup of keys in gtidsSeenByFlinkCheckpointID that have already been snapshotted
        List<Long> cleanupKeys = gtidsSeenByFlinkCheckpointID.keySet()
                .stream()
                .filter(k -> k < l)
                .filter(k -> gtidsSeenByFlinkCheckpointID.get(k).f0 == true)
                .collect(Collectors.toList());

        cleanupKeys.forEach(
                key -> {
                    gtidsSeenByFlinkCheckpointID.get(key).f1.forEach(gtid -> System.out.println("BinlogSourceOnConfirmed: mark for cleanup =>" + key + ", "+ gtid.getGtidSet()));
                    gtidsSeenByFlinkCheckpointID.remove(key);
                }
        );

    }


    private Checkpoint getCheckpoint(
            boolean overrideCheckpointStartPosition,
            String overrideCheckpointBinLogFileName,
            long overrideCheckpointBinlogPosition,
            String overrideCheckpointGtidSet,
            Coordinator coordinator) throws IOException {

        Checkpoint from;

        Seeker seeker = Seeker.build(configuration);

        if(overrideCheckpointStartPosition){

            if (overrideCheckpointBinLogFileName != null && !overrideCheckpointBinLogFileName.equals("")) {

                LOG.info("Checkpoint startup mode: override Binlog filename and position:" +
                        overrideCheckpointBinLogFileName +
                        ":" +
                        overrideCheckpointBinlogPosition);

                from = seeker.seek(
                        new Checkpoint(new Binlog(overrideCheckpointBinLogFileName, overrideCheckpointBinlogPosition))
                );

            } else if (overrideCheckpointGtidSet != null && !overrideCheckpointGtidSet.equals("")) {

                LOG.info("Checkpoint startup mode: override gtidSet: " + overrideCheckpointGtidSet);
                from = seeker.seek(
                        new Checkpoint(overrideCheckpointGtidSet)
                );

            } else {
                throw new RuntimeException("Impossible case!");
            }

        } else {
            LOG.info("Checkpoint startup mode: loading safe checkpoint from checkpoint store");
            from = seeker.seek(
                    this.loadSafeCheckpoint(coordinator)
            );
        }

        seeker.close();

        return from;
    }

    private Checkpoint loadSafeCheckpoint(Coordinator coordinator) throws IOException {

        Checkpoint checkpoint = coordinator.loadCheckpoint(this.checkpointPath);

        Object checkpointDefault = configuration.get(BinlogConfiguration.CHECKPOINT_DEFAULT);

        String checkpointDefaultString = (checkpointDefault != null) ? (checkpointDefault.toString()) : (null);

        if (checkpoint == null && checkpointDefaultString != null) {
            checkpoint = new ObjectMapper().readValue(checkpointDefaultString, Checkpoint.class);
        }

        return checkpoint;
    }

    public synchronized boolean isRunning() {
        return isRunning;
    }

}
