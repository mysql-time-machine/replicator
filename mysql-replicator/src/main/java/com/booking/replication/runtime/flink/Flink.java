package com.booking.replication.runtime.flink;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.metrics.Metrics;
import com.booking.replication.controller.WebServer;
import com.booking.replication.flink.sinks.ReplicatorFlinkSink;
import com.booking.replication.flink.sources.binlog.BinlogEventFlinkPartitioner;
import com.booking.replication.flink.sources.binlog.BinlogSource;
import com.booking.replication.runtime.ReplicatorRuntime;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Flink implements ReplicatorRuntime {

    // TODO: adapt/use these props for flink
    public interface Configuration {
        String CHECKPOINT_PATH = "checkpoint.path";
        String CHECKPOINT_DEFAULT = "checkpoint.default";
        String REPLICATOR_THREADS = "replicator.threads";
        String REPLICATOR_TASKS = "replicator.tasks";
        String REPLICATOR_QUEUE_SIZE = "replicator.queue.size";
        String REPLICATOR_QUEUE_TIMEOUT = "replicator.queue.timeout";
        String OVERRIDE_CHECKPOINT_START_POSITION = "override.checkpoint.start.position";
        String OVERRIDE_CHECKPOINT_BINLOG_FILENAME = "override.checkpoint.binLog.filename";
        String OVERRIDE_CHECKPOINT_BINLOG_POSITION = "override.checkpoint.binLog.position";
        String OVERRIDE_CHECKPOINT_GTID_SET = "override.checkpoint.gtidSet";
    }

    private static final Logger LOG = LogManager.getLogger(com.booking.replication.Replicator.class);

    private final Metrics<?> metrics;
    private final String errorCounter;
    private final WebServer webServer;
    private final AtomicLong checkPointDelay;

    private final StreamExecutionEnvironment env;
    private BinlogSource binlogSource;

    private final String METRIC_COORDINATOR_DELAY               = MetricRegistry.name("coordinator", "delay");
    private final String METRIC_STREAM_DESTINATION_QUEUE_SIZE   = MetricRegistry.name("streams", "destination", "queue", "size");
    private final String METRIC_STREAM_SOURCE_QUEUE_SIZE        = MetricRegistry.name("streams", "source", "queue", "size");

    public Flink(final Map<String, Object> configuration) throws IOException {

        this.webServer = WebServer.build(configuration);
        this.metrics = Metrics.build(configuration, webServer.getServer());
        this.errorCounter = MetricRegistry.name(
                String.valueOf(configuration.getOrDefault(Metrics.Configuration.BASE_PATH, "replicator")),
                "error"
        );
        this.checkPointDelay = new AtomicLong(0L);
        this.metrics.register(METRIC_COORDINATOR_DELAY, (Gauge<Long>) () -> this.checkPointDelay.get());


        //////////
        // FLINK:
        env = StreamExecutionEnvironment
                .createLocalEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000).setStateBackend(
                new FsStateBackend(
                        "file:///tmp",
                        false
                )
        );
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        this.binlogSource = new BinlogSource(configuration);

        Partitioner<List<AugmentedEvent>> binlogEventFlinkPartitioner = BinlogEventFlinkPartitioner.build(configuration);

        DataStream<List<AugmentedEvent>> augmentedEventStream = env.addSource(binlogSource).forceNonParallel();

         augmentedEventStream
                .partitionCustom(
                        binlogEventFlinkPartitioner, // <- event to partition, so no need for separate KeySelector
                        event -> event // <- identity key selector
                )
                .addSink(new ReplicatorFlinkSink(configuration))
        ;

    }

    public void start() {
        LOG.info("Execution plan => " + env.getExecutionPlan());
        try {
            env.execute("Replicator");
        } catch (Exception e) {
            Flink.LOG.error("error starting replicator ", e);
        }
    }

    public void stop() {
        try {

            Flink.LOG.info("Stopping Binlog Flink Source");
            if (this.binlogSource != null) {
                this.binlogSource.cancel();
            }

            Flink.LOG.info("stopping web server");
            this.webServer.stop();

            Flink.LOG.info("closing metrics sink");
            this.metrics.close();

        } catch (IOException exception) {
            Flink.LOG.error("error stopping coordinator", exception);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

