package com.booking.replication.runtime.flink;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.metrics.Metrics;
import com.booking.replication.controller.WebServer;
import com.booking.replication.flink.BinlogEventFlinkPartitioner;
import com.booking.replication.flink.BinlogSource;
import com.booking.replication.flink.ReplicatorGenericFlinkDummySink;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicatorFlinkApplication {

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
    private static final String COMMAND_LINE_SYNTAX = "java -jar mysql-replicator-<version>.jar";

    private final String checkpointDefault;
    private final Metrics<?> metrics;
    private final String errorCounter;
    private final WebServer webServer;
    private final AtomicLong checkPointDelay;

    private final StreamExecutionEnvironment env;
    private BinlogSource source;
    private ReplicatorGenericFlinkDummySink sink;

    private final String METRIC_COORDINATOR_DELAY               = MetricRegistry.name("coordinator", "delay");
    private final String METRIC_STREAM_DESTINATION_QUEUE_SIZE   = MetricRegistry.name("streams", "destination", "queue", "size");
    private final String METRIC_STREAM_SOURCE_QUEUE_SIZE        = MetricRegistry.name("streams", "source", "queue", "size");

    public ReplicatorFlinkApplication(final Map<String, Object> configuration) throws IOException {

        Object checkpointPath = configuration.get(ReplicatorFlinkApplication.Configuration.CHECKPOINT_PATH);
        Object checkpointDefault = configuration.get(ReplicatorFlinkApplication.Configuration.CHECKPOINT_DEFAULT);

        Objects.requireNonNull(checkpointPath, String.format("Configuration required: %s", ReplicatorFlinkApplication.Configuration.CHECKPOINT_PATH));

        this.checkpointDefault = (checkpointDefault != null) ? (checkpointDefault.toString()) : (null);

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


        this.source = new BinlogSource(configuration);

        DataStream<AugmentedEvent> augmentedEventDataStream =
                env.addSource(source)
                        .forceNonParallel();

        Partitioner<AugmentedEvent> binlogEventFlinkPartitioner =
                BinlogEventFlinkPartitioner.build(configuration);

        DataStream<AugmentedEvent> partitionedDataStream =
                  augmentedEventDataStream
                           .partitionCustom(
                                binlogEventFlinkPartitioner,
                                // binlogEventPartitioner knows how to convert event to partition,
                                // so there is no need for a separate KeySelector
                                event -> event // <- identity key selector
                        );

        // TODO: switch to FlinkKafkaProducer
        // ReplicatorKafkaSink s = new ReplicatorKafkaSink(configuration);
        RichSinkFunction<AugmentedEvent> s = new ReplicatorGenericFlinkDummySink(configuration);

        augmentedEventDataStream.addSink(s);
    }

    public void start() throws Exception {
        LOG.info("Execution plan => " + env.getExecutionPlan());
        env.execute("Replicator");
    }

    public void stop() {
        try {

            ReplicatorFlinkApplication.LOG.info("Stopping Binlog Flink Source");
            if (this.source != null) {
                this.source.cancel();
            }

            ReplicatorFlinkApplication.LOG.info("closing sink");
            if (this.sink != null) {
                this.sink.close();
            }

            ReplicatorFlinkApplication.LOG.info("stopping web server");
            this.webServer.stop();

            ReplicatorFlinkApplication.LOG.info("closing metrics sink");
            this.metrics.close();

        } catch (IOException exception) {
            ReplicatorFlinkApplication.LOG.error("error stopping coordinator", exception);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

