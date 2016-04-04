package com.booking.replication;

import com.booking.replication.metrics.ReplicatorMetrics;
import com.booking.replication.pipeline.BinlogEventProducer;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.BinlogPositionInfo;
import com.booking.replication.monitor.Overseer;

import com.booking.replication.util.MutableLong;
import com.google.code.or.binlog.BinlogEventV4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Booking replicator. Has two main objects (producer and consumer)
 * that reference the same thread-safe queue.
 * Producer pushes binlog events to the queue and consumer
 * reads them. Producer is basically a wrapper for open replicator,
 * and consumer is wrapper for all booking specific logic (schema
 * version control, augmenting events and storing events).
 */
public class Replicator {

    private final Configuration        configuration;
    private final BinlogEventProducer  binlogEventProducer;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final Overseer             overseer;

    private static final int MAX_QUEUE_SIZE = Constants.MAX_QUEUE_SIZE;

    private final LinkedBlockingQueue<BinlogEventV4> binlogEventQueue =
            new LinkedBlockingQueue<BinlogEventV4>(MAX_QUEUE_SIZE);

    private final ConcurrentHashMap<Integer,Object> lastKnownInfo =
            new ConcurrentHashMap<Integer, Object>();

    //private final  ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>> pipelineStats =
    //        new  ConcurrentHashMap<Integer, HashMap<Integer, MutableLong>>();

    private final ReplicatorMetrics replicatorMetrics;

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    // Replicator()
    public Replicator(Configuration conf) throws SQLException, URISyntaxException, IOException {
        configuration  = conf;

        replicatorMetrics = new ReplicatorMetrics(configuration);

        BinlogPositionInfo lastKnownPosition = new BinlogPositionInfo(
            conf.getStartingBinlogFileName(),
            conf.getStartingBinlogPosition()
        );
        lastKnownInfo.put(Constants.LAST_KNOWN_BINLOG_POSITION, lastKnownPosition);

        binlogEventProducer = new BinlogEventProducer(binlogEventQueue, lastKnownInfo, configuration);
        pipelineOrchestrator = new PipelineOrchestrator(binlogEventQueue, lastKnownInfo, configuration, replicatorMetrics);

        overseer = new Overseer(binlogEventProducer, pipelineOrchestrator, replicatorMetrics ,lastKnownInfo);
    }

    // start()
    public void start() throws Exception {

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOGGER.info("Executing replicator shutdown hook...");
                // Overseer
                try {
                    LOGGER.info("Stopping Overseer...");
                    overseer.stopMonitoring();
                    overseer.join();
                    LOGGER.info("Overseer thread successfully stopped");
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted.", e);
                } catch (Exception e) {
                    LOGGER.error("Failed to stop Overseer", e);
                }
                // Producer
                try {
                    // let open replicator stop its own threads
                    LOGGER.info("Stopping Producer...");
                    binlogEventProducer.stop(1000, TimeUnit.MILLISECONDS);
                    if (!binlogEventProducer.getOr().isRunning()) {
                        LOGGER.info("Successfully stopped Producer thread");
                    }
                    else {
                        throw new Exception("Failed to stop Producer thread");
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted.", e);
                } catch (Exception e) {
                    LOGGER.error("Failed to stop Producer thread", e);
                }
                // Consumer
                try {
                    LOGGER.info("Stopping Pipeline Orchestrator...");
                    pipelineOrchestrator.setRunning(false);
                    pipelineOrchestrator.join();
                    LOGGER.info("Pipeline Orchestrator successfully stopped");
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted.", e);
                } catch (Exception e) {
                    LOGGER.error("Failed to stop Pipeline Orchestrator", e);
                }
            }
        });

        // Start up
        binlogEventProducer.start();
        pipelineOrchestrator.start();
        overseer.start();

        while (!pipelineOrchestrator.isReplicatorShutdownRequested()) {
            Thread.sleep(1000);
        }

        System.exit(0);
    }

    // stop()
    public void stop(long timeout, TimeUnit unit) throws Exception {
        try {
            binlogEventProducer.stop(timeout,unit);
            pipelineOrchestrator.stopRunning();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public BinlogEventProducer getBinlogEventProducer() {
        return binlogEventProducer;
    }
}
