package com.booking.replication.pipeline;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.booking.replication.Metrics;
import com.booking.replication.replicant.ReplicantPool;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventV4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Simple wrapper for Open Replicator. Writes events to blocking queue.
 */
public class BinlogEventProducer {

    // queue is private, but it will reference the same queue
    // as the consumer object
    private final BlockingQueue<BinlogEventV4> queue;

    private final PipelinePosition pipelinePosition;

    private final OpenReplicator   openReplicator;
    private final Configuration    configuration;
    private final ReplicantPool replicantPool;

    private final int serverId = (new Random().nextInt() >>> 1) | (1 << 30); // a large positive random integer;

    private long opCounter = 0;
    private long backPressureSleep = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogEventProducer.class);

    private static final Meter producedEvents = Metrics.registry.meter(name("events", "eventsProduced"));

    /**
     * Set up and manage the Open Replicator instance.
     *
     * @param queue             Event queue.
     * @param pipelinePosition  Binlog position information
     * @param configuration     Replicator configuration
     * @param replicantPool     ReplicantPool
     */
    public BinlogEventProducer(
            BlockingQueue<BinlogEventV4> queue,
            PipelinePosition pipelinePosition,
            Configuration configuration,
            ReplicantPool replicantPool) {
        this.configuration = configuration;
        this.queue = queue;
        this.pipelinePosition = pipelinePosition;
        this.replicantPool = replicantPool;

        Metrics.registry.register(name("events", "producerBackPressureSleep"),
                (Gauge<Long>) () -> backPressureSleep);

        openReplicator = initOpenReplicator();
        setBinlogPosition(pipelinePosition.getCurrentPosition().getBinlogPosition());
        setBinlogFileName(pipelinePosition.getCurrentPosition().getBinlogFilename());

    }

    private OpenReplicator initOpenReplicator() {
        OpenReplicator openReplicator = new OpenReplicator();
        // config
        openReplicator.setUser(configuration.getReplicantDBUserName());
        openReplicator.setPassword(configuration.getReplicantDBPassword());
        openReplicator.setPort(configuration.getReplicantPort());

        // pool
        openReplicator.setHost(pipelinePosition.getCurrentReplicantHostName());
        openReplicator.setServerId(serverId);

        // disable lv2 buffer
        openReplicator.setLevel2BufferSize(-1);

        openReplicator.setBinlogEventListener(event -> {
            producedEvents.mark();

            // This call is blocking the writes from server side. If time goes above
            // net_write_timeout (which defaults to 60s) server will drop connection.
            //      => Use back pressure to regulate the write rate to the queue.

            if (isRunning()) {
                boolean eventQueued = false;
                while (!eventQueued) { // blocking block
                    try {
                        backPressureSleep();
                        boolean added = queue.offer(event, 100, TimeUnit.MILLISECONDS);

                        if (added) {
                            opCounter++;
                            eventQueued = true;
                            if (opCounter % 1000 == 0) {
                                LOGGER.debug("Producer reporting queue size => " + queue.size());
                            }
                        } else {
                            LOGGER.error("queue.offer timed out. Will sleep for 100ms and try again");
                            Thread.sleep(100);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        return openReplicator;
    }


    public void setBinlogFileName(String binlogFileName) {
        LOGGER.info("Changing binlog filename from: " + openReplicator.getBinlogFileName() + " to: " + binlogFileName);
        openReplicator.setBinlogFileName(binlogFileName);
    }

    public void setBinlogPosition(long binlogPosition) {
        LOGGER.info("Changing binlog position from: " + openReplicator.getBinlogPosition() + " to: " + binlogPosition);
        openReplicator.setBinlogPosition(binlogPosition);
    }

    /**
     * Start.
     * @throws Exception openReplicator Exception
     */
    public void start() throws Exception {
        LOGGER.info("Starting producer from: { binlog-file => " + openReplicator.getBinlogFileName()
                + ", position => " + openReplicator.getBinlogPosition() + " }");
        openReplicator.start();
    }

    public void stop(long timeout, TimeUnit unit) throws Exception {
        LOGGER.info("Stopping producer. Start point was: { binlog-file => " + openReplicator.getBinlogFileName()
                + ", position => " + openReplicator.getBinlogPosition() + " }");
        openReplicator.stopQuietly(timeout, unit);
    }

    public void clearQueue() {
        LOGGER.debug("Clearing queue");
        queue.clear();
    }

    public void stopAndClearQueue(long timeout, TimeUnit unit) throws Exception {
        stop(timeout, unit);
        clearQueue();
    }

    public boolean isRunning() {
        return this.openReplicator.isRunning();
    }

    private void backPressureSleep() {
        int queueSize = queue.size();

        // For an explanation please plug "max(0, 20*(10000/(10000+1-x)-10)) x from 6000 to 10000" into WolframAlpha
        backPressureSleep = Math.max(
            20 * ( Constants.MAX_RAW_QUEUE_SIZE / (Constants.MAX_RAW_QUEUE_SIZE + 1 - queueSize) - 10),
            0
        );

        // Queue size is 9243.(42 period) (where MAX_RAW_QUEUE_SIZE = 10000)
        if (backPressureSleep < 64) {
            return;
        }

        // Queue size is 9953.(380952 period)
        if (backPressureSleep > 4000) {
            LOGGER.warn("Queue is getting big, back pressure is getting high");
        }

        try {
            Thread.sleep(backPressureSleep);
        } catch (InterruptedException e) {
            LOGGER.error("Thread wont sleep");
            e.printStackTrace();
        }
    }

    public OpenReplicator getOpenReplicator() {
        return openReplicator;
    }
}
