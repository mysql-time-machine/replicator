package com.booking.replication.pipeline;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.booking.replication.Metrics;
import com.booking.replication.replicant.ReplicantPool;
import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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
    private final ReplicantPool    replicantPool;

    private long opCounter = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogEventProducer.class);

    private static final Meter producedEvents = Metrics.registry.meter(name("events", "eventsProduced"));

    /**
     * Set up and manage the Open Replicator instance.
     *
     * @param queue             Event queue.
     * @param pipelinePosition  Binlog position information
     * @param configuration     Replicator configuration
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

        openReplicator = new OpenReplicator();

        Metrics.registry.register(name("events", "producerBackPressureSleep"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return backPressureSleep;
                    }
                });

    }

    /**
     * Start.
     */
    public void start() throws Exception {

        Random random = new Random();

        int serverId = (random.nextInt() >>> 1) | (1 << 30); // a large positive random integer

        // config
        openReplicator.setUser(configuration.getReplicantDBUserName());
        openReplicator.setPassword(configuration.getReplicantDBPassword());
        openReplicator.setPort(configuration.getReplicantPort());

        // pool
        openReplicator.setHost(pipelinePosition.getCurrentReplicantHostName());
        openReplicator.setServerId(serverId);

        // position
        openReplicator.setBinlogPosition(pipelinePosition.getCurrentPosition().getBinlogPosition());
        openReplicator.setBinlogFileName(pipelinePosition.getCurrentPosition().getBinlogFilename());

        // disable lv2 buffer
        openReplicator.setLevel2BufferSize(-1);

        openReplicator.setBinlogEventListener(new BinlogEventListener() {

            public void onEvents(BinlogEventV4 event) {
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
                                if (opCounter % 100000 == 0) {
                                    LOGGER.info("Producer reporting queue size => " + queue.size());
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
            }
        });

        LOGGER.info("starting Open Replicator from: { binlog-file => "
                + openReplicator.getBinlogFileName()
                + ", position => "
                + openReplicator.getBinlogPosition()
                + " }"
        );
        openReplicator.start();
    }

    private long backPressureSleep = 0;

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

    public void stop(long timeout, TimeUnit unit) throws Exception {
        openReplicator.stop(timeout, unit);
    }

    public boolean isRunning() {
        return this.openReplicator.isRunning();
    }

    public OpenReplicator getOpenReplicator() {
        return openReplicator;
    }
}
