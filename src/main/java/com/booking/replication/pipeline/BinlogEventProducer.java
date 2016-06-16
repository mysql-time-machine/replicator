package com.booking.replication.pipeline;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.booking.replication.Metrics;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Simple wrapper for Open Replicator. Writes events to blocking queue.
 */
public class BinlogEventProducer {

    // queue is private, but it will reference the same queue
    // as the consumer object
    private final BlockingQueue<BinlogEventV4> queue;

    private final ConcurrentHashMap<Integer,BinlogPositionInfo> lastKnownInfo;

    private final OpenReplicator openReplicator;
    private final Configuration configuration;

    private long opCounter = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogEventProducer.class);

    private static final Meter producedEvents = Metrics.registry.meter(name("events", "eventsProduced"));

    public BinlogEventProducer(
            BlockingQueue<BinlogEventV4> queue,
            ConcurrentHashMap<Integer, BinlogPositionInfo> lastKnownInfo,
            Configuration conf) {
        configuration = conf;
        this.queue = queue;
        this.lastKnownInfo = lastKnownInfo;
        LOGGER.info("Created producer with lastKnownInfo position => { "
                + " binlogFileName => "
                +   this.lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION).getBinlogFilename()
                + ", binlogPosition => "
                +   this.lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION).getBinlogPosition()
                + " }"
        );
        openReplicator = new OpenReplicator();

        Metrics.registry.register(name("events", "producerBackPressureSleep"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return backPressureSleep;
                    }
                });
    }

    public void start() throws Exception {
        // init
        openReplicator.setUser(configuration.getReplicantDBUserName());
        openReplicator.setPassword(configuration.getReplicantDBPassword());
        openReplicator.setHost(configuration.getReplicantDBActiveHost());
        openReplicator.setPort(configuration.getReplicantPort());
        openReplicator.setServerId(configuration.getReplicantDBServerID());

        openReplicator.setBinlogPosition(lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION).getBinlogPosition());
        openReplicator.setBinlogFileName(lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION).getBinlogFilename());

        // disable lv2 buffer
        openReplicator.setLevel2BufferSize(-1);

        LOGGER.info("starting OR from: { file => "
                + openReplicator.getBinlogFileName()
                + ", position => "
                + openReplicator.getBinlogPosition()
                + " }"
        );

        openReplicator.setBinlogEventListener(new BinlogEventListener() {

            public void onEvents(BinlogEventV4 event) {
                producedEvents.mark();

                // This call is blocking the writes from server side. If time goes above
                // net_write_timeout (which defaults to 60s) server will drop connection.
                //      => Use back pressure to regulate the write rate to the queue.
                backPressureSleep();

                if (isRunning()) {

                    boolean eventQueued = false;

                    while (!eventQueued) { // blocking block

                        try {
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

        openReplicator.start();
    }

    private long backPressureSleep = 0;

    private void backPressureSleep() {
        int queueSize = queue.size();

        backPressureSleep = (long) Math.pow(2, ((int) (13 * ((float) queueSize / Constants.MAX_RAW_QUEUE_SIZE))));

        if (backPressureSleep < 64) {
            return;
        }

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

    public void startOpenReplicatorFromLastKnownMapEventPosition() throws Exception {
        if (openReplicator != null) {
            if (!openReplicator.isRunning()) {
                if (lastKnownInfo != null) {
                    BinlogPositionInfo lastMapEventPosition = lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION);
                    if (lastMapEventPosition != null) {
                        String binlogFileName   = lastMapEventPosition.getBinlogFilename();
                        Long   binlogPosition   = lastMapEventPosition.getBinlogPosition();
                        if ((binlogFileName != null) && (binlogPosition != null)) {
                            LOGGER.info("restarting OR from last known map event: { file => "
                                    + binlogFileName
                                    + ", position => "
                                    + binlogPosition
                                    + " }"
                            );
                            openReplicator.setBinlogFileName(
                                    lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION).getBinlogFilename()
                            );
                            openReplicator.setBinlogPosition(
                                    lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION).getBinlogPosition()
                            );
                            openReplicator.start();
                        } else {
                            LOGGER.error("last mapEvent position object is not initialized. This should not happen. Shuting down...");
                        }
                    } else {
                        LOGGER.error("lastMapEventPosition object is null. This should not happen. Shuting down...");
                        Runtime.getRuntime().exit(1);
                    }
                }
            } else {
                LOGGER.error("lastKnownInfo is gone. This should never happen. Shutting down...");
                Runtime.getRuntime().exit(1);
            }
        } else {
            LOGGER.error("OpenReplicator is gone, need to recreate it again, but that is not yet implemented. So for now, just shutdown.");
            Runtime.getRuntime().exit(1);
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
