package com.booking.replication.pipeline;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;

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

    private final ConcurrentHashMap<Integer,Object> lastKnownInfo;

    private final OpenReplicator or;
    private final Configuration configuration;

    private long opCounter = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogEventProducer.class);

    public BinlogEventProducer(BlockingQueue<BinlogEventV4> q, ConcurrentHashMap chm, Configuration c) {
        this.configuration = c;
        this.queue = q;
        this.lastKnownInfo = chm;
        LOGGER.info("Created producer with lastKnownInfo position => { "
                + " binlogFileName => "
                +   ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION)).getBinlogFilename()
                + ", binlogPosition => "
                +   ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION)).getBinlogPosition()
                + " }"
        );
        this.or = new OpenReplicator();
    }

    public ConcurrentHashMap<Integer, Object> getLastKnownInfo() {
        return lastKnownInfo;
    }

    public void start() throws Exception {

        // init
        or.setUser(this.configuration.getReplicantDBUserName());
        or.setPassword(this.configuration.getReplicantDBPassword());
        or.setHost(this.configuration.getReplicantDBActiveHost());
        or.setPort(this.configuration.getReplicantPort());
        or.setServerId(this.configuration.getReplicantDBServerID());

        or.setBinlogPosition(this.configuration.getStartingBinlogPosition());
        or.setBinlogFileName(this.configuration.getStartingBinlogFileName());

        // disable lv2 buffer
        or.setLevel2BufferSize(-1);

        LOGGER.info("starting OR from: { file => "
                + or.getBinlogFileName()
                + ", position => "
                + or.getBinlogPosition()
                + " }"
        );

        or.setBinlogEventListener(new BinlogEventListener() {

            public void onEvents(BinlogEventV4 event) {

                long start = System.currentTimeMillis();

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
                            }
                            else {
                                LOGGER.error("queue.offer timed out. Will sleep for 100ms and try again");
                                Thread.sleep(100);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                long end = System.currentTimeMillis();
                long diff = end - start;
            }
        });

        or.start();
    }

    private void backPressureSleep() {

        int qSize = queue.size();

        int qPercent = (int) (100 * ((float) qSize / Constants.MAX_RAW_QUEUE_SIZE));

        // LOGGER.info("qPercent => " + qPercent + "%");

        long preasureSleep = 0;

        if      (qPercent < 30) { preasureSleep = 0;    }
        else if (qPercent < 40) { preasureSleep = 4;    }
        else if (qPercent < 50) { preasureSleep = 8;    }
        else if (qPercent < 60) { preasureSleep = 16;   }
        else if (qPercent < 70) { preasureSleep = 32;   }
        else if (qPercent < 75) { preasureSleep = 64;   }
        else if (qPercent < 80) { preasureSleep = 128;  }
        else if (qPercent < 85) { preasureSleep = 256;  }
        else if (qPercent < 90) { preasureSleep = 1024; }
        else if (qPercent < 95) { preasureSleep = 2048; }
        else if (qPercent < 95) { preasureSleep = 4096; }
        else                    { preasureSleep = 8192; }

        if (preasureSleep > 4000) {
            LOGGER.warn("Queue is getting big, back pressure is getting high");
        }

        try {
            Thread.sleep(preasureSleep);
        } catch (InterruptedException e) {
            LOGGER.error("Thread wont sleep");
            e.printStackTrace();
        }
    }

    public void startOpenReplicatorFromLastKnownMapEventPosition() throws Exception, java.net.ConnectException {
        if (or != null) {
            if (!or.isRunning()) {
                if (lastKnownInfo != null) {
                    BinlogPositionInfo lastMapEventPosition = (BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION);
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
                            this.or.setBinlogFileName(
                                    ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogFilename()
                            );
                            this.or.setBinlogPosition(
                                    ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION)).getBinlogPosition()
                            );
                            this.or.start();
                        }
                        else {
                            LOGGER.error("last mapEvent position object is not initialized. This should not happen. Shuting down...");
                        }
                    }
                    else {
                        LOGGER.error("lastMapEventPosition object is null. This should not happen. Shuting down...");
                        Runtime.getRuntime().exit(1);
                    }
                }
            }
            else {
                LOGGER.error("lastKnownInfo is gone. This should never happen. Shutting down...");
                Runtime.getRuntime().exit(1);
            }
        }
        else {
            LOGGER.error("OpenReplicator is gone, need to recreate it again, but that is not yet implemented. So for now, just shutdown.");
            Runtime.getRuntime().exit(1);
        }
    }

    public void stop(long timeout, TimeUnit unit) throws Exception {
        or.stop(timeout, unit);
    }

    public boolean isRunning()
    {
        return this.or.isRunning();
    }

    public OpenReplicator getOr() {
        return or;
    }
}
