package com.booking.replication.monitor;

import com.booking.replication.Constants;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.BinlogEventProducer;
import com.booking.replication.pipeline.BinlogPositionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.ConnectException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bdevetak on 26/11/15.
 */
public class Overseer extends Thread {

    private PipelineOrchestrator pipelineOrchestrator;
    private BinlogEventProducer producer;
    private final ConcurrentHashMap<Integer, BinlogPositionInfo> lastKnownInfo;

    private volatile boolean doMonitor = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(Overseer.class);

    public Overseer(BinlogEventProducer prod, PipelineOrchestrator orch, ConcurrentHashMap<Integer, BinlogPositionInfo> chm) {
        producer      = prod;
        pipelineOrchestrator = orch;
        lastKnownInfo = chm;
    }

    @Override
    public void run() {
        while (doMonitor) {
            try {
                // make sure that producer is running every 1s
                Thread.sleep(1000);
                makeSureProducerIsRunning();
// TODO: add status checks for pipelineOrchestrator and applier
//                makeSurePipelineIsRunning();

            } catch (InterruptedException e) {
                LOGGER.error("Overseer thread interrupted", e);
                doMonitor = false;
            }
        }
    }

    public void stopMonitoring() {
        doMonitor = false;
    }

    public void startMonitoring() {
        doMonitor = true;
    }

    //todo: Make this check better
    private void makeSurePipelineIsRunning() {
        if (!pipelineOrchestrator.isRunning()) {
            LOGGER.info("PipelineOrchestrator is not running!");
        } else {
            System.exit(-1);
        }
    }

    private void makeSureProducerIsRunning() {
        if (!producer.getOpenReplicator().isRunning()) {
            LOGGER.warn("Producer stopped running. OR position: "
                    + lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION).getBinlogFilename()
                    + ":"
                    + lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION).getBinlogPosition()
                    + "Trying to restart it...");
            try {

                //todo: Investigate potential race condition in setting the microsecond counter,
                //the PO may still have queued up events when this reset happens
                BinlogPositionInfo lastMapEventFakeMCounter = lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION);
                Long   lastFakeMCounter = lastMapEventFakeMCounter.getFakeMicrosecondsCounter();

                PipelineOrchestrator.setFakeMicrosecondCounter(lastFakeMCounter);

                producer.startOpenReplicatorFromLastKnownMapEventPosition();
                LOGGER.info("Restarted open replicator to run from position "
                        + producer.getOpenReplicator().getBinlogFileName()
                        + ":"
                        + producer.getOpenReplicator().getBinlogPosition()
                );
            } catch (ConnectException e) {
                LOGGER.error("Overseer tried to restart OpenReplicator and failed. Can not continue running. Requesting shutdown...");
                System.exit(-1);
            } catch (Exception e) {
                LOGGER.warn("Exception while trying to restart OpenReplicator", e);
                e.printStackTrace();
            }
        } else {
            LOGGER.debug("MonitorCheck: producer is running.");
        }
    }
}
