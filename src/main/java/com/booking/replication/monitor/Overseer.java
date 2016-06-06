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

    private int observedStatus = ObservedStatus.OK;

    private static final Logger LOGGER = LoggerFactory.getLogger(Overseer.class);

    public Overseer(BinlogEventProducer prod, PipelineOrchestrator orch, ConcurrentHashMap<Integer, BinlogPositionInfo> chm) {
        this.producer      = prod;
        this.pipelineOrchestrator = orch;
        this.lastKnownInfo = chm;
    }

    @Override
    public void run() {
        while (doMonitor) {

            try {
                // make sure that producer is running every 1s
                Thread.sleep(1000);
                makeSureProducerIsRunning();
                // TODO: add status checks for pipelineOrchestrator and applier

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

    private void makeSureProducerIsRunning() {
        if (!producer.getOr().isRunning()) {
            LOGGER.warn("Producer stopped running. OR position: "
                    + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION)).getBinlogFilename()
                    + ":"
                    + ((BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_BINLOG_POSITION)).getBinlogPosition()
                    + "Trying to restart it...");
            try {
                BinlogPositionInfo lastMapEventFakeMCounter = (BinlogPositionInfo) lastKnownInfo.get(Constants.LAST_KNOWN_MAP_EVENT_POSITION_FAKE_MICROSECONDS_COUNTER);
                Long   lastFakeMCounter = lastMapEventFakeMCounter.getFakeMicrosecondsCounter();

                pipelineOrchestrator.setFakeMicrosecondCounter(lastFakeMCounter);

                producer.startOpenReplicatorFromLastKnownMapEventPosition();
                LOGGER.info("Restarted open replicator to run from position "
                        + producer.getOr().getBinlogFileName()
                        + ":"
                        + producer.getOr().getBinlogPosition()
                );
            }
            catch (ConnectException e) {
                LOGGER.error("Overseer tried to restart OpenReplicator and failed. Can not continue running. Requesting shutdown...");
                observedStatus = ObservedStatus.ERROR_SHOULD_SHUTDOWN;
                System.exit(-1);
            }
            catch (Exception e) {
                LOGGER.warn("Exception while trying to restart OpenReplicator", e);
                e.printStackTrace();
            }
        }
        else {
            LOGGER.debug("MonitorCheck: producer is running.");
        }
    }
}
