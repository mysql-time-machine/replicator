package com.booking.replication.monitor;

import com.booking.replication.Constants;
import com.booking.replication.metrics.*;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.BinlogEventProducer;
import com.booking.replication.pipeline.BinlogPositionInfo;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import java.io.*;
import java.net.*;

/**
 * Created by bdevetak on 26/11/15.
 */
public class Overseer extends Thread {

    private PipelineOrchestrator pipelineOrchestrator;
    private BinlogEventProducer producer;
    private final ConcurrentHashMap<Integer, BinlogPositionInfo> lastKnownInfo;
    private final ReplicatorMetrics replicatorMetrics;

    private volatile boolean doMonitor = true;

    private int observedStatus = ObservedStatus.OK;

    private static final Logger LOGGER = LoggerFactory.getLogger(Overseer.class);

    public Overseer(BinlogEventProducer prod, PipelineOrchestrator orch, ReplicatorMetrics repMetrics, ConcurrentHashMap<Integer, BinlogPositionInfo> chm) {
        this.producer      = prod;
        this.pipelineOrchestrator = orch;
        this.lastKnownInfo = chm;
        this.replicatorMetrics = repMetrics;
    }

    @Override
    public void run() {
        while (doMonitor) {

            try {
                // make sure that producer is running every 1s
                Thread.sleep(1000);
                makeSureProducerIsRunning();
                String graphiteStatsNamespace = pipelineOrchestrator.configuration.getGraphiteStatsNamesapce();
                if (!graphiteStatsNamespace.equals("no-stats")) {
                    LOGGER.debug("processStats");
                    processStats();
                }
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

    // TODO: move this out of Overseer (it should only monitor state of other threads
    private void processStats() {

        int currentTimeSeconds = (int) (System.currentTimeMillis() / 1000L);

        List<String> metrics = new ArrayList<String>();

        String graphiteStatsNamespace = pipelineOrchestrator.configuration.getGraphiteStatsNamesapce();

        String dbAlias;

        if (pipelineOrchestrator.configuration.getReplicantShardID() > 0) {
            dbAlias = pipelineOrchestrator.configuration.getReplicantSchemaName()
                    + String.valueOf(pipelineOrchestrator.configuration.getReplicantShardID());
        }
        else {
            dbAlias = pipelineOrchestrator.configuration.getReplicantSchemaName();
        }

        // metrics per table (only for delta tables)
        if (pipelineOrchestrator.configuration.isWriteRecentChangesToDeltaTables()) {
            if (!graphiteStatsNamespace.equals("no-stats")) {
                List<String> deltaTables = pipelineOrchestrator.configuration.getTablesForWhichToTrackDailyChanges();

                Map<String, RowTotals> tablesToMetrics = replicatorMetrics.getTotalsPerTableSnapshot();

                for (String table : deltaTables) {
                    if (tablesToMetrics.containsKey(table)) {
                        RowTotals tableTotals = replicatorMetrics.getTotalsPerTableSnapshot().get(table);
                        if (tableTotals != null) {

                            INameValue[] metricValues = tableTotals.getAllNamesAndValues();

                            for (int i = 0; i < metricValues.length; i++) {
                                String graphitePoint =
                                        String.format("%s.%s.%s.%s %s %s",
                                                graphiteStatsNamespace,
                                                dbAlias,
                                                table,
                                                metricValues[i].getName(),
                                                metricValues[i].getValue().toString(),
                                                currentTimeSeconds);

                                metrics.add(graphitePoint);
                            }
                        }
                    }
                }
            }
        }

        Map<Integer, TotalsPerTimeSlot> metricSnapshot = replicatorMetrics.getMetricsSnapshot();

        // time bucket metric
        for (Integer timebucket : metricSnapshot.keySet()) {

            if (timebucket <  currentTimeSeconds) {

                LOGGER.debug("processing stats for bucket => " + timebucket + " since < then " + currentTimeSeconds);

                if (!graphiteStatsNamespace.equals("no-stats")) {

                    TotalsPerTimeSlot timebucketStats;
                    timebucketStats = metricSnapshot.get(timebucket);

                    INameValue[] metricValues = timebucketStats.getAllNamesAndValues();

                    for (int i = 0; i < metricValues.length; i++) {
                        String graphitePoint =
                                String.format("%s.%s.%s %s %s",
                                graphiteStatsNamespace,
                                dbAlias,
                                metricValues[i].getName(),
                                metricValues[i].getValue().toString(),
                                timebucket.toString());

                        LOGGER.debug("" +
                                "graphite point => " + graphitePoint);

                        metrics.add(graphitePoint);
                    }
                }

                String message = Joiner.on("\n").join(metrics) + "\n";
                LOGGER.debug("Graphite metrics from processed second => " + message);
                sendToGraphite(message);
                replicatorMetrics.removeBucketStats(timebucket);
            }
        }
    }

    private void sendToGraphite(String message) {

        DatagramSocket sock = null;
        int port = 3002;
        InetAddress host;

        String graphitUrl = pipelineOrchestrator.configuration.getGraphiteUrl();

        try {

            sock = new DatagramSocket();

            if(graphitUrl.contains(":")) {
                host = InetAddress.getByName(graphitUrl.split(":")[0]);
                port = Integer.parseInt(graphitUrl.split(":")[1]);
            } else {
                host = InetAddress.getByName(graphitUrl);
            }

            // send
            byte[] b = message.getBytes();
            DatagramPacket  dp = new DatagramPacket(b , b.length , host , port);
            sock.send(dp);
        }
        catch(IOException e) {
            LOGGER.warn("Graphite IOException ", e);
        }
    }
}
