package com.booking.replication;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.HBaseApplier;
import com.booking.replication.applier.KafkaApplier;
import com.booking.replication.applier.StdoutJsonApplier;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import com.booking.replication.monitor.Overseer;
import com.booking.replication.pipeline.BinlogEventProducer;
import com.booking.replication.pipeline.BinlogPositionInfo;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.booking.replication.queues.ReplicatorQueues;
import com.booking.replication.replicant.ReplicantPool;

import com.booking.replication.sql.QueryInspector;
import com.booking.replication.util.BinlogCoordinatesFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Booking replicator. Has two main objects (producer and consumer)
 * that reference the same thread-safe queue.
 * Producer pushes binlog events to the queue and consumer
 * reads them. Producer is basically a wrapper for open replicator,
 * and consumer is wrapper for all booking specific logic (schema
 * version control, augmenting events and storing events).
 */
public class Replicator {

    private final BinlogEventProducer  binlogEventProducer;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final Overseer             overseer;
    private final ReplicantPool        replicantPool;
    private final PipelinePosition     pipelinePosition;

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    // Replicator()
    public Replicator(Configuration configuration) throws Exception {

        boolean mysqlFailoverActive = false;
        if (configuration.getMySQLFailover() != null) {
            mysqlFailoverActive = true;
        }

        // Replicant Pool
        replicantPool = new ReplicantPool(configuration.getReplicantDBHostPool(), configuration);

        // 1. init pipelinePosition -> move to separate method
        if (mysqlFailoverActive) {

            // mysql high-availability mode
            if (configuration.getStartingBinlogFileName() != null) {

                // TODO: make mandatory to specify host name when starting from specific binlog file
                // At the moment the first host in the pool list is assumed when starting with
                // specified binlog-file

                String mysqlHost = configuration.getReplicantDBHostPool().get(0);
                int serverID     = replicantPool.getReplicantDBActiveHostServerID();

                LOGGER.info(String.format("Starting replicator in high-availability mode with: "
                        + "mysql-host %s, server-id %s, binlog-filename %s",
                        mysqlHost, serverID, configuration.getStartingBinlogFileName()));

                pipelinePosition = new PipelinePosition(
                        mysqlHost,
                        serverID,
                        configuration.getStartingBinlogFileName(),
                        configuration.getStartingBinlogPosition(),
                        configuration.getStartingBinlogFileName(),
                        4L
                );

            } else {
                // failover:
                //  1. get Pseudo GTID from safe checkpoint
                //  2. get active host from the pool
                //  3. get binlog-filename and binlog-position that correspond to
                //     Pseudo GTID on the active host (this is done by calling the
                //     MySQL Orchestrator http API (https://github.com/outbrain/orchestrator).
                LastCommittedPositionCheckpoint safeCheckPoint = Coordinator.getSafeCheckpoint();
                if ( safeCheckPoint != null ) {

                    String pseudoGTID = safeCheckPoint.getPseudoGTID();

                    if (pseudoGTID != null) {

                        String replicantActiveHost = replicantPool.getReplicantDBActiveHost();
                        int    serverID            = replicantPool.getReplicantDBActiveHostServerID();
                        boolean sameHost = replicantActiveHost.equals(safeCheckPoint.getHostName());

                        LOGGER.info("found pseudoGTID in safe checkpoint: " + pseudoGTID);

                        BinlogCoordinatesFinder coordinatesFinder = new BinlogCoordinatesFinder(replicantActiveHost,3306,configuration.getReplicantDBUserName(),configuration.getReplicantDBPassword(), new QueryInspector(configuration));

                        BinlogCoordinatesFinder.BinlogCoordinates coordinates = coordinatesFinder.findCoordinates(pseudoGTID);

                        String startingBinlogFileName = coordinates.getFileName();
                        Long   startingBinlogPosition = coordinates.getPosition();

                        LOGGER.info("PseudoGTID resolved to: " + startingBinlogFileName + ":" + startingBinlogPosition);

                        pipelinePosition = new PipelinePosition(
                            replicantActiveHost,
                            serverID,
                            startingBinlogFileName,
                            startingBinlogPosition,
                            // positions are not comparable between different hosts
                            (sameHost ? safeCheckPoint.getLastVerifiedBinlogFileName() : "NA"),
                            (sameHost ? safeCheckPoint.getLastVerifiedBinlogPosition() : 0)
                        );

                    } else {
                        LOGGER.warn("PsuedoGTID not available in safe checkpoint. "
                            + "Defaulting back to host-specific binlog coordinates.");

                        // the binlog file name and position are host specific which means that
                        // we can't get mysql host from the pool. We must use the host from the
                        // safe checkpoint. If that host is not avaiable then, without pGTID,
                        // failover can not be done and the replicator will exit with SQLException.
                        String mysqlHost = safeCheckPoint.getHostName();
                        int    serverID  = replicantPool.obtainServerID(mysqlHost);

                        String startingBinlogFileName = safeCheckPoint.getLastVerifiedBinlogFileName();
                        Long   startingBinlogPosition = safeCheckPoint.getLastVerifiedBinlogPosition();

                        pipelinePosition = new PipelinePosition(
                            mysqlHost,
                            serverID,
                            startingBinlogFileName,
                            startingBinlogPosition,
                            safeCheckPoint.getLastVerifiedBinlogFileName(),
                            safeCheckPoint.getLastVerifiedBinlogPosition()
                        );
                    }
                } else {
                    throw new RuntimeException("Could not load safe check point.");
                }
            }
        } else {
            // single replicant mode
            if (configuration.getStartingBinlogFileName() != null) {

                // TODO: make mandatory to specify host name when starting from specific binlog file
                // At the moment the first host in the pool list is assumed when starting with
                // specified binlog-file

                String mysqlHost = configuration.getReplicantDBHostPool().get(0);
                int serverID     = replicantPool.getReplicantDBActiveHostServerID();

                LOGGER.info(String.format("Starting replicator in single-replicant mode with: "
                    + "mysql-host %s, server-id %s, binlog-filename %s",
                    mysqlHost, serverID, configuration.getStartingBinlogFileName()));

                pipelinePosition = new PipelinePosition(
                    mysqlHost,
                    serverID,
                    configuration.getStartingBinlogFileName(),
                    configuration.getStartingBinlogPosition(),
                    configuration.getStartingBinlogFileName(),
                    4L
                );
            } else {
                LOGGER.info("Start binlog not specified, reading metadata from coordinator");
                // Safe Check Point
                LastCommittedPositionCheckpoint safeCheckPoint = Coordinator.getSafeCheckpoint();
                if ( safeCheckPoint != null ) {

                    String mysqlHost      = safeCheckPoint.getHostName();
                    int    serverID       = safeCheckPoint.getSlaveId();
                    String lastVerifiedBinlogFileName = safeCheckPoint.getLastVerifiedBinlogFileName();
                    Long   lastVerifiedBinlogPosition = safeCheckPoint.getLastVerifiedBinlogPosition();

                    // starting from checkpoint position, so startPosition := lastVerifiedPosition
                    pipelinePosition = new PipelinePosition(
                        mysqlHost,
                        serverID,
                        lastVerifiedBinlogFileName,
                        lastVerifiedBinlogPosition,
                        lastVerifiedBinlogFileName,
                        lastVerifiedBinlogPosition
                    );

                    LOGGER.info(
                        "Got safe checkpoint from coordinator: "
                        + "{ mysqlHost       => " + mysqlHost
                        + "{ serverID        => " + serverID
                        + "{ binlog-file     => " + lastVerifiedBinlogFileName
                        + ", binlog-position => " + lastVerifiedBinlogPosition
                        + " }"
                    );
                } else {
                    throw new RuntimeException("Could not find start binlog in metadata or startup options");
                }
            }
        }

        // 2. validate pipelinePosition
        if (configuration.getLastBinlogFileName() != null
                && pipelinePosition.getStartPosition().greaterThan(new BinlogPositionInfo(
                    replicantPool.getReplicantDBActiveHost(),
                    replicantPool.getReplicantDBActiveHostServerID(),
                    configuration.getLastBinlogFileName(), 4L))) {
            LOGGER.error(String.format(
                    "The current position is beyond the last position you configured.\nThe current position is: %s %s",
                    pipelinePosition.getStartPosition().getBinlogFilename(),
                    pipelinePosition.getStartPosition().getBinlogPosition())
            );
            System.exit(1);
        }

        LOGGER.info(String.format(
                "Starting replication from: %s %s",
                pipelinePosition.getStartPosition().getBinlogFilename(),
                pipelinePosition.getStartPosition().getBinlogPosition()));

        // Queues
        ReplicatorQueues replicatorQueues = new ReplicatorQueues();

        // Producer
        binlogEventProducer = new BinlogEventProducer(
            replicatorQueues.rawQueue,
            pipelinePosition,
            configuration,
            replicantPool
        );

        // Applier
        Applier applier;

        if (configuration.getApplierType().equals("STDOUT")) {
            applier = new StdoutJsonApplier(
                    configuration
            );
        } else if (configuration.getApplierType().toLowerCase().equals("hbase")) {
            applier = new HBaseApplier(
                    configuration
            );
        } else if (configuration.getApplierType().toLowerCase().equals("kafka")) {
            applier = new KafkaApplier(
                    configuration
            );
        } else {
            throw new RuntimeException(String.format("Unknown applier: %s", configuration.getApplierType()));
        }

        // Orchestrator
        pipelineOrchestrator = new PipelineOrchestrator(
            replicatorQueues,
            pipelinePosition,
            configuration,
            applier,
            replicantPool
    );

        // Overseer
        overseer = new Overseer(
                binlogEventProducer,
                pipelineOrchestrator,
                pipelinePosition
        );
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
                    if (binlogEventProducer.getOpenReplicator().isRunning()) {
                        LOGGER.info("Stopping Producer...");
                        binlogEventProducer.stop(10000, TimeUnit.MILLISECONDS);
                        if (!binlogEventProducer.getOpenReplicator().isRunning()) {
                            LOGGER.info("Successfully stopped Producer thread");
                        } else {
                            throw new Exception("Failed to stop Producer thread");
                        }
                    } else {
                        LOGGER.info("Producer was allready stopped.");
                    }
                } catch (InterruptedException ie) {
                    LOGGER.error("Interrupted.", ie);
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
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                LOGGER.error("Main thread interrupted with: ", ie);
                pipelineOrchestrator.requestReplicatorShutdown();
            }
        }

        System.exit(0);
    }
}
