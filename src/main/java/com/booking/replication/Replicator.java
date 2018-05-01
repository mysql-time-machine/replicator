package com.booking.replication;

import com.booking.replication.applier.*;

import com.booking.replication.binlog.event.RawBinlogEvent;
import com.booking.replication.checkpoints.PseudoGTIDCheckpoint;

import com.booking.replication.monitor.*;
import com.booking.replication.schema.MysqlActiveSchemaVersion;

import com.booking.replication.pipeline.BinlogEventProducer;
import com.booking.replication.pipeline.BinlogPositionInfo;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;

import com.booking.replication.replicant.MysqlReplicantPool;
import com.booking.replication.replicant.ReplicantPool;
import com.booking.replication.schema.MysqlActiveSchemaVersion;
import com.booking.replication.util.BinlogCoordinatesFinder;
import com.booking.replication.validation.ValidationService;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Booking replicator. Has two main objects (producer and consumer)
 * that reference the same thread-safe queue.
 * Producer pushes binlog events to the queue and consumer
 * reads them.
 * Producer is basically a wrapper for open replicator
 * and/or binlog connector.
 * Consumer is entry point for the event processing pipeline,
 * which includes:
 *      - schema version control,
 *      - augmenting events with schema information
 *      - storing events at designated destination (Kafka, HBase)
 */
public class Replicator {

    private final LinkedBlockingQueue<RawBinlogEvent> rawBinlogEventQueue;
    private final BinlogEventProducer                 binlogEventProducer;
    private final PipelineOrchestrator                pipelineOrchestrator;
    private final Overseer                            overseer;
    private final ReplicantPool                       replicantPool;
    private final PipelinePosition                    pipelinePosition;
    private final ReplicatorHealthTrackerProxy        healthTracker;

    private final boolean metricsEnabled = true; // TODO: move to configuration

    private static final int MAX_RAW_QUEUE_SIZE = Constants.MAX_RAW_QUEUE_SIZE;

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    // Replicator()
    public Replicator(
          Configuration configuration,
          ReplicatorHealthTrackerProxy healthTracker,
          Counter interestingEventsObservedCounter,
          int binlogParserProviderCode
        ) throws Exception {

        this.healthTracker = healthTracker;
        long fakeMicrosecondCounter = 0;

        boolean mysqlFailoverActive = false;
        if (configuration.getMySQLFailover() != null) {
            mysqlFailoverActive = true;
        }

        // Replicant Pool
        replicantPool = new MysqlReplicantPool(configuration.getReplicantDBHostPool(), configuration);

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
                PseudoGTIDCheckpoint safeCheckPoint = Coordinator.getSafeCheckpoint();
                if ( safeCheckPoint != null ) {

                    String pseudoGTID = safeCheckPoint.getPseudoGTID();
                    fakeMicrosecondCounter = safeCheckPoint.getFakeMicrosecondCounter();

                    if (pseudoGTID != null) {

                        String replicantActiveHost = replicantPool.getReplicantDBActiveHost();
                        int    serverID            = replicantPool.getReplicantDBActiveHostServerID();
                        boolean sameHost = replicantActiveHost.equals(safeCheckPoint.getHostName());

                        LOGGER.info("found pseudoGTID in safe checkpoint: " + pseudoGTID);

                        BinlogCoordinatesFinder coordinatesFinder = new BinlogCoordinatesFinder(replicantActiveHost,3306,configuration.getReplicantDBUserName(),configuration.getReplicantDBPassword());

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
                            (sameHost ? safeCheckPoint.getLastVerifiedBinlogFileName() : startingBinlogFileName),
                            (sameHost ? safeCheckPoint.getLastVerifiedBinlogPosition() : startingBinlogPosition)
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
                PseudoGTIDCheckpoint safeCheckPoint = Coordinator.getSafeCheckpoint();
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

        rawBinlogEventQueue = new LinkedBlockingQueue<>(MAX_RAW_QUEUE_SIZE);

        // Producer
        binlogEventProducer = new BinlogEventProducer(
            rawBinlogEventQueue,
            pipelinePosition,
            configuration,
            replicantPool,
            binlogParserProviderCode
        );

        // Validation service
        ValidationService validationService = ValidationService.getInstance(configuration);

        // Applier
        Applier applier;
        Counting mainProgressCounter = null;
        String mainProgressCounterDescription = null;

        if (configuration.getApplierType().equals("STDOUT")) {
            applier = new EventCountingApplier(new StdoutJsonApplier(configuration), interestingEventsObservedCounter);
        } else if (configuration.getApplierType().toLowerCase().equals("hbase")) {
            mainProgressCounter = Metrics.registry.counter(name("HBase", "applierTasksSucceededCounter"));
            mainProgressCounterDescription = "# of HBase tasks that have succeeded";
            applier = new EventCountingApplier(new HBaseApplier(configuration, (Counter)mainProgressCounter, validationService), interestingEventsObservedCounter);
        } else if (configuration.getApplierType().toLowerCase().equals("kafka")) {
            mainProgressCounter = Metrics.registry.meter(name("Kafka", "producerToBroker"));
            mainProgressCounterDescription = "# of messages pushed to the Kafka broker";
            applier = new EventCountingApplier(new KafkaApplier(configuration, (Meter)mainProgressCounter), interestingEventsObservedCounter);
        } else {
            throw new RuntimeException(String.format("Unknown applier: %s", configuration.getApplierType()));
        }

        if (mainProgressCounter != null)
        {
            ReplicatorHealthTracker tracker = new ReplicatorHealthTracker(
                    new ReplicatorDoctor(mainProgressCounter, mainProgressCounterDescription, LoggerFactory.getLogger(ReplicatorDoctor.class.getName()), interestingEventsObservedCounter), 600);

            this.healthTracker.setTrackerImplementation(tracker);
        }
        else
        {
            this.healthTracker.setTrackerImplementation(new ReplicatorHealthTrackerDummy());
        }

        // Pipeline
        pipelineOrchestrator = new PipelineOrchestrator(
            rawBinlogEventQueue,
            pipelinePosition,
            configuration,
            new MysqlActiveSchemaVersion(configuration),
            applier,
            replicantPool,
            binlogEventProducer,
            fakeMicrosecondCounter,
            metricsEnabled
        );

        // Overseer
        // TODO: remove, obsolete since healthchecks have been added we relay
        //       on container orchestration to do the restarts
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
                    healthTracker.stop();
                    LOGGER.info("Overseer thread successfully stopped");
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted.", e);
                } catch (Exception e) {
                    LOGGER.error("Failed to stop Overseer", e);
                }
                // Producer
                try {
                    // let open replicator stop its own threads
                    if (binlogEventProducer.isRunning()) {
                        LOGGER.info("Stopping Producer...");
                        binlogEventProducer.stop(10000, TimeUnit.MILLISECONDS);
                        if (!binlogEventProducer.isRunning()) {
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

                // Spark Web Server
                try {
                    LOGGER.info("Stopping the Spark web server...");
                    Spark.stop(); // TODO: static stuff? Do we want to test this class?
                    LOGGER.info("Stopped the Spark web server...");
                }
                catch (Exception e) {
                    LOGGER.error("Failed to stop the Spark web server", e);
                }
            }
        });

        // Start up
        binlogEventProducer.start();
        pipelineOrchestrator.start();
        overseer.start();

        healthTracker.start();

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
