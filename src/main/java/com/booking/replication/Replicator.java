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
import com.booking.replication.util.BinlogCoordinatesFinder;
import com.booking.replication.validation.ValidationService;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import static com.codahale.metrics.MetricRegistry.name;

import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static spark.Spark.get;
import static spark.Spark.port;

/**
 * Booking replicator. Has two main objects (producer and consumer)
 * that reference the same thread-safe queue.
 *
 * Producer pushes binlog events to the queue and consumer
 * reads them.
 *
 * Producer is basically a wrapper for open replicator
 * and/or binlog connector.
 *
 * Consumer is entry point for the event processing pipeline,
 * which includes:
 *      - schema version control,
 *      - augmenting events with schema information
 *      - storing events at designated destination (Kafka, HBase)
 */
public class Replicator {

    private final ReplicantPool                       replicantPool;
    private final LinkedBlockingQueue<RawBinlogEvent> rawBinlogEventQueue;
    private final BinlogEventProducer                 binlogEventSupplier;
    private final PipelineOrchestrator                pipelineOrchestrator;
    private       PipelinePosition                    pipelinePosition;
    private final ReplicatorHealthTrackerProxy        healthTracker;

    private static final int MAX_RAW_QUEUE_SIZE = Constants.MAX_RAW_QUEUE_SIZE;

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    public Replicator(Configuration configuration, int binlogParserProviderCode)
            throws Exception {

        Counter interestingEventsObservedCounter =
                Metrics.registry.counter(name("events", "applierEventsObserved"));

        this.healthTracker = new ReplicatorHealthTrackerProxy();
        if (configuration.getHealthTrackerPort() > 0) {
            startServerForHealthInquiries(configuration.getHealthTrackerPort(), healthTracker);
        }

        long fakeMicrosecondCounter = 0;

        boolean mysqlFailoverActive = false;
        if (configuration.getMySQLFailover() != null) {
            mysqlFailoverActive = true;
        }

        // Replicant Pool
        replicantPool = new MysqlReplicantPool(configuration.getReplicantDBHostPool(), configuration);

        // init pipelinePosition
        if (mysqlFailoverActive) {
            // failover mode
            initPipelinePositionInFailoverMode(configuration, fakeMicrosecondCounter);
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
        binlogEventSupplier = new BinlogEventProducer(
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
            binlogEventSupplier,
            fakeMicrosecondCounter
        );
        PipelineOrchestrator.registerMetrics();

    }

    private void initPipelinePositionInFailoverMode(Configuration configuration, Long fakeMicrosecondCounter) throws SQLException {

        if (configuration.getStartingBinlogFileName() != null) { // <- No previous PseudoGTID

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

        } else { // <- previous pseudoGTID available

            //  1. get PseudoGTID from safe checkpoint
            PseudoGTIDCheckpoint safeCheckPoint = Coordinator.getSafeCheckpoint();
            if ( safeCheckPoint != null ) {

                String pseudoGTID = safeCheckPoint.getPseudoGTID();
                fakeMicrosecondCounter = safeCheckPoint.getFakeMicrosecondCounter();

                if (pseudoGTID != null) {

                    //  2. get active host from the pool
                    String replicantActiveHost = replicantPool.getReplicantDBActiveHost();
                    int    serverID            = replicantPool.getReplicantDBActiveHostServerID();
                    boolean sameHost = replicantActiveHost.equals(safeCheckPoint.getHostName());

                    LOGGER.info("found pseudoGTID in safe checkpoint: " + pseudoGTID);

                    //  3. get binlog-filename and binlog-position that correspond to
                    //     Pseudo GTID on the active host.
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
                    LOGGER.warn("PseudoGTID not available in safe checkpoint. "
                            + "Defaulting back to host-specific binlog coordinates.");

                    // the binlog file name and position are host specific which means that
                    // we can't get mysql host from the pool. We must use the host from the
                    // safe checkpoint. If that host is not available then, without pseudoGTID,
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
    }

    private static void startServerForHealthInquiries(int port, IReplicatorHealthTracker healthTracker) {

        port(port);

        get("/is_healthy",
                (req, response) ->
                {
                    try
                    {
                        ReplicatorHealthAssessment healthAssessment = healthTracker.getLastHealthAssessment();

                        if (healthAssessment.isOk())
                        {
                            //For Marathon any HTTP code between 200 and 399 indicates we're healthy

                            response.status(200);
                            // don't really need the response body
                            return "";
                        }
                        else
                        {
                            response.status(503);
                            return healthAssessment.getDiagnosis();
                        }
                    }
                    catch (Exception e)
                    {
                        response.status(503);

                        String errorMessage = "Failed to assess the health status of the Replicator";

                        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).warn(errorMessage, e);

                        return errorMessage;
                    }
                });
    }

    // start()
    public void start() throws Exception {

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(

                new Thread(

                        () -> {

                            LOGGER.info("Executing replicator shutdown hook...");

                            // HealthTracker
                            try {
                                LOGGER.info("Stopping HealthTracker...");
                                healthTracker.stop();
                                LOGGER.info("HealthTracker thread successfully stopped");
                            } catch (Exception e) {
                                LOGGER.error("Failed to stop HealthTracker", e);
                            }

                            // Binlog Event Supplier
                            try {
                                // let open replicator stop its own threads
                                if (binlogEventSupplier.isRunning()) {
                                    LOGGER.info("Stopping Producer...");
                                    binlogEventSupplier.stop(10000, TimeUnit.MILLISECONDS);
                                    if (!binlogEventSupplier.isRunning()) {
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
                )
        );

        // Start up
        binlogEventSupplier.start();
        pipelineOrchestrator.start();
        healthTracker.start();

        while (!pipelineOrchestrator.isReplicatorShutdownRequested()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                LOGGER.error("Main thread interrupted with: ", ie);
                pipelineOrchestrator.requestReplicatorShutdown();
            }
        }
    }
}
