package com.booking.replication;

import com.booking.replication.coordinator.CoordinatorInterface;
import com.booking.replication.coordinator.FileCoordinator;
import com.booking.replication.coordinator.ZookeeperCoordinator;
import com.booking.replication.exceptions.ReplicatorStartException;
import com.booking.replication.monitor.IReplicatorHealthTracker;
import com.booking.replication.monitor.ReplicatorHealthAssessment;
import com.booking.replication.monitor.ReplicatorHealthTrackerProxy;
import com.booking.replication.sql.QueryInspector;
import com.booking.replication.util.Cmd;
import com.booking.replication.util.StartupParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.logging.Level;

import static com.codahale.metrics.MetricRegistry.name;
import static spark.Spark.get;
import static spark.Spark.port;

public class Main {

    private static int BINLOG_PARSER_PROVIDER_CODE;

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    /**
     * Main entry point
     */
    public static void main(String[] args) throws Exception {

        try {

            final Configuration configuration = initConfiguration(args);

            initCoordinator(configuration);

            Runnable replicatorStart = () -> {
                try {
                    Metrics.startReporters(configuration);
                    Replicator replicator = new Replicator(
                        configuration,
                        BINLOG_PARSER_PROVIDER_CODE
                    );
                    replicator.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };

            Coordinator.onLeaderElection(
                replicatorStart
            );

        } catch (Exception e) {
            throw new Exception("Replicator exception", e);
        }
    }

    private static void initCoordinator(Configuration configuration) {

        CoordinatorInterface coordinator;

        switch (configuration.getMetadataStoreType()) {

            case Configuration.METADATASTORE_ZOOKEEPER:
                coordinator = new ZookeeperCoordinator(configuration);
                break;
            case Configuration.METADATASTORE_FILE:
                coordinator = new FileCoordinator(configuration);
                break;
            default:
                throw new RuntimeException(String.format(
                        "Metadata store type not implemented: %s",
                        configuration.getMetadataStoreType()));
        }

        Coordinator.setImplementation(coordinator);
    }

    private static Configuration initConfiguration(String[] args) throws IOException {

        OptionSet optionSet = Cmd.parseArgs(args);

        StartupParameters startupParameters = new StartupParameters(optionSet);

        BINLOG_PARSER_PROVIDER_CODE = startupParameters.getParser();

        String  configPath = startupParameters.getConfigPath();

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        InputStream in = Files.newInputStream(Paths.get(configPath));

        Configuration configuration = mapper.readValue(in, Configuration.class);

        if (configuration == null) {
            throw new RuntimeException(String.format("Unable to load configuration from file: %s", configPath));
        }

        configuration.loadStartupParameters(startupParameters);

        configuration.validate();

        LOGGER.info("loaded configuration: \n" + configuration.toString());

        QueryInspector.setIsPseudoGTIDPattern(configuration.getpGTIDPattern());

        return configuration;
    }
}
