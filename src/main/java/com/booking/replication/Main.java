package com.booking.replication;

import com.booking.replication.coordinator.CoordinatorInterface;
import com.booking.replication.coordinator.FileCoordinator;
import com.booking.replication.coordinator.ZookeeperCoordinator;
import com.booking.replication.util.CMD;
import com.booking.replication.util.StartupParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import joptsimple.OptionSet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.URISyntaxException;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args) throws Exception {

        OptionSet optionSet = CMD.parseArgs(args);

        StartupParameters startupParameters = new StartupParameters(optionSet);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        String  configPath = startupParameters.getConfigPath();

        final Configuration configuration;

        try {
            InputStream in = Files.newInputStream(Paths.get(configPath));
            configuration = mapper.readValue(in, Configuration.class);

            if (configuration == null) {
                throw new RuntimeException(String.format("Unable to load configuration from file: %s", configPath));
            }

            configuration.loadStartupParameters(startupParameters);
            configuration.validate();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println("loaded configuration: \n" + configuration.toString());

        Coordinator.setConfiguration(configuration);
        CoordinatorInterface coordinator;
        switch (configuration.getMetadataStoreType()) {
            case Configuration.METADATASTORE_ZOOKEEPER:
                coordinator = new ZookeeperCoordinator(configuration);
                break;
            case Configuration.METADATASTORE_FILE:
                coordinator = new FileCoordinator(configuration);
                break;
            default:
                throw new RuntimeException(String.format("Metadata store type not implemented: %s", configuration.getMetadataStoreType()));
        }
        Coordinator.setImplementation(coordinator);

        Coordinator.onLeaderElection(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        new Replicator(configuration).start();
                    } catch (SQLException | URISyntaxException | IOException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        );
    }

}
