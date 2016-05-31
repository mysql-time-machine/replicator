package com.booking.replication;

import com.booking.replication.util.CMD;
import com.booking.replication.util.StartupParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import joptsimple.OptionSet;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.math3.linear.SymmLQ;
import org.jruby.RubyProcess;
import zookeeper.ZookeeperTalk;
import zookeeper.impl.ZookeeperTalkImpl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

        ZookeeperTalk zkTalk = new ZookeeperTalkImpl(configuration);

        zkTalk.onLeaderElection(
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

        System.out.println("Done leading...");
    }

}
