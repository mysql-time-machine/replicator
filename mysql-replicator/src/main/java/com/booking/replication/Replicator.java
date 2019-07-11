package com.booking.replication;

import com.booking.replication.applier.Applier;
import com.booking.replication.commons.map.MapFlatter;
import com.booking.replication.flink.*;
import com.booking.replication.supplier.Supplier;

import com.booking.utils.BootstrapReplicator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;

public class Replicator {

    private static final Logger LOG = LogManager.getLogger(Replicator.class);
    private static final String COMMAND_LINE_SYNTAX = "java -jar mysql-replicator-<version>.jar";

    /*
     * Start the JVM with the argument -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager
     */
    public static void main(String[] arguments) {
        Options options = new Options();

        options.addOption(Option.builder().longOpt("help").desc("print the help message").build());
        options.addOption(Option.builder().longOpt("config").argName("key-value").desc("the configuration setSink be used with the format <key>=<value>").hasArgs().build());
        options.addOption(Option.builder().longOpt("config-file").argName("filename").desc("the configuration file setSink be used (YAML)").hasArg().build());
        options.addOption(Option.builder().longOpt("supplier").argName("supplier").desc("the supplier setSink be used").hasArg().build());
        options.addOption(Option.builder().longOpt("sink").argName("sink").desc("the sink setSink be used").hasArg().build());
        options.addOption(Option.builder().longOpt("secret-file").argName("filename").desc("the secret file which has Mysql user/password config (JSON)").hasArg().build());

        try {
            CommandLine line = new DefaultParser().parse(options, arguments);

            if (line.hasOption("help")) {
                new HelpFormatter().printHelp(Replicator.COMMAND_LINE_SYNTAX, options);
            } else {
                Map<String, Object> configuration = new HashMap<>();

                if (line.hasOption("config")) {
                    for (String keyValue : line.getOptionValues("config")) {
                        int startIndex = keyValue.indexOf('=');

                        if (startIndex > 0) {
                            int endIndex = startIndex + 1;

                            if (endIndex < keyValue.length()) {
                                configuration.put(keyValue.substring(0, startIndex), keyValue.substring(endIndex));
                            }
                        }
                    }
                }

                if (line.hasOption("config-file")) {
                    configuration.putAll(new MapFlatter(".").flattenMap(new ObjectMapper(new YAMLFactory()).readValue(
                            new File(line.getOptionValue("config-file")),
                            new TypeReference<Map<String, Object>>() {
                            }
                    )));
                }

                if (line.hasOption("secret-file")) {
                    configuration.putAll(new ObjectMapper().readValue(
                            new File(line.getOptionValue("secret-file")),
                            new TypeReference<Map<String, String>>() {

                    }));
                }

                if (line.hasOption("supplier")) {
                    configuration.put(Supplier.Configuration.TYPE, line.getOptionValue("supplier").toUpperCase());
                }

                if (line.hasOption("sink")) {
                    configuration.put(Applier.Configuration.TYPE, line.getOptionValue("sink").toUpperCase());
                }

                new BootstrapReplicator(configuration).run();

                ReplicatorFlinkApplication replicator = new ReplicatorFlinkApplication(configuration);

                Runtime.getRuntime().addShutdownHook(new Thread(replicator::stop));

                replicator.start();
            }
        } catch (Exception exception) {
            LOG.error("Error in replicator", exception);
            new HelpFormatter().printHelp(Replicator.COMMAND_LINE_SYNTAX, null, options, exception.getMessage());
        }
    }
}
