package com.booking.replication.util;

/**
 * Simple utility class for parsing command line options
 */
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class CMD {
    private static final String DEFAULT_BINLOG_FILENAME_PATERN = "mysql-bin.";

    public static OptionSet parseArgs(String[] args) {

        OptionParser parser = new OptionParser();

        parser.accepts("hbase-namespace").withRequiredArg().ofType(String.class);
        parser.accepts("schema").withRequiredArg().ofType(String.class);
        parser.accepts("applier").withRequiredArg().ofType(String.class).defaultsTo("STDOUT");
        parser.accepts("binlog-filename").withRequiredArg().ofType(String.class).defaultsTo(DEFAULT_BINLOG_FILENAME_PATERN + "000001");
        parser.accepts("last-binlog-filename").withRequiredArg().ofType(String.class);
        parser.accepts("position").withRequiredArg().ofType(Long.class).defaultsTo(4L);
        parser.accepts("config-path").withRequiredArg().ofType(String.class).defaultsTo("./config.yml");
        parser.accepts("shard").withRequiredArg().ofType(Integer.class);
        parser.accepts("delta");
        parser.accepts("initial-snapshot");

        OptionSet options = parser.parse(args);

        return options;
    }
}

