package com.booking.replication.util;

/**
 * Simple utility class for parsing command line options.
 */
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Cmd {
    private static final String DEFAULT_BINLOG_FILENAME_PATERN = "mysql-bin.";

    public static OptionSet parseArgs(String[] args) {

        OptionParser parser = new OptionParser();

        parser.accepts("hbase-namespace").withRequiredArg().ofType(String.class);
        parser.accepts("schema").withRequiredArg().ofType(String.class);
        parser.accepts("parser").withRequiredArg().ofType(String.class);
        parser.accepts("applier").withRequiredArg().ofType(String.class).defaultsTo("STDOUT");

        parser.accepts("binlog-filename").withRequiredArg().ofType(String.class);
        parser.accepts("binlog-position").withRequiredArg().ofType(Long.class).defaultsTo(4L);

        parser.accepts("last-binlog-filename").withRequiredArg().ofType(String.class);
        parser.accepts("config-path").withRequiredArg().ofType(String.class).defaultsTo("./config.yml");
        parser.accepts("delta");
        parser.accepts("dryrun");
        parser.accepts("initial-snapshot");

        return parser.parse(args);
    }
}

