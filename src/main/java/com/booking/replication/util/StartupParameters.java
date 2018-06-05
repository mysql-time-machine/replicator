package com.booking.replication.util;

import com.booking.replication.binlog.BinlogEventParserProviderCode;
import joptsimple.OptionSet;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by bdevetak on 01/12/15.
 */
public class StartupParameters {

    private String  configPath;
    private String  schema;
    private String  applier;
    private String  binlogFileName;
    private Long    binlogPosition;
    private String  lastBinlogFileName;
    private boolean deltaTables;
    private boolean initialSnapshot;
    private String  hbaseNamespace;
    private boolean dryrun;
    private int     parser;
    private String  parserName;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(StartupParameters.class);

    public StartupParameters(OptionSet optionSet) throws IOException {

        // use delta tables
        deltaTables = optionSet.has("delta");

        // run in dry-run mode
        dryrun = optionSet.has("dryrun");

        // initial snapshot mode
        initialSnapshot  = optionSet.has("initial-snapshot");

        // schema
        if (optionSet.hasArgument("parser")) {
            parserName = optionSet.valueOf("parser").toString();
            if (parserName.equals("or")) {
                parser = BinlogEventParserProviderCode.OR;
            }
            else if (parserName.equals("bc")) {
                parser = BinlogEventParserProviderCode.SHYIKO;
            } else {
                throw  new IOException("Unsupported binlog parser " + parserName);
            }
        } else {
            parser = BinlogEventParserProviderCode.SHYIKO;
        }

        // schema
        if (optionSet.hasArgument("schema")) {
            schema = optionSet.valueOf("schema").toString();
        } else {
            schema = "test";
        }

        // config-path
        configPath = (String) optionSet.valueOf("config-path");

        // applier, defaults to STDOUT
        applier = (String) optionSet.valueOf("applier");

        // setup hbase namespace
        hbaseNamespace = (String) optionSet.valueOf("hbase-namespace");

        // Start binlog filename
        binlogFileName = (String) optionSet.valueOf("binlog-filename");

        // Start binlog position
        binlogPosition = (Long) optionSet.valueOf("binlog-position");

        // Last binlog filename
        lastBinlogFileName = (String) optionSet.valueOf("last-binlog-filename");

        System.out.println("----------------------------------------------");
        System.out.println("Parsed params:           ");
        System.out.println("\tconfig-path:           " + configPath);
        System.out.println("\tschema:                " + schema);
        System.out.println("\tapplier:               " + applier);
        System.out.println("\tbinlog-filename:       " + binlogFileName);
        System.out.println("\tposition:              " + binlogPosition);
        System.out.println("\tlast-binlog-filename:  " + lastBinlogFileName);
        System.out.println("\tinitial-snapshot:      " + initialSnapshot);
        System.out.println("\thbase-namespace:       " + hbaseNamespace);
        System.out.println("\tparser:                " + parserName);
        System.out.println("\tdry-run:               " + dryrun);
        System.out.println("----------------------------------------------\n");

    }

    public String getConfigPath() {
        return configPath;
    }

    public String getSchema() {
        return schema;
    }

    public String getApplier() {
        return applier;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public String getLastBinlogFileName() {
        return lastBinlogFileName;
    }

    public Long getBinlogPosition() {
        return binlogPosition;
    }

    public boolean isDeltaTables() {
        return deltaTables;
    }

    public boolean isDryrun() {
        return dryrun;
    }

    public boolean isInitialSnapshot() {
        return initialSnapshot;
    }

    public String getHbaseNamespace() {
        return hbaseNamespace;
    }

    public int getParser() {
        return parser;
    }

    public String getParserName() {
        return parserName;
    }
}
