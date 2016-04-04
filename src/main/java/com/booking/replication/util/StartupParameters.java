package com.booking.replication.util;

import joptsimple.OptionSet;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by bdevetak on 01/12/15.
 */
public class StartupParameters {

    private String  configPath;
    private String  dc;
    private String  schema;
    private String  applier;
    private String  binlogFileName;
    private Long    binlogPosition;
    private String  lastBinlogFileName;
    private Integer shard;
    private boolean deltaTables;
    private boolean initialSnapshot;

    private static final String DEFAULT_BINLOG_FILENAME_PATERN = "mysql-bin.";

    public void init(OptionSet o) throws MissingArgumentException {

        // dc
        if (o.hasArgument("dc")) {
            dc = o.valueOf("dc").toString();
        }
        else {
            dc = "dc1";
        }

        // use delta tables
        if (o.has("delta")) {
            deltaTables = true;
        }
        else {
            deltaTables = false;
        }

        // initial snapshot mode
        if (o.has("initial-snapshot")) {
            initialSnapshot = true;
        }
        else {
            initialSnapshot = false;
        }

        // schema
        if (o.hasArgument("schema")) {
            schema = o.valueOf("schema").toString();
            // shards can be specified in config file as ${schema_name}${shard_id}
            String maybeNumber = (new String(schema)).replaceAll("[A-Za-z]", "");
            shard = StringUtils.isNotBlank(maybeNumber) ? Integer.parseInt(maybeNumber) : 0;
            schema = schema.replaceAll("[0-9]","");
        }
        else {
            schema = "test";
        }

        // shard_id can also be explicity passed as cmd argument - that will overide config file setting
        if (o.hasArgument("shard")) {
            shard = Integer.parseInt(o.valueOf("shard").toString());
        }

        // config-path
        if (o.hasArgument("config-path")) {
            configPath = o.valueOf("config-path").toString();
        }
        else {
            configPath = "./config.yml";
        }

        // applier, defaults to STDOUT
        if (o.hasArgument("applier")) {
            applier = o.valueOf("applier").toString();
        }
        else {
            applier = "STDOUT";
        }

        // binlog-filename
        if (o.hasArgument("binlog-filename")) {
            binlogFileName = o.valueOf("binlog-filename").toString();
        }
        else {
            binlogFileName = DEFAULT_BINLOG_FILENAME_PATERN + "000001";
        }

        // position
        if (o.hasArgument("position")) {
            binlogPosition = Long.parseLong(o.valueOf("position").toString());
        }
        else {
            // default to 4
            binlogPosition = 4L;
        }

        if (o.hasArgument("last-binlog-filename")) {
            lastBinlogFileName = o.valueOf("last-binlog-filename").toString();
        }

        System.out.println("----------------------------------------------");
        System.out.println("Parsed params:          ");
        System.out.println("\tconfig-path:          " + configPath);
        System.out.println("\tdc:                   " + dc);
        System.out.println("\tschema:               " + schema);
        System.out.println("\tapplier:              " + applier);
        System.out.println("\tbinlog-filename:      " + binlogFileName);
        System.out.println("\tposition:             " + binlogPosition);
        System.out.println("\tlast-binlog-filename: " + lastBinlogFileName);
        System.out.println("\tinitial-snapshot:     " + initialSnapshot);
        System.out.println("----------------------------------------------\n");

    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getApplier() {
        return applier;
    }

    public void setApplier(String applier) {
        this.applier = applier;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public String getLastBinlogFileName() {
        return lastBinlogFileName;
    }

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }

    public Long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(Long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public Integer getShard() {
        return shard;
    }

    public void setShard(Integer shard) {
        this.shard = shard;
    }

    public boolean isDeltaTables() {
        return deltaTables;
    }

    public boolean isInitialSnapshot() {
        return initialSnapshot;
    }
}
