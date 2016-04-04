package com.booking.replication.util;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Simple utility class for loading configuration from yml file
 *
 * This is the structure of the config file:
 *
 *     schema_tracker:
 *         username: '<username>'
 *         password: '<password>'
 *         hosts:
 *             dc1: 'host-1'
 *             dc2: 'host-2'
 *     replicated_schema_name:
 *         username: '<username>'
 *         password: '<password>'
 *         slaves:
 *             dc1: ['host-dc1-1', 'host-dc1-2']
 *             dc2: ['host-dc2-1']
 *     zookeepers:
 *         dc1: 'hbase-dc1-zk1-host, ..., hbase-dc1-zk5-host'
 *         dc2: 'hbase-dc2-zk1-host, ..., hbase-dc2-zk5-host'
 *     graphite:
 *         namespace: '<my.graphite.namespace>'
 */
public class YAML {

    private static final Logger LOGGER = LoggerFactory.getLogger(YAML.class);

    private static final String SCHEMA_TRACKER  = "schema_history";
    private static final String HIVE_IMPORT_KEY = "hive_imports";

    public static Configuration loadReplicatorConfiguration(StartupParameters startupParameters){

        String  dc                     = startupParameters.getDc();
        String  schema                 = startupParameters.getSchema();
        String  applier                = startupParameters.getApplier();
        String  startingBinlogFilename = startupParameters.getBinlogFileName();
        Long    binlogPosition         = startupParameters.getBinlogPosition();
        String  lastBinlogFilename     = startupParameters.getLastBinlogFileName();
        String  configPath             = startupParameters.getConfigPath();
        Integer shard                  = startupParameters.getShard();
        Boolean useDeltaTables         = startupParameters.isDeltaTables();
        Boolean initialSnapshot        = startupParameters.isInitialSnapshot();

        Configuration rc = new Configuration();

        // staring position
        rc.setStartingBinlogFileName(startingBinlogFilename);
        rc.setStartingBinlogPosition(binlogPosition);
        rc.setLastBinlogFileName(lastBinlogFilename);

        // dc
        rc.setReplicantDC(dc);

        // delta tables
        rc.setWriteRecentChangesToDeltaTables(useDeltaTables);

        // initial snapshot mode
        rc.setInitialSnapshotMode(initialSnapshot);

        // yml
        Yaml yaml = new Yaml();
        try {
            InputStream in = Files.newInputStream(Paths.get(configPath));

            Map<String, Map<String,Object>> config =
                    (Map<String, Map<String,Object>>) yaml.load(in);

            for (String shardConfigKey : config.keySet()) {

                String shardName;

                if (shard > 0) {
                    shardName = schema + shard.toString();
                } else {
                    shardName = schema;
                }

                if (shardConfigKey.equals(shardName)) {

                    // configs
                    Map<String, Object> value = config.get(shardConfigKey);

                    rc.setReplicantSchemaName(shardConfigKey);
                    rc.setReplicantDBUserName((String) value.get("username"));
                    rc.setReplicantDBPassword((String) value.get("password"));
                    rc.setReplicantDBSlavesByDC((Map<String, List<String>>) value.get("slaves"));
                    rc.setReplicantSchemaName(schema);
                    rc.setReplicantShardID(shard);
                }

                if (shardConfigKey.equals(SCHEMA_TRACKER)) {
                    Map<String, Object> value = config.get(shardConfigKey);

                    rc.setActiveSchemaUserName((String) value.get("username"));
                    rc.setActiveSchemaPassword((String) value.get("password"));
                    rc.setActiveSchemaHostsByDC((Map<String, String>) value.get("host"));
                    rc.setActiveSchemaHost(rc.getActiveSchemaHostsByDC().get(dc));
                    if (shard > 0) {
                        rc.setActiveSchemaDB(schema + shard + "_" + Constants.ACTIVE_SCHEMA_SUFIX);
                    }
                    else {
                        rc.setActiveSchemaDB(schema + "_" + Constants.ACTIVE_SCHEMA_SUFIX);
                    }
                }

                if (shardConfigKey.equals("zookeepers")) {
                    Map<String, Object> value = config.get(shardConfigKey);
                    rc.setZOOKEEPER_QUORUM((String) value.get(dc));
                }

                if (shardConfigKey.equals("graphite")) {
                    Map<String, Object> value = config.get(shardConfigKey);
                    String graphiteStatsNamespace = (String) value.get("namespace");
                    rc.setGraphiteStatsNamesapce(graphiteStatsNamespace);
                }

                if (shardConfigKey.equals(HIVE_IMPORT_KEY)) {
                    Map<String, Object> value = config.get(shardConfigKey);
                    if (value.containsKey(shardName)) {
                        List<String> tableList = (List<String>) value.get(shardName);
                        rc.setTablesForWhichToTrackDailyChanges(tableList);
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        rc.setApplierType(applier);

        rc.setMetaDataDBName(schema + "_" + Constants.BLACKLISTED_DB);

        // TODO: Currently just take first slave from the list;
        //       later implement active slave tracking and slave fail-over

        rc.setReplicantDBActiveHost(rc.getReplicantDBSlavesByDC().get(rc.getReplicantDC()).iterator().next().toString());

        return rc;
    }
}
