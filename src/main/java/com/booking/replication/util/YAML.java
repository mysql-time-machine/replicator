package com.booking.replication.util;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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

    private static final String SCHEMA_TRACKER  = "metadata_store";
    private static final String REPLICATION_SCHEMA_KEY = "replication_schema";
    private static final String HIVE_IMPORT_KEY = "hive_imports";

    public static Configuration loadReplicatorConfiguration(StartupParameters startupParameters){

        String  schema                  = startupParameters.getSchema();
        String  applier                 = startupParameters.getApplier();
        String  startingBinlogFilename  = startupParameters.getBinlogFileName();
        Long    binlogPosition          = startupParameters.getBinlogPosition();
        String  lastBinlogFilename      = startupParameters.getLastBinlogFileName();
        String  configPath              = startupParameters.getConfigPath();
        Integer shard                   = startupParameters.getShard();
        Boolean useDeltaTables          = startupParameters.isDeltaTables();
        Boolean initialSnapshot         = startupParameters.isInitialSnapshot();
        String  hbaseNamespace          = startupParameters.getHbaseNamespace();

        Configuration rc = new Configuration();

        // staring position
        rc.setStartingBinlogFileName(startingBinlogFilename);
        rc.setStartingBinlogPosition(binlogPosition);
        rc.setLastBinlogFileName(lastBinlogFilename);

        // hbase namespace
        rc.setHbaseNamespace(hbaseNamespace);

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

            for (String configCategoryKey : config.keySet()) {

                if (configCategoryKey.equals(REPLICATION_SCHEMA_KEY)) {

                    // configs
                    Map<String, Object> value = config.get(configCategoryKey);

                    rc.setReplicantSchemaName(configCategoryKey);
                    rc.setReplicantDBUserName((String) value.get("username"));
                    rc.setReplicantDBPassword((String) value.get("password"));

                    rc.setReplicantDBSlaves((List<String>) value.get("slaves"));

                    schema = (String) value.get("name");
                    rc.setReplicantSchemaName(schema);

                    rc.setReplicantShardID(shard);
                }

                String shardName;

                if (shard > 0) {
                    shardName = schema + shard.toString();
                } else {
                    shardName = schema;
                }

                if (configCategoryKey.equals(SCHEMA_TRACKER)) {
                    Map<String, Object> value = config.get(configCategoryKey);

                    rc.setActiveSchemaUserName((String) value.get("username"));
                    rc.setActiveSchemaPassword((String) value.get("password"));
                    rc.setActiveSchemaDSN((String) value.get("dsn"));

//                    if (shard > 0) {
//                        rc.setActiveSchemaDB(schema + shard + "_" + Constants.ACTIVE_SCHEMA_SUFIX);
//                    }
//                    else {
//                        rc.setActiveSchemaDB(schema + "_" + Constants.ACTIVE_SCHEMA_SUFIX);
//                    }
                }

                if (configCategoryKey.equals("zookeepers")) {
                    Map<String, Object> value = config.get(configCategoryKey);
                    rc.setZOOKEEPER_QUORUM(Joiner.on(",").join((List<String>) value.get("quorum")));
                }

                if (configCategoryKey.equals("graphite")) {
                    Map<String, Object> value = config.get(configCategoryKey);
                    String graphiteStatsNamespace = (String) value.get("namespace");
                    rc.setGraphiteStatsNamesapce(graphiteStatsNamespace);
                }

                if (configCategoryKey.equals(HIVE_IMPORT_KEY)) {
                    Map<String, Object> value = config.get(configCategoryKey);
                    if (value.containsKey(shardName)) {
                        List<String> tableList = (List<String>) value.get(shardName);
                        rc.setTablesForWhichToTrackDailyChanges(tableList);
                    }
                }
            }

            // optional config keys
            if (rc.getTablesForWhichToTrackDailyChanges() == null) {
                List<String> tableList = new ArrayList<>();
                rc.setTablesForWhichToTrackDailyChanges(tableList);
            }

        } catch (Exception e){
            e.printStackTrace();
        }

        rc.setApplierType(applier);

        rc.setMetaDataDBName(schema + "_" + Constants.BLACKLISTED_DB);

        // TODO: Currently just take first slave from the list;
        //       later implement active slave tracking and slave fail-over

        rc.setReplicantDBActiveHost(rc.getReplicantDBSlaves().get(0));

        return rc;
    }
}
