package com.booking.replication;

import com.booking.replication.util.StartupParameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Joiner;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sun.tools.javac.util.Assert;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Stores configuration properties
 */
public class Configuration {

    /**
     * Constructor
     */
    public Configuration() {}

    private String          applierType;

    @JsonDeserialize
    private ReplicationSchema replication_schema;

    private static class ReplicationSchema implements Serializable {
        public String       name;
        public String       username;
        public String       password;
        public List<String> slaves;
        public int          port        = 3306;
        public int          server_id   = 1;
        public int          shard_id;
    }

    @JsonDeserialize
    private HBaseConfiguration hbase;

    private static class HBaseConfiguration {
        public String       namespace;
        public List<String> zookeeper_quorum;
        public boolean      writeRecentChangesToDeltaTables;


        @JsonDeserialize
        public HiveImports     hive_imports = new HiveImports();

        private static class HiveImports {
            public List<String> tables = Collections.<String>emptyList();
        }

    }

    @JsonDeserialize
    private MetadataStore metadata_store;

    private static class MetadataStore {
        public String       username;
        public String       password;
        public String       host;
        public String       database;
        public List<String> zookeeper_quorum;
    }

    @JsonDeserialize
    private GraphiteConfig graphite;

    private static class GraphiteConfig {
        public String       namespace;
        public String       url;
    }


    private boolean         initialSnapshotMode;
    private long            startingBinlogPosition;
    private String          startingBinlogFileName;
    private String          endingBinlogFileName;

    public void loadStartupParameters(StartupParameters startupParameters ) {

        applierType = startupParameters.getApplier();

        if(applierType == "hbase" && hbase == null) {
            throw new RuntimeException("HBase not configured");
        }

        // staring position
        startingBinlogFileName = startupParameters.getBinlogFileName();
        startingBinlogPosition = startupParameters.getBinlogPosition();
        endingBinlogFileName   = startupParameters.getLastBinlogFileName();

        replication_schema.shard_id = startupParameters.getShard();

        // delta tables
        hbase.writeRecentChangesToDeltaTables = startupParameters.isDeltaTables();

        // initial snapshot mode
        initialSnapshotMode = startupParameters.isInitialSnapshot();

        //Hbase namespace
        if (startupParameters.getHbaseNamespace() != null) {
            hbase.namespace = startupParameters.getHbaseNamespace();
        }
    }

    public void validate() {

        Assert.checkNonNull(replication_schema.name);
        Assert.checkNonNull(replication_schema.slaves);
        Assert.checkNonNull(replication_schema.username);
        Assert.checkNonNull(replication_schema.password);

        if(applierType == "habse") {
            Assert.checkNonNull(hbase.namespace);
        }
    }

    public String toString() {
        try {
            return new ObjectMapper(new YAMLFactory()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    public int getReplicantPort() {
        return replication_schema.port;
    }

    public int getReplicantDBServerID() {
        return replication_schema.server_id;
    }

    public long getStartingBinlogPosition() {
        return startingBinlogPosition;
    }

    public String getReplicantDBActiveHost() {
        return this.replication_schema.slaves.get(0);
    }

    public String getReplicantDBUserName() {
        return replication_schema.username;
    }

    @JsonIgnore
    public String getReplicantDBPassword() {
        return replication_schema.password;
    }

    public String getStartingBinlogFileName() {
        return startingBinlogFileName;
    }

    public String getLastBinlogFileName() {
        return endingBinlogFileName;
    }

    public String getReplicantSchemaName() {
        return replication_schema.name;
    }

    public String getApplierType() {
        return applierType;
    }

    public String getActiveSchemaDSN() {
        return String.format("jdbc:mysql://%s/%s", metadata_store.host, metadata_store.database);
    }

    public String getActiveSchemaHost() {
        return metadata_store.host;
    }

    public String getActiveSchemaUserName() {
        return metadata_store.username;
    }

    @JsonIgnore
    public String getActiveSchemaPassword() {
        return metadata_store.password;
    }

    public String getActiveSchemaDB() {
        return metadata_store.database;
    }

    public int getReplicantShardID() {
        return replication_schema.shard_id;
    }

    public String getHBaseQuorum() {
        return Joiner.on(",").join(hbase.zookeeper_quorum);
    }

    public String getGraphiteStatsNamesapce() {
        return graphite.namespace;
    }

    public String getGraphiteUrl() {
        return graphite.url;
    }

    public boolean isWriteRecentChangesToDeltaTables() {
        return hbase.writeRecentChangesToDeltaTables;
    }

    public List<String> getTablesForWhichToTrackDailyChanges() {
        return hbase.hive_imports.tables;
    }

    public boolean isInitialSnapshotMode() {
        return initialSnapshotMode;
    }

    public String getHbaseNamespace() {
        return hbase.namespace;
    }
}
