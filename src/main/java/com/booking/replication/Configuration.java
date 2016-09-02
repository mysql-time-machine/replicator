package com.booking.replication;

import com.booking.replication.util.Duration;
import com.booking.replication.util.StartupParameters;
import com.google.common.base.Joiner;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Configuration instance.
 *
 * <p>This object is instantiated by deserializing from a yaml config file.</p>
 */
public class Configuration {

    /**
     * Empty constructor.
     */
    public Configuration() {}

    private boolean initialSnapshotMode;
    private boolean dryRunMode;
    private long    startingBinlogPosition;
    private String  startingBinlogFileName;
    private String  endingBinlogFileName;
    private String  applierType;

    @JsonDeserialize
    private ReplicationSchema replication_schema;

    private static class ReplicationSchema implements Serializable {
        public String       name;
        public String       username;
        public String       password;
        public String       host;
        public int          port        = 3306;
        public int          server_id   = 1;
    }

    @JsonDeserialize
    private PseudoGTIDConfig pgtid;

    private static class PseudoGTIDConfig implements Serializable {
        public String p_gtid_pattern;
    }


    @JsonDeserialize
    private Orchestrator orchestrator;

    private static class Orchestrator {
        public String username;
        public String password;
        public String url;
    }

    @JsonDeserialize
    @JsonProperty("hbase")
    private HBaseConfiguration hbaseConfiguration;

    private static class HBaseConfiguration {

        public String       namespace;
        public List<String> zookeeper_quorum;
        public boolean      writeRecentChangesToDeltaTables;

        @JsonDeserialize
        public HiveImports     hive_imports = new HiveImports();

        private static class HiveImports {
            public List<String> tables = Collections.emptyList();
        }
    }

    @JsonDeserialize
    private MetadataStore metadata_store;

    private static class MetadataStore {
        public String       username;
        public String       password;
        public String       host;
        public String       database;

        @JsonDeserialize
        public ZookeeperConfig zookeeper;

        private static class ZookeeperConfig {
            public List<String> quorum;
            public String       path = "/";
        }

        @JsonDeserialize
        public FileConfig file;

        private static class FileConfig {
            public String       path;
        }
    }

    public static final int METADATASTORE_ZOOKEEPER = 1;
    public static final int METADATASTORE_FILE      = 2;

    /**
     * Metadata store type.
     *
     * @return Zookeeper/File
     */
    public int getMetadataStoreType() {
        if (metadata_store.zookeeper != null) {
            return METADATASTORE_ZOOKEEPER;
        } else if (metadata_store.file != null) {
            return METADATASTORE_FILE;
        } else {
            throw new RuntimeException("Metadata store not configured, please define a zookeeper or file metadata store.");
        }
    }

    @JsonDeserialize
    private KafkaConfiguration kafka = new KafkaConfiguration();

    private static class KafkaConfiguration {
        public String broker;
        public List<String> tables;
        public List<String> excludetables;
        public String topic;
    }

    @JsonDeserialize
    public MetricsConfig metrics = new MetricsConfig();

    public static class MetricsConfig {
        public Duration     frequency;

        @JsonDeserialize
        public HashMap<String, ReporterConfig> reporters = new HashMap<>();

        public static class ReporterConfig {
            public String       type;
            public String       namespace;
            public String       url;
        }
    }

    public Duration getReportingFrequency() {
        return metrics.frequency;
    }

    public HashMap<String, MetricsConfig.ReporterConfig> getMetricReporters() {
        return metrics.reporters;
    }

    /**
     * Get metrics reporter configuration.
     *
     * @param type  The type of reporter
     * @return      Configuration object
     */
    public MetricsConfig.ReporterConfig getReporterConfig(String type) {
        if (! metrics.reporters.containsKey(type)) {
            return null;
        }

        return metrics.reporters.get(type);
    }

    /**
     * Apply command line parameters to the configuration object.
     *
     * @param startupParameters     Startup parameters
     */
    public void loadStartupParameters(StartupParameters startupParameters ) {

        applierType = startupParameters.getApplier();

        if (applierType.equals("hbase") && hbaseConfiguration == null) {
            throw new RuntimeException("HBase not configured");
        }

        // staring position
        startingBinlogFileName = startupParameters.getBinlogFileName();
        startingBinlogPosition = startupParameters.getBinlogPosition();
        endingBinlogFileName   = startupParameters.getLastBinlogFileName();

        // hbase specific parameters
        if (applierType.equals("hbase") && hbaseConfiguration != null) {
            // delta tables
            hbaseConfiguration.writeRecentChangesToDeltaTables = startupParameters.isDeltaTables();
            // namespace
            if (startupParameters.getHbaseNamespace() != null) {
                hbaseConfiguration.namespace = startupParameters.getHbaseNamespace();
            }
        }

        // initial snapshot mode
        initialSnapshotMode = startupParameters.isInitialSnapshot();

        dryRunMode = startupParameters.isDryrun();

    }

    /**
     * Validate configuration.
     */
    public void validate() {

        if (replication_schema.name == null) {
            throw new RuntimeException("Replication schema name cannot be null.");
        }
        if (replication_schema.host == null) {
            throw new RuntimeException("Replication schema host name cannot be null.");
        }
        if (replication_schema.username == null) {
            throw new RuntimeException("Replication schema user name cannot be null.");
        }

        if (metadata_store.zookeeper == null && metadata_store.file == null) {
            throw new RuntimeException("No metadata store specified, please provide "
                    + "either zookeeper or file-based metadata storage.");
        } else if (metadata_store.zookeeper != null && metadata_store.zookeeper.quorum == null) {
            throw new RuntimeException("Metadata store set as zookeeper but no zookeeper quorum is specified");
        } else if (metadata_store.file != null && metadata_store.file.path == null) {
            throw new RuntimeException("Metadata store set as file but no path is specified");
        }

        if (applierType.equals("hbase")) {
            if (hbaseConfiguration.namespace == null) {
                throw new RuntimeException("HBase namespace cannot be null.");
            }
        }
    }

    /**
     * Serialize configuration.
     *
     * @return String Serialized configuration
     */
    public String toString() {
        try {
            return new ObjectMapper(new YAMLFactory()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    // =========================================================================
    // Replication schema config getters
    public int getReplicantPort() {
        return replication_schema.port;
    }

    public int getReplicantDBServerID() {
        return replication_schema.server_id;
    }

    public String getReplicantDBActiveHost() {
        return this.replication_schema.host;
    }

    public String getReplicantSchemaName() {
        return replication_schema.name;
    }

    public String getReplicantDBUserName() {
        return replication_schema.username;
    }

    @JsonIgnore
    public String getReplicantDBPassword() {
        return replication_schema.password;
    }

    // =========================================================================
    // Orchestrator config getters
    public String getOrchestratorUserName() {
        return orchestrator.username;
    }

    @JsonIgnore
    public String getOrchestratorPassword() {
        return orchestrator.password;
    }

    public String getOrchestratorUrl() {
        return orchestrator.url;
    }

    // =========================================================================
    // Binlog file names and position getters
    public String getStartingBinlogFileName() {
        return startingBinlogFileName;
    }

    public String getLastBinlogFileName() {
        return endingBinlogFileName;
    }

    public long getStartingBinlogPosition() {
        return startingBinlogPosition;
    }

    // =========================================================================
    // Applier type
    public String getApplierType() {
        return applierType;
    }


    // ========================================================================
    // Metadata store config getters
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

    public String getpGTIDPattern() {
        return pgtid.p_gtid_pattern;
    }

    /**
     * Get metadata store zookeeper quorum.
     */
    public String getZookeeperQuorum() {
        if (getMetadataStoreType() != Configuration.METADATASTORE_ZOOKEEPER) {
            return "[]";
        }
        return Joiner.on(",").join(metadata_store.zookeeper.quorum);
    }

    /**
     * Get metadata store zookeeper path.
     */
    public String getZookeeperPath() {
        if (getMetadataStoreType() != Configuration.METADATASTORE_ZOOKEEPER) {
            return "";
        }
        return metadata_store.zookeeper.path;
    }

    /**
     * Get metadata store file location.
     */
    public String getMetadataFile() {
        if (getMetadataStoreType() != Configuration.METADATASTORE_FILE) {
            return "";
        }
        return metadata_store.file.path;
    }

    /**
     * Get initial snapshot mode flag.
     */
    public boolean isInitialSnapshotMode() {
        return initialSnapshotMode;
    }

    /**
     * HBase configuration getters.
     */
    public String getHbaseNamespace() {
        if (hbaseConfiguration != null) {
            return hbaseConfiguration.namespace;
        } else {
            return null;
        }
    }

    public Boolean isWriteRecentChangesToDeltaTables() {
        if (hbaseConfiguration != null) {
            return hbaseConfiguration.writeRecentChangesToDeltaTables;
        } else {
            return null;
        }
    }

    public List<String> getTablesForWhichToTrackDailyChanges() {
        if (hbaseConfiguration != null) {
            return hbaseConfiguration.hive_imports.tables;
        } else {
            return null;
        }
    }

    public String getHBaseQuorum() {
        if (hbaseConfiguration != null) {
            return Joiner.on(",").join(hbaseConfiguration.zookeeper_quorum);
        } else {
            return null;
        }
    }

    /**
     * Kafka configuation getters.
     */
    public String getKafkaBrokerAddress() {
        return kafka.broker;
    }

    public List<String> getKafkaTableList() {
        return kafka.tables;
    }

    public List<String> getKafkaExcludeTableList() {
        return kafka.excludetables;
    }

    public String getKafkaTopicName() {
        return kafka.topic;
    }

    public boolean isDryRunMode() {
        return dryRunMode;
    }


}
