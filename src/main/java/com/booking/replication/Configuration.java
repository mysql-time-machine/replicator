package com.booking.replication;

import com.booking.replication.util.Duration;
import com.booking.replication.util.StartupParameters;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Joiner;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    private int     healthTrackerPort;
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
        public List<String> host_pool;
        public int          port        = 3306;
    }

    @JsonDeserialize
    private Payload payload;

    private static class Payload implements Serializable {
        public String table_name;
    }


    @JsonDeserialize
    @JsonProperty("mysql_failover")
    private MySQLFailover mySQLFailover;

    private static class MySQLFailover {

        @JsonDeserialize
        public PseudoGTIDConfig pgtid;

        private static class PseudoGTIDConfig implements Serializable {
            public String p_gtid_pattern;
            public String p_gtid_prefix;
        }

        @JsonDeserialize
        public Orchestrator orchestrator;

        private static class Orchestrator {
            public String username;
            public String password;
            public String url;
        }
    }

    @JsonDeserialize
    @JsonProperty("augmenter")
    private AugmenterConfiguration augmenterConfiguration = new AugmenterConfiguration();

    private static class AugmenterConfiguration {
        public boolean apply_uuid = false;
        public boolean apply_xid = false;
    }

    @JsonDeserialize
    @JsonProperty("converter")
    private ConverterConfiguration converterConfiguration = new ConverterConfiguration();

    private static class ConverterConfiguration {
        public boolean stringify_null = true;
    }

    @JsonDeserialize
    @JsonProperty("hbase")
    private HBaseConfiguration hbaseConfiguration;

    private static class HBaseConfiguration {

        public String       namespace;
        public List<String> zookeeper_quorum;
        public boolean      writeRecentChangesToDeltaTables;
        public boolean      apply_uuid = false;

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

    public static class KafkaConfiguration {
        public String broker;
        public List<String> tables;
        public List<String> excludetables;
        public int partitioning_method = PARTITIONING_METHOD_HASH_TABLE_NAME;
        public HashMap<String, String> partition_columns;
        public String topic;
        @JsonProperty("apply_begin_event")
        public Boolean applyBeginEvent = false;
        @JsonProperty("apply_commit_event")
        public Boolean applyCommitEvent = false;
        @JsonProperty("rows_per_message")
        public Integer numberOfRowsPerMessage = 10;
    }

    public static final int PARTITIONING_METHOD_HASH_ROW = 0;
    public static final int PARTITIONING_METHOD_HASH_TABLE_NAME = 1;
    public static final int PARTITIONING_METHOD_HASH_PRIMARY_COLUMN = 2;
    public static final int PARTITIONING_METHOD_HASH_CUSTOM_COLUMN = 3;

    public static class ValidationConfiguration {
        private String broker;
        private String topic;
        private String tag = "general";
        @JsonProperty("source_domain")
        private String sourceDomain;
        @JsonProperty("target_domain")
        private String targetDomain;
        private long throttling = TimeUnit.SECONDS.toMillis(5);

        public String getBroker() {
            return broker;
        }

        public String getTopic() {
            return topic;
        }

        public String getTag() {
            return tag;
        }

        public String getSourceDomain() {
            return sourceDomain;
        }

        public String getTargetDomain() {
            return targetDomain;
        }

        public long getThrottling() {
            return throttling;
        }
    }

    @JsonDeserialize
    @JsonProperty("validation")
    public ValidationConfiguration validationConfig;

    public ValidationConfiguration getValidationConfiguration(){
        return validationConfig;
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


    @JsonDeserialize
    @JsonProperty("orchestrator")
    public OrchestratorConfiguration orchestratorConfiguration = new OrchestratorConfiguration();

    public static class OrchestratorConfiguration {
        @JsonProperty("rewinding_threshold")
        private long rewindingThreshold = 1000;
        @JsonProperty("rewinding_enabled")
        private boolean rewindingEnabled = true;

        public long getRewindingThreshold() {
            return rewindingThreshold;
        }

        public boolean isRewindingEnabled() {
            return rewindingEnabled;
        }
    }


    public OrchestratorConfiguration getOrchestratorConfiguration(){
        return orchestratorConfiguration;
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

        if (replication_schema == null) {
            throw new RuntimeException("Replication schema cannot be null.");
        }
        if (replication_schema.name == null) {
            throw new RuntimeException("Replication schema name cannot be null.");
        }
        if (replication_schema.host_pool == null) {
            throw new RuntimeException("Replication schema host_pool cannot be null.");
        }
        if (replication_schema.username == null) {
            throw new RuntimeException("Replication schema user name cannot be null.");
        }

        if (metadata_store == null || (metadata_store.zookeeper == null && metadata_store.file == null)) {
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

        if (mySQLFailover == null) {
            throw new RuntimeException("MySql failover cannot be null.");
        }
    }

    /**
     * Serialize configuration.
     *
     * @return String Serialized configuration
     */
    public String toString() {

        Joiner joiner = Joiner.on(", ");

        StringBuilder config = new StringBuilder();

        // applier
        config.append("\n")
                .append("\tapplierType                       : ")
                .append(applierType)
                .append("\n");

        // replication schema
        config.append("\treplication_schema                : ")
                .append(replication_schema.name)
                .append("\n")
                .append("\tuser name                         : ")
                .append(replication_schema.username)
                .append("\n")
                .append("\treplicantDBSlaves                 : ")
                .append(Joiner.on(" | ").join(replication_schema.host_pool))
                .append("\n")
                .append("\treplicantDBActiveHost             : ")
                .append(this.getActiveSchemaHost())
                .append("\n")
                .append("\tactiveSchemaUserName              : ")
                .append(metadata_store.username)
                .append("\n")
                .append("\tactiveSchemaHost                  : ")
                .append(metadata_store.host)
                .append("\n")
                .append("\tactiveSchemaDB                    : ")
                .append(metadata_store.database)
                .append("\n");

        // hbase
        if (hbaseConfiguration != null) {
            if (hbaseConfiguration.hive_imports.tables != null) {
                config
                        .append("\tdeltaTables                       : ")
                        .append(hbaseConfiguration.writeRecentChangesToDeltaTables)
                        .append("\n")
                        .append("\tinitialSnapshotMode               : ")
                        .append(initialSnapshotMode)
                        .append("\n")
                        .append("\ttablesForWhichToTrackDailyChanges : ")
                        .append(joiner.join(hbaseConfiguration.hive_imports.tables))
                        .append("\n");
            }
        }

        // TODO: kafkaInfo

        // TODO: metadata_storeInfo

        // TODO: augmenterInfo

        // TODO: orchestratorInfo (rewind configuration)

        // TODO: validationInfo

        // metrics config
        if (metrics.reporters != null) {
            config.append("\tmetrics                           : ")
                    .append(metrics.reporters.toString())
                    .append("\n");
        }


        return config.toString();
    }

    // =========================================================================
    // Replication schema config getters
    public int getReplicantPort() {
        return replication_schema.port;
    }

    public List<String> getReplicantDBHostPool() {
        return this.replication_schema.host_pool;
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
    // Payload config getters
    public String getPayloadTableName() {
        return payload.table_name;
    }

    // =========================================================================
    // Orchestrator config getters
    public String getOrchestratorUserName() {
        return mySQLFailover.orchestrator.username;
    }

    @JsonIgnore
    public String getOrchestratorPassword() {
        return mySQLFailover.orchestrator.password;
    }

    public String getOrchestratorUrl() {
        return mySQLFailover.orchestrator.url;
    }

    public MySQLFailover getMySQLFailover() {
        return  mySQLFailover;
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
        return mySQLFailover.pgtid.p_gtid_pattern;
    }


    public String getpGTIDPrefix() {
        return mySQLFailover.pgtid.p_gtid_prefix;
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

    public int getHealthTrackerPort() {
        return this.healthTrackerPort;
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

    public boolean getHBaseApplyUuid(){
        return hbaseConfiguration.apply_uuid;
    }

    /**
     * Converter configuration getters.
     */
    public boolean getConverterStringifyNull(){
        return converterConfiguration.stringify_null;
    }

    /**
     * Augmenter configuration getters.
     */
    public boolean getAugmenterApplyUuid(){
        return augmenterConfiguration.apply_uuid;
    }
    public boolean getAugmenterApplyXid(){
        return augmenterConfiguration.apply_xid;
    }

    /**
     * Kafka configuration getters.
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

    public int getKafkaPartitioningMethod() { return kafka.partitioning_method; }

    public HashMap<String, String> getKafkaPartitionColumns() { return kafka.partition_columns; }

    public boolean isKafkaApplyBeginEvent() { return kafka.applyBeginEvent; }

    public boolean isKafkaApplyCommitEvent() { return kafka.applyCommitEvent; }

    public int getKafkaNumberOfRowsPerMessage() {
        return kafka.numberOfRowsPerMessage;
    }

    public boolean isDryRunMode() {
        return dryRunMode;
    }

    /**
     * Kafka configuration setters
     */

    public void setKafka(KafkaConfiguration kafka) {
        this.kafka = kafka;
    }

    public void setDryRunMode(boolean dryRunMode) {
        this.dryRunMode = dryRunMode;
    }

}
