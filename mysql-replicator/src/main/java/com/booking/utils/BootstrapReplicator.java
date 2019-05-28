package com.booking.utils;

import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.applier.schema.registry.BCachedSchemaRegistryClient;
import com.booking.replication.augmenter.ActiveSchemaManager;
import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.model.event.format.avro.EventDataPresenterAvro;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.supplier.mysql.binlog.BinaryLogSupplier;

import org.apache.avro.Schema;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Creates active schema db,tables and publishes schema setSink schema registry if necessary.
 */
public class BootstrapReplicator {
    private static final Logger LOG = LogManager.getLogger(BootstrapReplicator.class);
    private final Map<String, Object> configuration;

    public BootstrapReplicator(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    public void run() {

        if ((boolean) configuration.get(Augmenter.Configuration.BOOTSTRAP) == false) {
            LOG.info("Skipping active schema bootstrapping");
            return;
        }
        LOG.info("Running bootstrapping");

        ActiveSchemaManager activeSchemaManager = new ActiveSchemaManager(configuration);
        boolean dbCreated = activeSchemaManager.createDbIfNotExists(configuration);
        if (!dbCreated) throw new IllegalStateException("Could not create active schema.");

        Object binlogSchemaObj = configuration.get(BinaryLogSupplier.Configuration.MYSQL_SCHEMA);
        Objects.requireNonNull(binlogSchemaObj);
        String binlogSchema = String.valueOf(binlogSchemaObj);

        BasicDataSource binLogDS = activeSchemaManager.initBinlogDatasource(configuration);

        try (Connection binlogConn = binLogDS.getConnection()) {
            PreparedStatement binlogShowTablesQuery = binlogConn.prepareStatement("show tables");
//            List<String> activeSchemaTables = activeSchemaManager.getActiveSchemaTables();
            ResultSet binlogTables = binlogShowTablesQuery.executeQuery();

            Object schemaRegistryUrlConfig = configuration.get(KafkaApplier.Configuration.SCHEMA_REGISTRY_URL);
            String dataFormat = configuration.get(KafkaApplier.Configuration.FORMAT) == null ? KafkaApplier.MessageFormat.AVRO : String.valueOf(configuration.get(KafkaApplier.Configuration.FORMAT));

            BCachedSchemaRegistryClient schemaRegistryClient = null;
            if (Objects.equals(dataFormat, KafkaApplier.MessageFormat.AVRO)) {
                schemaRegistryClient = new BCachedSchemaRegistryClient(String.valueOf(schemaRegistryUrlConfig), 2000);
            }

            while (binlogTables.next()) {
                String binlogTableName = binlogTables.getString(1);

                LOG.info(binlogTableName + " Recreating in active schema.");
                // TO prevent reading stale columns.
                activeSchemaManager.dropTable(binlogTableName);
                // implicitly creates table in active schema if doesnt exist
                List<ColumnSchema> columnSchemas = activeSchemaManager.listColumns(binlogTableName);

                if (schemaRegistryClient == null) continue;

                Schema avroSchema = EventDataPresenterAvro.createAvroSchema(true, true, new FullTableName(binlogSchema, binlogTableName), columnSchemas);
                String schemaKey = String.format("bigdata-%s-%s-value", binlogSchema, binlogTableName);
                LOG.info("Registering " + schemaKey + " in schemaregistry.");
                schemaRegistryClient.register(schemaKey, avroSchema);
            }
            LOG.info("Finished bootstrapping.");
        } catch (Exception e) {
            LOG.error("Error while bootstrapping", e);
        }
    }
}
