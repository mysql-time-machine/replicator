package com.booking.replication.applier.hbase.schema;

import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.applier.hbase.StorageConfig;
import com.booking.replication.augmenter.model.schema.SchemaAtPositionCache;
import com.booking.replication.augmenter.model.schema.SchemaSnapshot;
import com.booking.replication.augmenter.model.schema.SchemaTransitionSequence;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bdevetak on 27/11/15.
 */
public class HBaseSchemaManager {

    private static final Logger LOG = LogManager.getLogger(HBaseSchemaManager.class);

    private final Configuration hbaseConfig;

    private final StorageConfig storageConfig;

    private Connection connection;

    private final Map<String, Integer> seenHBaseTables = new ConcurrentHashMap<>();

    private final Map<String, Object> configuration;

    // Mirrored tables
    private static final int MIRRORED_TABLE_DEFAULT_REGIONS = 16;
    private static final int MIRRORED_TABLE_NUMBER_OF_VERSIONS = 1000;

    // schema history table
    private static final int SCHEMA_HISTORY_TABLE_NR_VERSIONS = 1;

    private final boolean DRY_RUN;
    private static boolean USE_SNAPPY;

    private static final byte[] CF = Bytes.toBytes("d");

    private static final ObjectMapper MAPPER = new ObjectMapper();


    public HBaseSchemaManager(Map<String, Object> configuration) {

        DRY_RUN = (boolean) configuration.get(HBaseApplier.Configuration.DRYRUN);
        USE_SNAPPY = (boolean) configuration.get(HBaseApplier.Configuration.HBASE_USE_SNAPPY);

        this.configuration = configuration;

        this.storageConfig = StorageConfig.build(configuration);

        this.hbaseConfig = storageConfig.getConfig();

        if (!DRY_RUN) {
            try {
                connection = ConnectionFactory.createConnection(storageConfig.getConfig());
                LOG.info("HBaseSchemaManager successfully established connection to HBase.");
            } catch (IOException e) {
                LOG.error("HBaseSchemaManager could not connect to HBase", e);
            }
        }
    }

    public synchronized void createHBaseTableIfNotExists(String hbaseTableName) throws IOException {

        if (!DRY_RUN) {
            hbaseTableName = hbaseTableName.toLowerCase();
            try ( Admin admin = connection.getAdmin() ){

                if (seenHBaseTables.containsKey(hbaseTableName)) {
                    return;
                }

                if (connection == null) {
                    connection = ConnectionFactory.createConnection(storageConfig.getConfig());
                }

                TableName tableName;

                String namespace = (String) configuration.get(HBaseApplier.Configuration.TARGET_NAMESPACE);
                if (namespace.isEmpty()) {
                    tableName = TableName.valueOf(hbaseTableName);
                } else {
                    tableName = TableName.valueOf(namespace, hbaseTableName);
                }

                if (admin.tableExists(tableName)) {
                    LOG.warn("Table " + tableName + " exists in HBase, but not in schema cache. Probably a case of a table that was dropped and than created again");
                    seenHBaseTables.put(hbaseTableName, 1);
                } else {
                    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                    HColumnDescriptor cd = new HColumnDescriptor("d");

                    if (USE_SNAPPY) {
                        cd.setCompressionType(Compression.Algorithm.SNAPPY);
                    }

                    cd.setMaxVersions(MIRRORED_TABLE_NUMBER_OF_VERSIONS);
                    tableDescriptor.addFamily(cd);
                    tableDescriptor.setCompactionEnabled(true);

                    admin.createTable(tableDescriptor);

                    seenHBaseTables.put(hbaseTableName, 1);

                    LOG.warn("Created hbase table " + hbaseTableName);
                }

            } catch (IOException e) {
                throw new IOException("Failed to create table in HBase", e);
            }
        }
    }

    public void writeSchemaSnapshot(SchemaSnapshot schemaSnapshot, Map<String, Object> configuration)
            throws IOException, SchemaTransitionException {

        // get sql_statement
        String ddl = schemaSnapshot.getSchemaTransitionSequence().getDdl();
        if (ddl == null) {
            throw new SchemaTransitionException("DDL can not be null");
        }

        // get pre/post schemas
        SchemaAtPositionCache schemaSnapshotBefore = schemaSnapshot.getSchemaBefore();
        SchemaAtPositionCache schemaSnapshotAfter = schemaSnapshot.getSchemaAfter();

        Map<String, String> createStatementsBefore  = schemaSnapshot.getSchemaBefore().getCreateTableStatements();
        Map<String, String> createStatementsAfter = schemaSnapshot.getSchemaAfter().getCreateTableStatements();

        SchemaTransitionSequence schemaTransitionSequence = schemaSnapshot.getSchemaTransitionSequence();

        // json-ify
        // TODO: add unit test that makes sure that snapshot format is compatible with HBaseSnapshotter
        String jsonSchemaSnapshotBefore = MAPPER.writeValueAsString(schemaSnapshotBefore);
        String jsonSchemaSnapshotAfter = MAPPER.writeValueAsString(schemaSnapshotAfter);
        String jsonSchemaTransitionSequence = MAPPER.writeValueAsString(schemaTransitionSequence);
        String jsonCreateStatementsBefore = MAPPER.writeValueAsString(createStatementsBefore);
        String jsonCreateStatementsAfter = MAPPER.writeValueAsString(createStatementsAfter);

        // get event timestamp
        Long eventTimestamp = schemaSnapshot.getSchemaTransitionSequence().getSchemaTransitionTimestamp();

        String hbaseTableName = HBaseTableNameMapper.getSchemaSnapshotHistoryHBaseTableName(configuration);

        String hbaseRowKey = eventTimestamp.toString();
        if ((boolean)configuration.get(HBaseApplier.Configuration.INITIAL_SNAPSHOT_MODE)) {
            // in initial-snapshot mode timestamp is overridden by 0 so all create statements
            // fall under the same timestamp. This is ok since there should be only one schema
            // snapshot for the initial-snapshot. However, having key=0 is not good, so replace
            // it with:
            hbaseRowKey = "initial-snapshot";
        }

        try ( Admin admin = connection.getAdmin() ) {

            if (connection == null) {
                connection = ConnectionFactory.createConnection(storageConfig.getConfig());
            }

            TableName tableName = TableName.valueOf(hbaseTableName);

            if (!admin.tableExists(tableName)) {

                LOG.info("table " + hbaseTableName + " does not exist in HBase. Creating...");

                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor cd = new HColumnDescriptor("d");
                cd.setMaxVersions(SCHEMA_HISTORY_TABLE_NR_VERSIONS);
                tableDescriptor.addFamily(cd);
                tableDescriptor.setCompactionEnabled(true);

                admin.createTable(tableDescriptor);

            } else {
                LOG.info("Table " + hbaseTableName + " already exists in HBase. Probably a case of replaying the binlog.");
            }

            Put put = new Put(Bytes.toBytes(hbaseRowKey));

            String ddlColumnName = "ddl";
            put.addColumn(
                    CF,
                    Bytes.toBytes(ddlColumnName),
                    eventTimestamp,
                    Bytes.toBytes(ddl)
            );

            String schemaTransitionSequenceColumnName = "schemaTransitionSequence";
            put.addColumn(
                    CF,
                    Bytes.toBytes(schemaTransitionSequenceColumnName),
                    eventTimestamp,
                    Bytes.toBytes(jsonSchemaTransitionSequence)
            );

            String schemaSnapshotPreColumnName = "schemaPreChange";
            put.addColumn(
                    CF,
                    Bytes.toBytes(schemaSnapshotPreColumnName),
                    eventTimestamp,
                    Bytes.toBytes(jsonSchemaSnapshotBefore)
            );

            String schemaSnapshotPostColumnName = "schemaPostChange";
            put.addColumn(
                    CF,
                    Bytes.toBytes(schemaSnapshotPostColumnName),
                    eventTimestamp,
                    Bytes.toBytes(jsonSchemaSnapshotAfter)
            );

            String preChangeCreateStatementsColumn = "createsPreChange";
            put.addColumn(
                    CF,
                    Bytes.toBytes(preChangeCreateStatementsColumn),
                    eventTimestamp,
                    Bytes.toBytes(jsonCreateStatementsBefore)
            );

            String postChangeCreateStatementsColumn = "createsPostChange";
            put.addColumn(
                    CF,
                    Bytes.toBytes(postChangeCreateStatementsColumn),
                    eventTimestamp,
                    Bytes.toBytes(jsonCreateStatementsAfter)
            );

            Table hbaseTable = connection.getTable(tableName);
            hbaseTable.put(put);
            hbaseTable.close();
        } catch (TableExistsException tee) {
            LOG.warn("trying to create hbase table that already exists", tee);
        } catch (IOException ioe) {
            throw new SchemaTransitionException("Failed to store schemaChangePointSnapshot in HBase.", ioe);
        }
    }
}