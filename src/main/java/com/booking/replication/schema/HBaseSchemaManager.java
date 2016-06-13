package com.booking.replication.schema;

import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.util.JSONBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bdevetak on 27/11/15.
 */
public class HBaseSchemaManager {

    private static final Configuration hbaseConf = HBaseConfiguration.create();

    private static Map<String, Integer> knownHBaseTables = new HashMap<>();

    private static Connection connection;

    private static final int DELTA_TABLE_MAX_VERSIONS = 1;

    private static final int INITIAL_SNAPSHOT_DEFAULT_REGIONS = 256;

    private static final int DAILY_DELTA_TABLE_DEFAULT_REGIONS = 16;

    private static final int MIRRORED_TABLE_DEFAULT_REGIONS = 16;

    private static final int DEFAULT_SCHEMA_VERSIONS = 1; // timestamp is part of row key

    private static final int SCHEMA_HISTORY_TABLE_DEFAULT_REGIONS = 1;

    private static final boolean DRY_RUN = false;

    private static final byte[] CF = Bytes.toBytes("d");

    public HBaseSchemaManager(String ZOOKEEPER_QUORUM) {

        hbaseConf.set("hbase.zookeeper.quorum",ZOOKEEPER_QUORUM);

        if (! DRY_RUN) {
            try {
                connection = ConnectionFactory.createConnection(hbaseConf);
                LOGGER.info("HBaseSchemaManager successfully established connection to HBase.");
            } catch (IOException e) {
                LOGGER.error("HBaseSchemaManager could not connect to HBase");
                e.printStackTrace();
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaManager.class);

    public void createMirroredTableIfNotExists(String hbaseTableName, Integer versions)  {

        try {

            if (!DRY_RUN) {
                if (connection == null) {
                    connection = ConnectionFactory.createConnection(hbaseConf);
                }

                Admin admin = connection.getAdmin();
                TableName TABLE_NAME = TableName.valueOf(hbaseTableName);

                if (!admin.tableExists(TABLE_NAME)) {

                    LOGGER.info("table " + hbaseTableName + " does not exist in HBase. Creating...");

                    HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
                    HColumnDescriptor cd = new HColumnDescriptor("d");
                    cd.setMaxVersions(versions);
                    tableDescriptor.addFamily(cd);

                    // presplit into 16 regions
                    RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                    byte[][] splitKeys = splitter.split(MIRRORED_TABLE_DEFAULT_REGIONS);

                    admin.createTable(tableDescriptor, splitKeys);
                } else {
                    LOGGER.info("Table " + hbaseTableName + " allready exists in HBase. Probably a case of replaying the binlog.");
                }

                knownHBaseTables.put(hbaseTableName, 1);
            }
        } catch (IOException e) {
            LOGGER.info("Failed to create table in HBase.");
            // TODO: wait and retry if failed. After a while set status of applier
            // to 'blocked' & handle by overseer by stopping the replicator
            e.printStackTrace();
        }
    }

    public void createDeltaTableIfNotExists(String hbaseTableName, boolean isInitialSnapshotMode)  {

        try {
            if (! DRY_RUN) {

                if (connection == null) {
                    connection = ConnectionFactory.createConnection(hbaseConf);
                }

                Admin admin = connection.getAdmin();
                TableName TABLE_NAME = TableName.valueOf(hbaseTableName);

                if (!admin.tableExists(TABLE_NAME)) {

                    LOGGER.info("table " + hbaseTableName + " does not exist in HBase. Creating...");

                    HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
                    HColumnDescriptor cd = new HColumnDescriptor("d");
                    cd.setMaxVersions(DELTA_TABLE_MAX_VERSIONS);
                    tableDescriptor.addFamily(cd);

                    // if daily table pre-split to 16 regions;
                    // if initial snapshot pre-split to 256 regions
                    if (isInitialSnapshotMode) {
                        RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                        byte[][] splitKeys = splitter.split(INITIAL_SNAPSHOT_DEFAULT_REGIONS);
                        admin.createTable(tableDescriptor, splitKeys);
                    } else {
                        RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                        byte[][] splitKeys = splitter.split(DAILY_DELTA_TABLE_DEFAULT_REGIONS);
                        admin.createTable(tableDescriptor, splitKeys);
                    }
                } else {
                    LOGGER.info("Table " + hbaseTableName + " allready exists in HBase. Probably a case of replaying the binlog.");
                }
            }
            knownHBaseTables.put(hbaseTableName,1);

        } catch (IOException e) {
            LOGGER.info("Failed to create table in HBase.");
            // TODO: wait and retry if failed. After a while set status of applier
            // to 'blocked' & handle by overseer by stopping the replicator
            e.printStackTrace();
        }
    }

    public boolean isTableKnownToHBase(String tableName) {
        return knownHBaseTables.get(tableName) != null;
    }

    public void writeSchemaSnapshotToHBase(AugmentedSchemaChangeEvent e, com.booking.replication.Configuration configuration) {

        // get database_name
        String mySQLDBName = configuration.getReplicantSchemaName();

        // get sql_statement
        String ddl = e.getSchemaTransitionSequence().get("ddl");
        if (ddl == null) {
            LOGGER.error("DDL can not be null");
            System.exit(-1);
        }

        // get pre/post schemas
        String preChangeTablesSchemaJSON  = e.getPreTransitionSchemaSnapshot().getSchemaVersionTables_JSONSnaphot();
        String postChangeTablesSchemaJSON = e.getPostTransitionSchemaSnapshot().getSchemaVersionTables_JSONSnaphot();
        String schemaTransitionSequenceJSON = JSONBuilder.schemaTransitionSequenceToJSON(
                e.getSchemaTransitionSequence()
        );

        // get pre/post creates
        String preChangeCreateStatementsJSON  = e.getPreTransitionSchemaSnapshot().getSchemaVersionCreateStatements_JSONSnapshot();
        String postChangeCreateStatementsJSON = e.getPostTransitionSchemaSnapshot().getSchemaVersionCreateStatements_JSONSnapshot();

        // get event timestamp
        Long eventTimestamp = e.getSchemaChangeEventTimestamp();

        String hbaseTableName = "schema_history:" + mySQLDBName.toLowerCase();
        int shard = configuration.getReplicantShardID();
        if (shard > 0) {
            hbaseTableName += shard;
        }

        String hbaseRowKey = eventTimestamp.toString();
        if (configuration.isInitialSnapshotMode()) {
            // in initial-snapshot mode timestamp is overridden by 0 so all create statements
            // fall under the same timestamp. This is ok since there should be only one schema
            // snapshot for the initial-snapshot. However, having key=0 is not good, so replace
            // it with:
            hbaseRowKey = "initial-snapshot";
        }

        try {

            if (connection == null) {
                connection = ConnectionFactory.createConnection(hbaseConf);
            }

            Admin admin = connection.getAdmin();

            TableName TABLE_NAME = TableName.valueOf(hbaseTableName);

            if (!admin.tableExists(TABLE_NAME)) {

                LOGGER.info("table " + hbaseTableName + " does not exist in HBase. Creating...");

                HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
                HColumnDescriptor cd = new HColumnDescriptor("d");
                cd.setMaxVersions(DEFAULT_SCHEMA_VERSIONS);
                tableDescriptor.addFamily(cd);

                // pre-split into 16 regions
                RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                byte[][] splitKeys = splitter.split(SCHEMA_HISTORY_TABLE_DEFAULT_REGIONS);

                admin.createTable(tableDescriptor, splitKeys);
            }
            else {
                LOGGER.info("Table " + hbaseTableName + " already exists in HBase. Probably a case of replaying the binlog.");
            }

            Table hbaseTable = connection.getTable(TABLE_NAME);

            // write schema info
            Put p = new Put(Bytes.toBytes(hbaseRowKey));
            String ddlColumnName  = "ddl";
            p.addColumn(
                    CF,
                    Bytes.toBytes(ddlColumnName),
                    eventTimestamp,
                    Bytes.toBytes(schemaTransitionSequenceJSON)
            );

            String schemaSnapshotPreColumnName  = "schemaPreChange";
            p.addColumn(
                    CF,
                    Bytes.toBytes(schemaSnapshotPreColumnName),
                    eventTimestamp,
                    Bytes.toBytes(preChangeTablesSchemaJSON)
            );

            String schemaSnapshotPostColumnName = "schemaPostChange";
            p.addColumn(
                    CF,
                    Bytes.toBytes(schemaSnapshotPostColumnName),
                    eventTimestamp,
                    Bytes.toBytes(postChangeTablesSchemaJSON)
            );

            String preChangeCreateStatementsColumn = "createsPreChange";
            p.addColumn(
                    CF,
                    Bytes.toBytes(preChangeCreateStatementsColumn),
                    eventTimestamp,
                    Bytes.toBytes(preChangeCreateStatementsJSON)
            );

            String postChangeCreateStatementsColumn = "createsPostChange";
            p.addColumn(
                    CF,
                    Bytes.toBytes(postChangeCreateStatementsColumn),
                    eventTimestamp,
                    Bytes.toBytes(postChangeCreateStatementsJSON)
            );

            hbaseTable.put(p);

        } catch (IOException ioe) {
            LOGGER.error("Failed to store schemaChangePointSnapshot in HBase.", ioe);
            // TODO: add wait and retry.
            System.exit(-1);
        }
    }
}
