package com.booking.replication.schema;

import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.util.JsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
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

    private static boolean DRY_RUN;

    private static final byte[] CF = Bytes.toBytes("d");

    public HBaseSchemaManager(String zookeeperQuorum, boolean dryRun) {

        DRY_RUN = dryRun;

        hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum);

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
                TableName tableName = TableName.valueOf(hbaseTableName);

                if (!admin.tableExists(tableName)) {

                    LOGGER.info("table " + hbaseTableName + " does not exist in HBase. Creating...");

                    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                    HColumnDescriptor cd = new HColumnDescriptor("d");
                    cd.setCompressionType(Compression.Algorithm.SNAPPY);
                    cd.setMaxVersions(versions);
                    tableDescriptor.addFamily(cd);
                    tableDescriptor.setCompactionEnabled(true);

                    // presplit into 16 regions
                    RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                    byte[][] splitKeys = splitter.split(MIRRORED_TABLE_DEFAULT_REGIONS);

                    admin.createTable(tableDescriptor, splitKeys);
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
                TableName tableName = TableName.valueOf(hbaseTableName);

                if (!admin.tableExists(tableName)) {

                    LOGGER.info("table " + hbaseTableName + " does not exist in HBase. Creating...");

                    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                    HColumnDescriptor cd = new HColumnDescriptor("d");
                    cd.setCompressionType(Compression.Algorithm.SNAPPY);
                    cd.setMaxVersions(DELTA_TABLE_MAX_VERSIONS);
                    tableDescriptor.addFamily(cd);
                    tableDescriptor.setCompactionEnabled(true);

                    // if daily table pre-split to 16 regions;
                    // if initial snapshot pre-split to 256 regions
                    /*if (isInitialSnapshotMode) {
                        RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                        byte[][] splitKeys = splitter.split(INITIAL_SNAPSHOT_DEFAULT_REGIONS);
                        admin.createTable(tableDescriptor, splitKeys);
                    } else {
                        RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                        byte[][] splitKeys = splitter.split(DAILY_DELTA_TABLE_DEFAULT_REGIONS);
                        admin.createTable(tableDescriptor, splitKeys);
                    }*/

                    admin.createTable(tableDescriptor);
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

    public void writeSchemaSnapshotToHBase(
            AugmentedSchemaChangeEvent event,
            com.booking.replication.Configuration configuration) {

        // get database_name
        String mySqlDbName = configuration.getReplicantSchemaName();

        // get sql_statement
        String ddl = event.getSchemaTransitionSequence().get("ddl");
        if (ddl == null) {
            LOGGER.error("DDL can not be null");
            System.exit(-1);
        }

        // get pre/post schemas
        String preChangeTablesSchemaJson  = event.getPreTransitionSchemaSnapshot().getSchemaVersionTablesJsonSnaphot();
        String postChangeTablesSchemaJson = event.getPostTransitionSchemaSnapshot().getSchemaVersionTablesJsonSnaphot();
        String schemaTransitionSequenceJson = JsonBuilder.schemaTransitionSequenceToJson(
                event.getSchemaTransitionSequence()
        );

        // get pre/post creates
        String preChangeCreateStatementsJson  = event
                .getPreTransitionSchemaSnapshot()
                .getSchemaVersionCreateStatementsJsonSnapshot();
        String postChangeCreateStatementsJson = event
                .getPostTransitionSchemaSnapshot()
                .getSchemaVersionCreateStatementsJsonSnapshot();

        // get event timestamp
        Long eventTimestamp = event.getSchemaChangeEventTimestamp();

        String hbaseTableName = TableNameMapper.getSchemaHistoryHBaseTableName(configuration);

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

            TableName tableName = TableName.valueOf(hbaseTableName);

            if (!admin.tableExists(tableName)) {

                LOGGER.info("table " + hbaseTableName + " does not exist in HBase. Creating...");

                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor cd = new HColumnDescriptor("d");
                cd.setMaxVersions(DEFAULT_SCHEMA_VERSIONS);
                tableDescriptor.addFamily(cd);
                tableDescriptor.setCompactionEnabled(true);

                // pre-split into 16 regions
                // RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                // byte[][] splitKeys = splitter.split(SCHEMA_HISTORY_TABLE_DEFAULT_REGIONS);

                admin.createTable(tableDescriptor); // , splitKeys);
            } else {
                LOGGER.info("Table " + hbaseTableName + " already exists in HBase. Probably a case of replaying the binlog.");
            }

            // write schema info
            Put put = new Put(Bytes.toBytes(hbaseRowKey));
            String ddlColumnName  = "ddl";
            put.addColumn(
                    CF,
                    Bytes.toBytes(ddlColumnName),
                    eventTimestamp,
                    Bytes.toBytes(schemaTransitionSequenceJson)
            );

            String schemaSnapshotPreColumnName  = "schemaPreChange";
            put.addColumn(
                    CF,
                    Bytes.toBytes(schemaSnapshotPreColumnName),
                    eventTimestamp,
                    Bytes.toBytes(preChangeTablesSchemaJson)
            );

            String schemaSnapshotPostColumnName = "schemaPostChange";
            put.addColumn(
                    CF,
                    Bytes.toBytes(schemaSnapshotPostColumnName),
                    eventTimestamp,
                    Bytes.toBytes(postChangeTablesSchemaJson)
            );

            String preChangeCreateStatementsColumn = "createsPreChange";
            put.addColumn(
                    CF,
                    Bytes.toBytes(preChangeCreateStatementsColumn),
                    eventTimestamp,
                    Bytes.toBytes(preChangeCreateStatementsJson)
            );

            String postChangeCreateStatementsColumn = "createsPostChange";
            put.addColumn(
                    CF,
                    Bytes.toBytes(postChangeCreateStatementsColumn),
                    eventTimestamp,
                    Bytes.toBytes(postChangeCreateStatementsJson)
            );

            Table hbaseTable = connection.getTable(tableName);
            hbaseTable.put(put);

        } catch (IOException ioe) {
            LOGGER.error("Failed to store schemaChangePointSnapshot in HBase.", ioe);
            // TODO: add wait and retry.
            System.exit(-1);
        }
    }
}
