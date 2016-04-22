package com.booking.replication.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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

    private static Map<String, Integer> knownHBaseTables = new HashMap<String,Integer>();

    private static Connection connection;

    private static final int DELTA_TABLE_MAX_VERSIONS = 1;

    private static final int INITIAL_SNAPSHOT_DEFAULT_REGIONS = 256;

    private static final int DAILY_DELTA_TABLE_DEFAULT_REGIONS = 16;

    private static final int MIRRORED_TABLE_DEFAULT_REGIONS = 16;

    public HBaseSchemaManager(String ZOOKEEPER_QUORUM) {

        hbaseConf.set("hbase.zookeeper.quorum",ZOOKEEPER_QUORUM);

        try {
            connection = ConnectionFactory.createConnection(hbaseConf);
            LOGGER.info("HBaseSchemaManager successfully established connection to HBase.");
        } catch (IOException e) {
            LOGGER.error("HBaseSchemaManager could not connect to HBase");
            e.printStackTrace();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaManager.class);

    public void createMirroredTableIfNotExists(String hbaseTableName, Integer versions)  {

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
                cd.setMaxVersions(versions);
                tableDescriptor.addFamily(cd);

                // presplit into 16 regions
                RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                byte[][] splitKeys = splitter.split(MIRRORED_TABLE_DEFAULT_REGIONS);

                admin.createTable(tableDescriptor, splitKeys);
            }
            else {
                LOGGER.info("Table " + hbaseTableName + " allready exists in HBase. Probably a case of replaying the binlog.");
            }

            knownHBaseTables.put(hbaseTableName,1);

        } catch (IOException e) {
            LOGGER.info("Failed to create table in HBase.");
            // TODO: wait and retry if failed. After a while set status of applier
            // to 'blocked' & handle by overseer by stopping the replicator
            e.printStackTrace();
        }
    }

    public void createDeltaTableIfNotExists(String hbaseTableName, boolean isInitialSnapshotMode)  {

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
                cd.setMaxVersions(DELTA_TABLE_MAX_VERSIONS);
                tableDescriptor.addFamily(cd);

                // if daily table pre-split to 16 regions;
                // if initial snapshot pre-split to 256 regions
                if (isInitialSnapshotMode) {
                    RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                    byte[][] splitKeys = splitter.split(INITIAL_SNAPSHOT_DEFAULT_REGIONS);
                    admin.createTable(tableDescriptor, splitKeys);
                }
                else {
                    RegionSplitter.HexStringSplit splitter = new RegionSplitter.HexStringSplit();
                    byte[][] splitKeys = splitter.split(DAILY_DELTA_TABLE_DEFAULT_REGIONS);
                    admin.createTable(tableDescriptor, splitKeys);
                }
            }
            else {
                LOGGER.info("Table " + hbaseTableName + " allready exists in HBase. Probably a case of replaying the binlog.");
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
        if (knownHBaseTables.get(tableName) != null) {
            return true;
        }
        else {
            return false;
        }
    }
}
