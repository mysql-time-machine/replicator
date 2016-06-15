package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.queues.ReplicatorQueues;
import com.booking.replication.schema.TableNameMapper;

import com.google.common.base.Joiner;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import org.apache.hadoop.hbase.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by bosko on 4/18/16.
 */
public class HBaseApplierMutationGenerator {

    private static final byte[] CF                           = Bytes.toBytes("d");
    private static final int    MUTATION_GENERATOR_POOL_SIZE = 10;
    private static final String DIGEST_ALGORITHM             = "MD5";

    private final com.booking.replication.Configuration configuration;

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplierMutationGenerator.class);

    // Constructor
    public HBaseApplierMutationGenerator(com.booking.replication.Configuration repCfg) {
        configuration = repCfg;
    }

    public HashMap<String,HashMap<String,List<Triple<String,String,Put>>>>
    generateMutationsFromAugmentedRows(List<AugmentedRow> augmentedRows) {

        // { $type => $tableName => @AugmentedMutations }
        HashMap<String,HashMap<String,List<Triple<String,String,Put>>>> preparedMutations = new HashMap<>();

        preparedMutations.put("mirrored", new HashMap<String, List<Triple<String, String, Put>>>());
        preparedMutations.put("delta", new HashMap<String, List<Triple<String, String, Put>>>());

        for (AugmentedRow row : augmentedRows) {

            // ==============================================================================
            // I. Mirrored table
            Triple<String,String,Put> mirroredTableKeyPut = getPutForMirroredTable(row);

            String mirroredTableName = mirroredTableKeyPut.getFirst();

            if (preparedMutations.get("mirrored").get(mirroredTableName) == null) {
                preparedMutations.get("mirrored").put(mirroredTableName, new ArrayList<Triple<String, String, Put>>());
            }
            preparedMutations.get("mirrored").get(mirroredTableName).add(mirroredTableKeyPut);

            // ==============================================================================
            // II. Optional Delta table used for incremental imports to Hive
            //
            // Delta tables have 2 important differences from mirrored tables:
            //
            // 1. Columns have only 1 version
            //
            // 2. we are storing the entire row (instead only the changes columns - since 1.)
            //
            List<String> tablesForDelta = configuration.getTablesForWhichToTrackDailyChanges();
            String mySQLTableName = row.getTableName();

            if (configuration.isWriteRecentChangesToDeltaTables() && tablesForDelta.contains(mySQLTableName)) {

                Triple<String,String,Put> deltaTableKeyPut = getPutForDeltaTable(row);

                String deltaTableName = deltaTableKeyPut.getFirst();
                if (preparedMutations.get("delta").get(deltaTableName) == null) {
                    preparedMutations.get("delta").put(deltaTableName, new ArrayList<Triple<String, String, Put>>());
                }
                preparedMutations.get("delta").get(deltaTableName).add(deltaTableKeyPut);
            }
        } // next row

        return preparedMutations;
    }

    private Triple<String,String,Put> getPutForMirroredTable(AugmentedRow row) {

        // RowID
        String hbaseRowID = getHBaseRowKey(row);

        String hbaseTableName =
                configuration.getHbaseNamespace() + ":" + row.getTableName().toLowerCase();

        Put p = new Put(Bytes.toBytes(hbaseRowID));

        switch (row.getEventType()) {
            case "DELETE": {

                // No need to process columns on DELETE. Only write delete marker.

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnName = "row_status";
                String columnValue = "D";
                p.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
                break;
            }
            case "UPDATE": {

                // Only write values that have changed

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnValue;

                for (String columnName : row.getEventColumns().keySet()) {

                    String valueBefore = row.getEventColumns().get(columnName).get("value_before");
                    String valueAfter = row.getEventColumns().get(columnName).get("value_after");

                    if ((valueAfter == null) && (valueBefore == null)) {
                        // no change, skip;
                    } else if (
                            ((valueBefore == null) && (valueAfter != null))
                                    ||
                                    ((valueBefore != null) && (valueAfter == null))
                                    ||
                                    (!valueAfter.equals(valueBefore))) {

                        columnValue = valueAfter;
                        p.addColumn(
                                CF,
                                Bytes.toBytes(columnName),
                                columnTimestamp,
                                Bytes.toBytes(columnValue)
                        );
                    } else {
                        // no change, skip
                    }
                }

                p.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("U")
                );
                break;
            }
            case "INSERT": {

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnValue;

                for (String columnName : row.getEventColumns().keySet()) {

                    columnValue = row.getEventColumns().get(columnName).get("value");
                    if (columnValue == null) {
                        columnValue = "NULL";
                    }

                    p.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(columnValue)
                    );
                }


                p.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("I")
                );
                break;
            }
            default:
                LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
                System.exit(1);
        }

        return new Triple<>(hbaseTableName,hbaseRowID,p);
    }

    private Triple<String,String,Put> getPutForDeltaTable(AugmentedRow row) {

        String hbaseRowID = getHBaseRowKey(row);

        // String  replicantSchema   = configuration.getReplicantSchemaName().toLowerCase();
        String  mySQLTableName    = row.getTableName();
        Long    timestampMicroSec = row.getEventV4Header().getTimestamp();
        boolean isInitialSnapshot = configuration.isInitialSnapshotMode();

        String deltaTableName = TableNameMapper.getCurrentDeltaTableName(
                timestampMicroSec,
                configuration.getHbaseNamespace(),
                mySQLTableName,
                isInitialSnapshot
        );

        Put p = new Put(Bytes.toBytes(hbaseRowID));

        switch (row.getEventType()) {
            case "DELETE": {

                // For delta tables in case of DELETE, just write a delete marker

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnName = "row_status";
                String columnValue = "D";
                p.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
                break;
            }
            case "UPDATE": {

                // for delta tables write the latest version of the entire row

                Long columnTimestamp = row.getEventV4Header().getTimestamp();

                for (String columnName : row.getEventColumns().keySet()) {
                    p.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(row.getEventColumns().get(columnName).get("value_after"))
                    );
                }

                p.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("U")
                );
                break;
            }
            case "INSERT": {

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnValue;

                for (String columnName : row.getEventColumns().keySet()) {

                    columnValue = row.getEventColumns().get(columnName).get("value");
                    if (columnValue == null) {
                        columnValue = "NULL";
                    }

                    p.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(columnValue)
                    );
                }

                p.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("I")
                );
                break;
            }
            default:
                LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
                System.exit(1);
        }

        return new Triple<>(deltaTableName,hbaseRowID,p);
    }

    public static String getHBaseRowKey(AugmentedRow row) {
        // RowID
        // This is sorted by column OP (from information schema)
        List<String> pkColumnNames  = row.getPrimaryKeyColumns();
        List<String> pkColumnValues = new ArrayList<>();

        for (String pkColumnName : pkColumnNames) {

            Map<String, String> pkCell = row.getEventColumns().get(pkColumnName);

            switch (row.getEventType()) {
                case "INSERT":
                case "DELETE":
                    pkColumnValues.add(pkCell.get("value"));
                    break;
                case "UPDATE":
                    pkColumnValues.add(pkCell.get("value_after"));
                    break;
                default:
                    LOGGER.error("Wrong event type. Expected RowType event.");
                    // TODO: throw WrongEventTypeException
                    break;
            }
        }

        String hbaseRowID = Joiner.on(";").join(pkColumnValues);
        String saltingPartOfKey = pkColumnValues.get(0);

        // avoid region hot-spotting
        hbaseRowID = saltRowKey(hbaseRowID, saltingPartOfKey);
        return hbaseRowID;
    }

    /**
     * Salting the row keys with hex representation of first two bytes of md5:
     *      hbaseRowID = md5(hbaseRowID)[0] + md5(hbaseRowID)[1] + "-" + hbaseRowID;
     */
    private static String saltRowKey(String hbaseRowID, String firstPartOfRowKey) {

        byte[] bytesOfSaltingPartOfRowKey = firstPartOfRowKey.getBytes(StandardCharsets.US_ASCII);

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            LOGGER.error("md5 algorithm not available. Shutting down...");
            System.exit(1);
        }
        byte[] bytes_md5 = md.digest(bytesOfSaltingPartOfRowKey);

        String byte_1_hex = Integer.toHexString(bytes_md5[0] & 0xFF);
        String byte_2_hex = Integer.toHexString(bytes_md5[1] & 0xFF);
        String byte_3_hex = Integer.toHexString(bytes_md5[2] & 0xFF);
        String byte_4_hex = Integer.toHexString(bytes_md5[3] & 0xFF);

        // add 0-padding
        String salt = ("00" + byte_1_hex).substring(byte_1_hex.length())
                + ("00" + byte_2_hex).substring(byte_2_hex.length())
                + ("00" + byte_3_hex).substring(byte_3_hex.length())
                + ("00" + byte_4_hex).substring(byte_4_hex.length())
                ;

        return salt + ";" + hbaseRowID;
    }
}
