package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.queues.ReplicatorQueues;
import com.booking.replication.schema.TableNameMapper;

import com.google.common.base.Joiner;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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

    public List<Put> generateMutationsFromAugmentedRows(List<AugmentedRow> augmentedRows) {

        String  replicantSchema = configuration.getReplicantSchemaName();

        List<Put> preparedMutations = new ArrayList<>();

        for (AugmentedRow row : augmentedRows) {

            // ==============================================================================
            // I. Mirrored table

            // get table_name from event
            String mySQLTableName = row.getTableName();
            Pair<String,Put> rowIDPutPair = getPutForMirroredTable(row);

            Put p = rowIDPutPair.getSecond();

            preparedMutations.add(p);

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

            if (configuration.isWriteRecentChangesToDeltaTables()
                    && tablesForDelta.contains(mySQLTableName)) {

                boolean isInitialSnapshot = configuration.isInitialSnapshotMode();
                String  mysqlTableName    = row.getTableName();
                Long    timestampMicroSec = row.getEventV4Header().getTimestamp();

                String deltaTableName = TableNameMapper.getCurrentDeltaTableName(
                        timestampMicroSec,
                        replicantSchema,
                        mysqlTableName,
                        isInitialSnapshot
                );

                Pair<String, Put> deltaPair = getPutForDeltaTable(row);
                // String deltaRowID           = deltaPair.getFirst(); // todo <- refactor and remove
                Put deltaPut                = deltaPair.getSecond();

                preparedMutations.add(deltaPut);
            }

        } // next row

        return preparedMutations;
    }

    private Pair<String,Put> getPutForMirroredTable(AugmentedRow row) {

        // RowID
        String hbaseRowID = getHBaseRowKey(row);

        Put p = new Put(Bytes.toBytes(hbaseRowID));

        if (row.getEventType().equals("DELETE")) {

            // No need to process columns on DELETE. Only write delete marker.

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnName  = "row_status";
            String columnValue = "D";
            p.addColumn(
                    CF,
                    Bytes.toBytes(columnName),
                    columnTimestamp,
                    Bytes.toBytes(columnValue)
            );
        }
        else if (row.getEventType().equals("UPDATE")) {

            // Only write values that have changed

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnValue;

            for (String columnName : row.getEventColumns().keySet()) {

                String valueBefore = row.getEventColumns().get(columnName).get("value_before");
                String valueAfter  = row.getEventColumns().get(columnName).get("value_after");

                if ((valueAfter == null) && (valueBefore == null)) {
                    // no change, skip;
                }
                else if (
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
                }
                else {
                    // no change, skip
                }
            }

            p.addColumn(
                    CF,
                    Bytes.toBytes("row_status"),
                    columnTimestamp,
                    Bytes.toBytes("U")
            );
        }
        else if (row.getEventType().equals("INSERT")) {

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
        }
        else {
            LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
            System.exit(1);
        }

        Pair<String,Put> idPut = new Pair<>(hbaseRowID,p);
        return idPut;
    }

    private Pair<String,Put> getPutForDeltaTable(AugmentedRow row) {

        String hbaseRowID = getHBaseRowKey(row);

        Put p = new Put(Bytes.toBytes(hbaseRowID));

        if (row.getEventType().equals("DELETE")) {

            // For delta tables in case of DELETE, just write a delete marker

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnName  = "row_status";
            String columnValue = "D";
            p.addColumn(
                    CF,
                    Bytes.toBytes(columnName),
                    columnTimestamp,
                    Bytes.toBytes(columnValue)
            );
        }
        else if (row.getEventType().equals("UPDATE")) {

            // for delta tables write the latest version of the entire row

            Long columnTimestamp = row.getEventV4Header().getTimestamp();
            String columnValue;

            for (String columnName : row.getEventColumns().keySet()) {

                String valueAfter  = row.getEventColumns().get(columnName).get("value_after");

                columnValue = valueAfter;
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
                    Bytes.toBytes("U")
            );
        }
        else if (row.getEventType().equals("INSERT")) {

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
        }
        else {
            LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
            System.exit(1);
        }

        Pair<String,Put> idPut = new Pair<>(hbaseRowID,p);
        return idPut;
    }

    public String getHBaseRowKey(AugmentedRow row) {
        // RowID
        List<String> pkColumnNames  = row.getPrimaryKeyColumns(); // <- this is sorted by column OP (from information schema)
        List<String> pkColumnValues = new ArrayList<String>();

        for (String pkColumnName : pkColumnNames) {

            Map<String, String> pkCell = row.getEventColumns().get(pkColumnName);

            if (row.getEventType().equals("INSERT") || row.getEventType().equals("DELETE")) {
                pkColumnValues.add(pkCell.get("value"));
            } else if (row.getEventType().equals("UPDATE")) {
                pkColumnValues.add(pkCell.get("value_after"));
            } else {
                LOGGER.error("Wrong event type. Expected RowType event.");
                // TODO: throw WrongEventTypeException
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
    private String saltRowKey(String hbaseRowID, String firstPartOfRowKey) {

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

        String saltedRowKey = salt + ";" + hbaseRowID;

        return saltedRowKey;
    }
}
