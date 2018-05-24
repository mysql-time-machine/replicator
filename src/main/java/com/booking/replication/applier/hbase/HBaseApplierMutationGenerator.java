package com.booking.replication.applier.hbase;

import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.schema.TableNameMapper;
import com.google.common.base.Joiner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by bosko on 4/18/16.
 */
public class HBaseApplierMutationGenerator {

    public class PutMutation {

        private final Put put;
        private final String table;
        private final String sourceRowUri;
        private final boolean isTableMirrored;

        public PutMutation(Put put, String table, String sourceRowUri, boolean isTableMirrored) {
            this.put = put;
            this.sourceRowUri = sourceRowUri;
            this.table = table;
            this.isTableMirrored = isTableMirrored;
        }

        public Put getPut() {
            return put;
        }

        public String getSourceRowUri() {
            return sourceRowUri;
        }

        public String getTable(){
            return table;
        }

        public String getTargetRowUri() {

            if (configuration.validationConfig == null) return null;

            // TODO: make URI generation in a right way

            try {

                String dataSource = configuration.getValidationConfiguration().getTargetDomain();

                String row = URLEncoder.encode(Bytes.toStringBinary(put.getRow()),"UTF-8");

                String cf = URLEncoder.encode(Bytes.toString(CF),"UTF-8");

                return String.format("hbase://%s/%s?row=%s&cf=%s", dataSource, table, row , cf);

            } catch (UnsupportedEncodingException e) {

                LOGGER.error("UTF-8 not supported?",e);

                return null;

            }

        }

        public boolean isTableMirrored() {
            return isTableMirrored;
        }
    }

    private static final byte[] CF                           = Bytes.toBytes("d");
    private static final byte[] TID                          = Bytes.toBytes("_transaction_uuid");
    private static final String DIGEST_ALGORITHM             = "MD5";

    private final com.booking.replication.Configuration configuration;

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplierMutationGenerator.class);

    // Constructor
    public HBaseApplierMutationGenerator(com.booking.replication.Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Transforms a list of {@link AugmentedRow} to a list of hbase mutations
     *
     * @param augmentedRows a list of augmented rows
     * @return a list of HBase mutations
     */
    public List<PutMutation> generateMutations(List<AugmentedRow> augmentedRows) {

        Set<String> tablesForDelta = configuration.getTablesForWhichToTrackDailyChanges().stream().collect(Collectors.toSet());

        boolean writeDelta = configuration.isWriteRecentChangesToDeltaTables();

        return augmentedRows.stream()
                .flatMap(
                        row -> writeDelta && tablesForDelta.contains(row.getTableName())
                                ? Stream.of(getPutForMirroredTable(row), getPutForDeltaTable(row) )
                                : Stream.of(getPutForMirroredTable(row)) )
                .collect(Collectors.toList());

    }

    private PutMutation getPutForMirroredTable(AugmentedRow row) {

        // RowID
        String hbaseRowID = getHBaseRowKey(row);
        if (configuration.getPayloadTableName() != null && configuration.getPayloadTableName().equals(row.getTableName())) {
            hbaseRowID = getPayloadTableHBaseRowKey(row);
        }

        String hbaseTableName =
                configuration.getHbaseNamespace() + ":" + row.getTableName().toLowerCase();

        Put put = new Put(Bytes.toBytes(hbaseRowID));
        UUID uuid = null;
        if (configuration.getHBaseApplyUuid()) {
            uuid = row.getTransactionUUID();
        }

        switch (row.getEventType()) {
            case "DELETE": {

                // No need to process columns on DELETE. Only write delete marker.

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnName = "row_status";
                String columnValue = "D";
                put.addColumn(
                        CF,
                        Bytes.toBytes(columnName),
                        columnTimestamp,
                        Bytes.toBytes(columnValue)
                );
                if (uuid != null) {
                    put.addColumn(
                            CF,
                            TID,
                            row.getTransactionUUIDTimestamp(),
                            Bytes.toBytes(uuid.toString())
                    );
                }
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
                        put.addColumn(
                                CF,
                                Bytes.toBytes(columnName),
                                columnTimestamp,
                                Bytes.toBytes(columnValue)
                        );
                    } else {
                        // no change, skip
                    }
                }

                put.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("U")
                );
                if (uuid != null) {
                    put.addColumn(
                            CF,
                            TID,
                            row.getTransactionUUIDTimestamp(),
                            Bytes.toBytes(uuid.toString())
                    );
                }
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

                    put.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(columnValue)
                    );
                }


                put.addColumn(
                        CF,
                        Bytes.toBytes("row_status"),
                        columnTimestamp,
                        Bytes.toBytes("I")
                );
                if (uuid != null) {
                    put.addColumn(
                            CF,
                            TID,
                            row.getTransactionUUIDTimestamp(),
                            Bytes.toBytes(uuid.toString())
                    );
                }
                break;
            }
            default:
                LOGGER.error("ERROR: Wrong event type. Expected RowType event. Shutting down...");
                System.exit(1);
        }

        return new PutMutation(put,hbaseTableName,getRowUri(row), true);

    }

    private PutMutation getPutForDeltaTable(AugmentedRow row) {

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

        Put put = new Put(Bytes.toBytes(hbaseRowID));

        switch (row.getEventType()) {
            case "DELETE": {

                // For delta tables in case of DELETE, just write a delete marker

                Long columnTimestamp = row.getEventV4Header().getTimestamp();
                String columnName = "row_status";
                String columnValue = "D";
                put.addColumn(
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
                    put.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(row.getEventColumns().get(columnName).get("value_after"))
                    );
                }

                put.addColumn(
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

                    put.addColumn(
                            CF,
                            Bytes.toBytes(columnName),
                            columnTimestamp,
                            Bytes.toBytes(columnValue)
                    );
                }

                put.addColumn(
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

        return new PutMutation(put,deltaTableName,getRowUri(row),false);

    }

    private String getRowUri(AugmentedRow row){

        if (configuration.validationConfig == null) return null;

        // TODO: generate URI in a better way

        String eventType = row.getEventType();

        String table = row.getTableName();

        String keys  = row.getPrimaryKeyColumns().stream()
                .map( column -> {
                    try {

                        String value = row.getEventColumns().get(column).get( "UPDATE".equals(eventType) ? "value_after" : "value" );

                        return URLEncoder.encode(column,"UTF-8") + "=" + URLEncoder.encode(value,"UTF-8");

                    } catch (UnsupportedEncodingException e) {

                        LOGGER.error("Unexpected encoding exception", e);

                        return null;

                    }
                } )
                .collect(Collectors.joining("&"));

        return String.format("mysql://%s/%s?%s", configuration.validationConfig.getSourceDomain(), table, keys  );
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

    private static String getPayloadTableHBaseRowKey(AugmentedRow row) {
        if (row.getTransactionUUID() != null) {
            return row.getTransactionUUID().toString();
        } else {
            throw new RuntimeException("Transaction ID missing in Augmented Row");
        }
    }

    /**
     * Salting the row keys with hex representation of first two bytes of md5.
     *
     * <p>hbaseRowID = md5(hbaseRowID)[0] + md5(hbaseRowID)[1] + "-" + hbaseRowID;</p>
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
        byte[] bytesMD5 = md.digest(bytesOfSaltingPartOfRowKey);

        String byte1hex = Integer.toHexString(bytesMD5[0] & 0xFF);
        String byte2hex = Integer.toHexString(bytesMD5[1] & 0xFF);
        String byte3hex = Integer.toHexString(bytesMD5[2] & 0xFF);
        String byte4hex = Integer.toHexString(bytesMD5[3] & 0xFF);

        // add 0-padding
        String salt = ("00" + byte1hex).substring(byte1hex.length())
                + ("00" + byte2hex).substring(byte2hex.length())
                + ("00" + byte3hex).substring(byte3hex.length())
                + ("00" + byte4hex).substring(byte4hex.length())
                ;

        return salt + ";" + hbaseRowID;
    }
}
