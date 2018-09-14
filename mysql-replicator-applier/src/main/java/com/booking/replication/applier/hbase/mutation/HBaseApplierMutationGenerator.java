package com.booking.replication.applier.hbase.mutation;

import com.booking.replication.applier.hbase.HBaseApplier;

import com.booking.replication.augmenter.model.row.AugmentedRow;
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
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Created by bosko on 4/18/16.
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

        public String getTable() {
            return table;
        }

        public String getTargetRowUri() {

            // TODO: config
            // if (configuration.validationConfig == null) return null;
            // targetDomain <- configuration.getValidationConfiguration().getTargetDomain();
            String targetDomain = "hbase-cluster";
            try {

                String dataSource = targetDomain;

                String row = URLEncoder.encode(Bytes.toStringBinary(put.getRow()), "UTF-8");

                String cf = URLEncoder.encode(Bytes.toString(CF), "UTF-8");

                return String.format("hbase://%s/%s?row=%s&cf=%s", dataSource, table, row, cf);

            } catch (UnsupportedEncodingException e) {
                LOGGER.error("UTF-8 not supported?", e);
                return null;
            }
        }
    }

    private static final byte[] CF                           = Bytes.toBytes("d");
    private static final byte[] TID                          = Bytes.toBytes("_transaction_uuid");
    private static final byte[] XID                          = Bytes.toBytes("_transaction_xid");
    private static final String DIGEST_ALGORITHM             = "MD5";

    private final Map<String, Object> configuration;
    private final MessageDigest md;

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplierMutationGenerator.class);

    // Constructor
    public HBaseApplierMutationGenerator(Map<String, Object> configuration)
            throws NoSuchAlgorithmException {
        this.configuration = configuration;
        this.md = MessageDigest.getInstance(DIGEST_ALGORITHM);
    }

    /**
     * Transforms a list of {@link AugmentedRow} to a {@link PutMutation}
     * @param augmentedRow
     * @return PutMutation
     */
    public PutMutation getPutForMirroredTable(AugmentedRow augmentedRow) {

        // RowID
        String hbaseRowID = getHBaseRowKey(augmentedRow);

        String namespace = (String) configuration.get(HBaseApplier.Configuration.TARGET_NAMESPACE);
        String prefix = "";
        if (!namespace.isEmpty()) {
            prefix = namespace + ":";
        }


        String hbaseTableName = prefix.toLowerCase() + augmentedRow.getTableName().toLowerCase();

        Put put = new Put(Bytes.toBytes(hbaseRowID));
        UUID uuid = augmentedRow.getTransactionUUID();
        Long xid = augmentedRow.getTransactionXid();

        switch (augmentedRow.getEventType()) {
            case "DELETE": {

                // No need to process columns on DELETE. Only write delete marker.

                Long columnTimestamp = augmentedRow.getRowMicrosecondTimestamp();
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
                            augmentedRow.getCommitTimestamp(),
                            Bytes.toBytes(uuid.toString())
                    );
                }
                if (xid != null) {
                    put.addColumn(
                            CF,
                            XID,
                            augmentedRow.getCommitTimestamp(),
                            Bytes.toBytes(xid.toString())
                    );
                }
                break;
            }
            case "UPDATE": {

                // Only write values that have changed

                Long columnTimestamp = augmentedRow.getRowMicrosecondTimestamp();
                String columnValue;

                for (String columnName : augmentedRow.getRowColumns().keySet()) {

                    String valueBefore = augmentedRow.getRowColumns().get(columnName).get("value_before");
                    String valueAfter = augmentedRow.getRowColumns().get(columnName).get("value_after");

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
                            augmentedRow.getCommitTimestamp(),
                            Bytes.toBytes(uuid.toString())
                    );
                }

                if (xid != null) {
                    put.addColumn(
                            CF,
                            XID,
                            augmentedRow.getCommitTimestamp(),
                            Bytes.toBytes(xid.toString())
                    );
                }
                break;
            }
            case "INSERT": {

                Long columnTimestamp = augmentedRow.getRowMicrosecondTimestamp();
                String columnValue;

                for (String columnName : augmentedRow.getRowColumns().keySet()) {

                    columnValue = augmentedRow.getRowColumns().get(columnName).get("value");
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
                            augmentedRow.getCommitTimestamp(),
                            Bytes.toBytes(uuid.toString())
                    );
                }
                if (xid != null) {
                    put.addColumn(
                            CF,
                            XID,
                            augmentedRow.getCommitTimestamp(),
                            Bytes.toBytes(xid.toString())
                    );
                }
                break;
            }
            default:
                LOGGER.error("Wrong event type " + augmentedRow.getEventType() + ". Expected INSERT/UPDATE/DELETE.");
        }
        return new PutMutation(put,hbaseTableName,getRowUri(augmentedRow), true);
    }

    private String getRowUri(AugmentedRow row){

        // TODO: add validator config options
        //        validation:
        //          broker: "localhost:9092,localhost:9093"
        //          topic: replicator_validation
        //          tag: test_hbase
        //          source_domain: mysql-schema
        //          target_domain: hbase-cluster
        // if (configuration.validationConfig == null) return null;
        String sourceDomain = row.getTableSchema().toString().toLowerCase();

        String eventType = row.getEventType();

        String table = row.getTableName();

        String keys  = row.getPrimaryKeyColumns().stream()
                .map( column -> {
                    try {

                        String value = row.getRowColumns().get(column).get( "UPDATE".equals(eventType) ? "value_after" : "value" );

                        return URLEncoder.encode(column,"UTF-8") + "=" + URLEncoder.encode(value,"UTF-8");

                    } catch (UnsupportedEncodingException e) {

                        LOGGER.error("Unexpected encoding exception", e);

                        return null;

                    }
                } )
                .collect(Collectors.joining("&"));

        return String.format("mysql://%s/%s?%s", sourceDomain, table, keys  );
    }

    public String getHBaseRowKey(AugmentedRow row) {

        // RowID
        // This is sorted by column OP (from information schema)
        List<String> pkColumnNames  = row.getPrimaryKeyColumns();
        List<String> pkColumnValues = new ArrayList<>();

        for (String pkColumnName : pkColumnNames) {

            Map<String, String> pkCell = row.getRowColumns().get(pkColumnName);

            switch (row.getEventType()) {
                case "INSERT":
                case "DELETE":
                    pkColumnValues.add(pkCell.get("value"));
                    break;
                case "UPDATE":
                    pkColumnValues.add(pkCell.get("value_after"));
                    break;
                default:
                    throw new RuntimeException("Wrong event type. Expected RowType event.");
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
    private String saltRowKey(String hbaseRowID, String firstPartOfRowKey) {

        byte[] bytesOfSaltingPartOfRowKey = firstPartOfRowKey.getBytes(StandardCharsets.US_ASCII);

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
