package com.booking.replication.applier.hbase.schema;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.google.common.base.Joiner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HBaseRowKeyMapper {

    public static final String DIGEST_ALGORITHM             = "MD5";
    private MessageDigest md;

    private static final Logger LOG = LogManager.getLogger(HBaseRowKeyMapper.class);

    public static String getSaltedHBaseRowKey(AugmentedRow row) {

        // RowID
        // This is sorted by column OP (from information schema)
        List<String> pkColumnNames  = row.getPrimaryKeyColumns();
        List<String> pkColumnValues = new ArrayList<>();

        for (String pkColumnName : pkColumnNames) {

            Map<String, String> pkCell = row.getStringifiedRowColumns().get(pkColumnName);

            switch (row.getEventType()) {

                case "INSERT":
                    pkColumnValues.add(pkCell.get("value"));
                    break;

                case "DELETE":
                    pkColumnValues.add(pkCell.get("value_before"));
                    break;

                case "UPDATE":
                    pkColumnValues.add(pkCell.get("value_after"));
                    break;

                default:
                    throw new RuntimeException("Wrong event type. Expected RowType event.");
            }
        }

        if (pkColumnValues.stream().filter(v -> v != null).collect(Collectors.toList()).isEmpty()) {
            throw new RuntimeException("Tables without primary key are not allowed");
        }

        String hbaseRowID = Joiner.on(";").join(pkColumnValues);
        String saltingPartOfKey = pkColumnValues.get(0);

        // avoid region hot-spotting
        hbaseRowID = saltRowKey(hbaseRowID, saltingPartOfKey);
        return hbaseRowID;
    }

    public static String getPayloadTableHBaseRowKey(AugmentedRow row) {
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
        MessageDigest md;

        try {
            md = MessageDigest.getInstance(HBaseRowKeyMapper.DIGEST_ALGORITHM);
        } catch( NoSuchAlgorithmException e ) {
            LOG.error("No MD5 Algorithm found", e);
            throw new RuntimeException("No MD5 Algorithm found");
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
