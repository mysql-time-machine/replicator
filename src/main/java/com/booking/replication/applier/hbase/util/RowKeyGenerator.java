package com.booking.replication.applier.hbase.util;

import com.booking.replication.augmenter.AugmentedRow;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by bosko on 1/5/17.
 */
public class RowKeyGenerator {

    private static final String DIGEST_ALGORITHM             = "MD5";

    private static final Logger LOGGER = LoggerFactory.getLogger(RowKeyGenerator.class);

    public static String getSaltedHBaseRowKey(AugmentedRow row) {

        List<String> pkColumnValues = getPKColumnValues(row);

        String hbaseRowID = getNonSaltedHBaseRowKeyPart(row);

        String saltingPartOfKey = pkColumnValues.get(0);

        // avoid region hot-spotting
        hbaseRowID = RowKeyGenerator.saltRowKey(hbaseRowID, saltingPartOfKey);

        return hbaseRowID;
    }

    public static List<String> getPKColumnValues(AugmentedRow row) {

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
        return pkColumnValues;
    }

    public static String getNonSaltedHBaseRowKeyPart(AugmentedRow row) {

        // This is sorted by column OP (from information schema)
        List<String> pkColumnNames  = row.getPrimaryKeyColumns();

        List<String> pkColumnValues =getPKColumnValues(row);

        String nonSaltedPartOfHBaseRowID = Joiner.on(";").join(pkColumnValues);

        return nonSaltedPartOfHBaseRowID;
    }

    /**
     * Salting the row keys with hex representation of first two bytes of md5.
     *
     * <p>hbaseRowID = md5(hbaseRowID)[0] + md5(hbaseRowID)[1] + "-" + hbaseRowID;</p>
     */
    public static String saltRowKey(String hbaseRowID, String firstPartOfRowKey) {

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
