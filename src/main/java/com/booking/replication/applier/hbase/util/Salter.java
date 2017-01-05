package com.booking.replication.applier.hbase.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by bosko on 1/5/17.
 */
public class Salter {

    private static final String DIGEST_ALGORITHM             = "MD5";

    private static final Logger LOGGER = LoggerFactory.getLogger(Salter.class);

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
