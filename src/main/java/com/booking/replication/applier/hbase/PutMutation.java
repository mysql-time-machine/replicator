package com.booking.replication.applier.hbase;

import com.booking.replication.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Created by bosko on 12/29/16.
 */
public class PutMutation {

    private static final Logger LOGGER = LoggerFactory.getLogger(PutMutation.class);

    private static final byte[] CF                           = Bytes.toBytes("d");


    private final Put put;
    private final String table;
    private final String sourceRowUri;
    private final boolean isTableMirrored;
    private final Configuration configuration;

    public boolean isSecondaryIndexTable() {
        return isSecondaryIndexTable;
    }

    private final boolean isSecondaryIndexTable;

    public PutMutation(
            Put put,
            String table,
            String sourceRowUri,
            boolean isTableMirrored,
            boolean isSecondaryIndexTable,
            Configuration configuration
    ) {
        this.put = put;
        this.sourceRowUri = sourceRowUri;
        this.table = table;
        this.isTableMirrored = isTableMirrored;
        this.isSecondaryIndexTable = isSecondaryIndexTable;
        this.configuration = configuration;
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
