package com.booking.replication.schema;

import com.google.code.or.binlog.impl.event.TableMapEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by bosko on 3/29/16.
 *
 *  TODO: optional hourly tables (--delta-hourly) (currently only daily tables are available)
 *
 *  TODO: timezone specification option (currently all [timestamp => YYYYMMDD] conversions use the default
 *        timezone of the system the replicator is running on)
 *
 */
public class TableNameMapper {

    public static String getCurrentDeltaTableName(
            long    eventTimestampMicroSec,
            String  replicantSchema,
            String  mysqlTableName,
            boolean isInitialSnapshot) {


        String suffix;

        if (isInitialSnapshot) {
            suffix = "initial";
        }
        else {
            long eventTimestamp = (long) eventTimestampMicroSec / 1000; // microsec => milisec

            String DATE_FORMAT = "yyyyMMdd";

            TimeZone timeZone = TimeZone.getDefault();
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
            sdf.setTimeZone(timeZone);
            Date resultDate = new Date(eventTimestamp);

            suffix = sdf.format(resultDate);
        }

        // TODO: read namespace from config
        String currentDeltaTable = "delta:" + replicantSchema.toLowerCase() + "_" + mysqlTableName.toLowerCase() + "_" + suffix;
        return currentDeltaTable;
    }

    public static String mysqlTableNameToHBaseTableName(String schemaName, String mysqlTableName) {
        String hbaseTableName = schemaName.toLowerCase() + ":" + mysqlTableName.toLowerCase();
        return hbaseTableName;
    }

}
