package com.booking.replication.applier.hbase.schema;

import com.booking.replication.applier.hbase.HBaseApplier;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by bosko on 3/29/16.
 *
 *  <p>TODO: optional hourly tables (--delta-hourly) (currently only daily tables are available)</p>
 *
 *  <p>TODO: timezone specification option (currently all  {@literal [timestamp = YYYYMMDD]} conversions use the default
 *        timezone of the system the replicator is running on)</p>
 */
public class TableNameMapper {

    public static String getCurrentDeltaTableName(
            long    eventTimestampMicroSec,
            String  replicantNamespace,
            String  mysqlTableName,
            boolean isInitialSnapshot) {


        String suffix;

        if (isInitialSnapshot) {
            suffix = "initial";
        } else {
            long eventTimestamp = (long) eventTimestampMicroSec / 1000; // microsec => milisec

            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            sdf.setTimeZone(timeZone);
            Date resultDate = new Date(eventTimestamp);

            suffix = sdf.format(resultDate);
        }

        return "delta:" + replicantNamespace.toLowerCase() + "_" + mysqlTableName.toLowerCase() + "_" + suffix;
    }

    public static String getSchemaSnapshotHistoryHBaseTableName(Map<String, Object> configuration) {
        String schemaHistoryTableName = (String) configuration.get(HBaseApplier.Configuration.TARGET_NAMESPACE);
        // TODO: make schema history namespace configurable
        return "" + schemaHistoryTableName + "_schema_history";
    }
}
