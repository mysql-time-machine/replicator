
package com.booking.replication.applier.hbase.schema;

import com.booking.replication.applier.hbase.HBaseApplier;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by bosko on 3/29/16.
 */
public class TableNameMapper {

    public static String getSchemaSnapshotHistoryHBaseTableName(Map<String, Object> configuration) {
        String schemaHistoryTableName =
                configuration.get(HBaseApplier.Configuration.SCHEMA_HISTORY_NAMESPACE)
                        + ":" +
                        configuration.get(HBaseApplier.Configuration.REPLICATED_SCHEMA_NAME).toString().toLowerCase();
        // TODO: make schema history namespace configurable
        return schemaHistoryTableName + "_schema_history";
    }
}
