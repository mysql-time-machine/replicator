
package com.booking.replication.applier.hbase.schema;

import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.augmenter.model.row.AugmentedRow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bosko on 3/29/16.
 */
public class HBaseTableNameMapper {

    private static final Logger LOG = LogManager.getLogger(HBaseTableNameMapper.class);

    public static String getSchemaSnapshotHistoryHBaseTableName(Map<String, Object> configuration) {
        String prefix = HBaseApplier.Configuration.SCHEMA_HISTORY_NAMESPACE.equals("")
                ?  configuration.get(HBaseApplier.Configuration.SCHEMA_HISTORY_NAMESPACE) + ":"
                : "";
        String schemaHistoryTableName =
                prefix + configuration.get(HBaseApplier.Configuration.REPLICATED_SCHEMA_NAME).toString().toLowerCase();

        return schemaHistoryTableName + "_schema_history";
    }
}
