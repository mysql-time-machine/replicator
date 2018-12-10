
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

    private static final String pattern = "([_][12]\\d{3}(0[1-9]|1[0-2]))";
    private static final Pattern tableNameMergePattern = Pattern.compile(pattern);

    private static final Logger LOG = LogManager.getLogger(HBaseTableNameMapper.class);

    public static String getSchemaSnapshotHistoryHBaseTableName(Map<String, Object> configuration) {
        String schemaHistoryTableName =
                configuration.get(HBaseApplier.Configuration.SCHEMA_HISTORY_NAMESPACE)
                        + ":" +
                        configuration.get(HBaseApplier.Configuration.REPLICATED_SCHEMA_NAME).toString().toLowerCase();
        // TODO: make schema history namespace configurable
        return schemaHistoryTableName + "_schema_history";
    }

    public static String getHBaseTableNameWithMergeStrategyApplied(String augmentedRowTableName, Map<String, Object> configuration) {
        String mergeStrategy = (String) configuration.get(HBaseApplier.Configuration.TABLE_MERGE_STRATEGY);
        if (mergeStrategy == null) {
            return  augmentedRowTableName;
        }

        if (mergeStrategy.equals("TABLE_NAME_AS_KEY_PREFIX")) {
            return
                   configuration.get(HBaseApplier.Configuration.TARGET_NAMESPACE)
                            + ":" +
                            configuration.get(HBaseApplier.Configuration.REPLICATED_SCHEMA_NAME).toString().toLowerCase();
        }

        if (mergeStrategy.equals("TABLE_NAME_SUFFIX_REMOVE")) {
            Matcher m = tableNameMergePattern.matcher(augmentedRowTableName);
            if (m.find()) {
                String mergedTableName = augmentedRowTableName.replaceAll(pattern, "");
                return mergedTableName;
            } else {
                return  augmentedRowTableName;
            }
        }
        LOG.warn("Failed to match table name against supported patterns. Default to base table name.");
        return augmentedRowTableName;
    }
}
