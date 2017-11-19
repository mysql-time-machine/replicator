package com.booking.replication.applier.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by bosko on 11/19/17.
 */
public class Util {

    public static int getHashCode_HashPrimaryKeyValuesMethod(
            String eventType,
            List<String> pkColumns,
            Map<String, Map<String, String>> eventColumns) {
        int hashCode = pkColumns.stream().map((r) -> {
            return eventColumns.get(r).get(
                    eventType.equals("UPDATE") ? "value_after" : "value"
            );
        }).collect(Collectors.joining("-")).hashCode();
        return hashCode;
    }

    public static int getHashCode_HashCustomColumn(
            String eventType,
            String tableName,
            Map<String, Map<String, String>> eventColumns,
            HashMap<String, String> partitionColumns) {
        int hashCode;
        String columnName = partitionColumns.get(tableName);
        if (columnName != null) {
            Map<String, String> column = eventColumns.get(columnName);
            if (column != null) {
                hashCode = column.get(
                        eventType.equals("UPDATE") ? "value_after" : "value"
                ).hashCode();
            } else {
                hashCode = tableName.hashCode();
            }
        } else {
            hashCode = tableName.hashCode();
        }

        return hashCode;
    }
}
