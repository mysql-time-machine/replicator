package com.booking.replication.augmenter.model.format;

import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.row.RowBeforeAfter;
import com.booking.replication.augmenter.model.schema.ColumnSchema;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventDeserializer {

    public interface Constants {
        String VALUE_BEFORE = "b";
        String VALUE_AFTER  = "a";
    }

    public static Map<String, Object> getDeserializeCellValues(
            AugmentedEventType eventType,
            List<ColumnSchema> columns,
            BitSet includedColumns,
            RowBeforeAfter row,
            Map<String, String[]> cache) {

        Map<String, Object> deserializeCellValues = new HashMap<>();

        if (columns != null) {
            // require binlog_row_image=full at all times
            // if during replication this config is changed, this condition will
            // detect this and replicator will exit.
            if (includedColumns.length() != columns.size()) {
                throw new RuntimeException("Severe environment error: there is a mismatch between the number of columns received and the schema cache, this can be caused in the event that replication began in the middle of an OSC or similar, or more severely in the event that binlog_row_image variable is not set to FULL. Data integrity cannot be guaranteed, discontinuing replication. includedColumns.length() = " + includedColumns.length() + ", columns.size() = " + columns.size());
            }

            switch (eventType) {
                case INSERT:
                case DELETE: {
                    Serializable[] rowByteSlices = (eventType == AugmentedEventType.INSERT)
                            ? row.getAfter().get()
                            : row.getBefore().get();

                    addToDeserializeCellValues(deserializeCellValues, columns, includedColumns, cache, rowByteSlices);
                    break;
                }
                case UPDATE: {

                    Serializable[] rowByteSlicesForUpdateBefore = row.getBefore().get();
                    Serializable[] rowByteSlicesForUpdateAfter  = row.getAfter().get();

                    addToDeserializeCellValues(deserializeCellValues, columns, includedColumns, cache,
                            new Serializable[][]{rowByteSlicesForUpdateBefore, rowByteSlicesForUpdateAfter},
                            new String[]{Constants.VALUE_BEFORE, Constants.VALUE_AFTER});

                    break;

                }
                default: {
                    throw new RuntimeException("Invalid event type in deserializer: " + eventType);
                }
            }
        } else {
            throw new RuntimeException("Invalid data. Columns list cannot be null!");
        }
        return deserializeCellValues;
    }

    private static void addToDeserializeCellValues(Map<String, Object> deserializeCellValues,
                                                   List<ColumnSchema> columns,
                                                   BitSet includedColumns,
                                                   Map<String, String[]> cache,
                                                   Serializable[] rowByteSlices) {
        for (int columnIndex = 0, rowIndex = 0; columnIndex < columns.size() && rowIndex < rowByteSlices.length; columnIndex++) {

            if (includedColumns.get(columnIndex)) {

                ColumnSchema column = columns.get(columnIndex);

                String columnName = column.getName();
                String columnType = column.getColumnType().toLowerCase();

                Serializable cellValue = rowByteSlices[rowIndex];
                Object deserializeValue = MysqlTypeDeserializer.convertToObject(cellValue, column, cache.get(columnType));
                deserializeCellValues.put(columnName, deserializeValue);

                rowIndex++;
            }
        }
    }

    private static void addToDeserializeCellValues(Map<String, Object> deserializeCellValues,
                                                   List<ColumnSchema> columns,
                                                   BitSet includedColumns,
                                                   Map<String, String[]> cache,
                                                   Serializable[][] rowByteSlices,
                                                   String[] values) {

        Serializable[] firstRowByteSlices = rowByteSlices[0];

        for (int columnIndex = 0, rowIndex = 0; columnIndex < columns.size() && rowIndex < firstRowByteSlices.length; columnIndex++) {

            if (includedColumns.get(columnIndex)) {

                ColumnSchema column = columns.get(columnIndex);

                String columnName = column.getName();
                String columnType = column.getColumnType().toLowerCase();

                Map<String, Object> columnUpdates = new HashMap<>();

                deserializeCellValues.put(columnName, columnUpdates);

                for (int i = 0; i < rowByteSlices.length; ++i) {
                    Serializable cellValue = rowByteSlices[i][rowIndex];

                    Object deserializeValue = MysqlTypeDeserializer.convertToObject(cellValue, column, cache.get(columnType));

                    columnUpdates.put(values[i], deserializeValue);
                }

                rowIndex++;
            }
        }
    }
}
