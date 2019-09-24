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
