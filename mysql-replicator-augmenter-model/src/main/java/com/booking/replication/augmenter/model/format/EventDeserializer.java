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

    public static Map<String, Map<String, Object>> getDeserializeCellValues(
            AugmentedEventType eventType,
            List<ColumnSchema> columns,
            BitSet includedColumns,
            RowBeforeAfter row,
            Map<String, String[]> cache) {

        Map<String, Map<String, Object>> deserializeCellValues = new HashMap<>();

        if (columns != null) {

            switch (eventType) {
                case INSERT: {

                    Serializable[] rowByteSlicesForInsert = row.getAfter().get();

                    addToDeserializeCellValues(deserializeCellValues, columns, includedColumns, cache,
                            new Serializable[][]{rowByteSlicesForInsert}, new String[]{"value"});

                    break;
                }
                case UPDATE: {

                    Serializable[] rowByteSlicesForUpdateBefore = row.getBefore().get();
                    Serializable[] rowByteSlicesForUpdateAfter  = row.getAfter().get();

                    addToDeserializeCellValues(deserializeCellValues, columns, includedColumns, cache,
                            new Serializable[][]{rowByteSlicesForUpdateBefore, rowByteSlicesForUpdateAfter},
                            new String[]{"value_before", "value_after"});

                    break;

                }
                case DELETE: {

                    Serializable[] rowByteSlicesForDelete = row.getBefore().get();

                    addToDeserializeCellValues(deserializeCellValues, columns, includedColumns, cache,
                            new Serializable[][]{rowByteSlicesForDelete}, new String[]{"value"});

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

    private static void addToDeserializeCellValues(Map<String, Map<String, Object>> deserializeCellValues,
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

                deserializeCellValues.put(columnName, new HashMap<>());

                for (int i = 0; i < rowByteSlices.length; ++i) {
                    Serializable cellValue = rowByteSlices[i][rowIndex];

                    Object deserializeValue = MysqlTypeDeserializer.convertToObject(cellValue, column, cache.get(columnType));

                    deserializeCellValues.get(columnName).put(values[i], deserializeValue);
                }

                rowIndex++;
            }
        }
    }
}
