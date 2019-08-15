package com.booking.replication.augmenter.model.format;

import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.row.RowBeforeAfter;
import com.booking.replication.augmenter.model.schema.ColumnSchema;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Stringifier {

    public static Map<String, Map<String, String>> stringifyRowCellsValues(
            AugmentedEventType eventType,
            List<ColumnSchema> columns,
            BitSet includedColumns,
            RowBeforeAfter row,
            Map<String, String[]> cache) {

        Map<String, Map<String, String>> stringifiedCellValues = new HashMap<>();

        if (columns != null) {

            switch (eventType) {
                case INSERT: {

                    Serializable[] rowByteSlicesForInsert = row.getAfter().get();

                    addToStringifiedCellValues(stringifiedCellValues, columns, includedColumns, cache,
                            new Serializable[][]{rowByteSlicesForInsert}, new String[]{"value"});

                    break;
                }
                case UPDATE: {

                    Serializable[] rowByteSlicesForUpdateBefore = row.getBefore().get();
                    Serializable[] rowByteSlicesForUpdateAfter  = row.getAfter().get();

                    addToStringifiedCellValues(stringifiedCellValues, columns, includedColumns, cache,
                            new Serializable[][]{rowByteSlicesForUpdateBefore, rowByteSlicesForUpdateAfter},
                            new String[]{"value_before", "value_after"});

                    break;

                }
                case DELETE: {

                    Serializable[] rowByteSlicesForDelete = row.getBefore().get();

                    addToStringifiedCellValues(stringifiedCellValues, columns, includedColumns, cache,
                            new Serializable[][]{rowByteSlicesForDelete}, new String[]{"value"});

                    break;

                }
                default: {
                    throw new RuntimeException("Invalid event type in stringifier: " + eventType);
                }
            }
        } else {
            throw new RuntimeException("Invalid data. Columns list cannot be null!");
        }
        return stringifiedCellValues;
    }

    private static void addToStringifiedCellValues(Map<String, Map<String, String>> stringifiedCellValues,
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

                stringifiedCellValues.put(columnName, new HashMap<>());

                for (int i = 0; i < rowByteSlices.length; ++i) {
                    Serializable cellValue = rowByteSlices[i][rowIndex];

                    String stringifiedCellValue = MysqlTypeStringifier.convertToString(cellValue, column, cache.get(columnType));

                    stringifiedCellValues.get(columnName).put(values[i], stringifiedCellValue);
                }

                rowIndex++;
            }
        }
    }
}
