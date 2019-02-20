package com.booking.replication.augmenter.model.deserializer;

import com.booking.replication.augmenter.model.format.Stringifier;
import com.booking.replication.augmenter.model.row.RowBeforeAfter;
import com.booking.replication.augmenter.model.schema.ColumnSchema;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowValueDeserializer {
    public static Map<String, Object> deserializeRowCellValues(String eventType, List<ColumnSchema> columns, BitSet includedColumns, RowBeforeAfter row, Map<String, String[]> cache) {
        Map<String, Object> cellValues = new HashMap<>();

        if (columns != null) {

            if (eventType.equals("INSERT")) {

                Serializable[] rowByteSlicesForInsert = row.getAfter().get();

                for (int columnIndex = 0, rowIndex = 0; columnIndex < columns.size() && rowIndex < rowByteSlicesForInsert.length; columnIndex++) {

                    if (includedColumns.get(columnIndex)) {

                        ColumnSchema column = columns.get(columnIndex);

                        String columnName = column.getName();

                        Serializable cellValue = rowByteSlicesForInsert[rowIndex++];
                        String collation = column.getCollation();

                        Object deserializedCellValue = CellValueDeserializer.deserialize(cache, column, cellValue, collation);

                        cellValues.put(columnName, deserializedCellValue);

                    }
                }
            } else if (eventType.equals("UPDATE")) {

                Serializable[] rowByteSlicesForUpdateAfter = row.getAfter().get();

                // Here we assume the included columns before are also included after. It should always be the case for update statement.
                for (int columnIndex = 0, sliceIndex = 0; columnIndex < columns.size() && sliceIndex < rowByteSlicesForUpdateAfter.length; columnIndex++) {

                    if (includedColumns.get(columnIndex)) {

                        ColumnSchema column = columns.get(columnIndex);

                        String columnName = column.getName();
                        String columnType = column.getType().toLowerCase();

                        String collation = column.getCollation();

                        Serializable cellValueAfter = rowByteSlicesForUpdateAfter[sliceIndex];

                        Object deserializedCellValue = CellValueDeserializer.deserialize(cache, column, cellValueAfter, collation);
                        cellValues.put(columnName, deserializedCellValue);
                        sliceIndex++;
                    }
                }
            } else if (eventType.equals("DELETE")) {

                Serializable[] rowByteSlicesForDelete = row.getBefore().get();

                for (int columnIndex = 0, rowIndex = 0; columnIndex < columns.size() && rowIndex < rowByteSlicesForDelete.length; columnIndex++) {

                    if (includedColumns.get(columnIndex)) {

                        ColumnSchema column = columns.get(columnIndex);

                        String columnName = column.getName();
                        String columnType = column.getType().toLowerCase();

                        Serializable cellValue = rowByteSlicesForDelete[rowIndex++];
                        String collation = column.getCollation();

                        Object deserializedCellValue = CellValueDeserializer.deserialize(cache, column, cellValue, collation);
                        cellValues.put(columnName, deserializedCellValue);
                    }
                }
            } else {
                throw new RuntimeException("Invalid event type in stringifier: " + eventType);
            }
        } else {
            throw new RuntimeException("Invalid data. Columns list cannot be null!");
        }
        return cellValues;
    }
}
