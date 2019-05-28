package com.booking.replication.augmenter.model.format;

import com.booking.replication.augmenter.model.row.RowBeforeAfter;
import com.booking.replication.augmenter.model.schema.ColumnSchema;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.IntStream;

public class Stringifier {

    private static final Logger LOG = LogManager.getLogger(Stringifier.class);

    public static Map<String, Map<String, String>> stringifyRowCellsValues(
            String eventType,
            List<ColumnSchema> columns,
            BitSet includedColumns,
            RowBeforeAfter row,
            Map<String, String[]> cache
        ) {

        Map<String, Map<String, String>> stringifiedCellValues = new HashMap<>();

        if (columns != null) {

            if (eventType.equals("INSERT")) {

                Serializable[] rowByteSlicesForInsert = row.getAfter().get();

                for (int columnIndex = 0, rowIndex = 0; columnIndex < columns.size() && rowIndex < rowByteSlicesForInsert.length; columnIndex++) {

                    if (includedColumns.get(columnIndex)) {

                        ColumnSchema column = columns.get(columnIndex);

                        String columnName = column.getName();
                        String columnType = column.getType().toLowerCase();

                        Serializable cellValue = rowByteSlicesForInsert[rowIndex++];
                        String collation = column.getCollation();

                        String stringifiedCellValue = Stringifier.stringifyCellValue(cache, columnType, cellValue, collation);

                        stringifiedCellValues.put(columnName, new HashMap<>());
                        stringifiedCellValues.get(columnName).put("value", stringifiedCellValue);
                    }
                }
            } else if (eventType.equals("UPDATE")) {

                Serializable[] rowByteSlicesForUpdateBefore = row.getBefore().get();
                Serializable[] rowByteSlicesForUpdateAfter = row.getAfter().get();

                // Here we assume the included columns before are also included after. It should always be the case for update statement.
                for (int columnIndex = 0, sliceIndex = 0; columnIndex < columns.size() && sliceIndex < rowByteSlicesForUpdateAfter.length; columnIndex++) {

                    if (includedColumns.get(columnIndex)) {

                        ColumnSchema column = columns.get(columnIndex);

                        String columnName = column.getName();
                        String columnType = column.getType().toLowerCase();

                        String collation = column.getCollation();

                        Serializable cellValueBefore = rowByteSlicesForUpdateBefore[sliceIndex];
                        Serializable cellValueAfter = rowByteSlicesForUpdateAfter[sliceIndex];

                        String stringifiedCellValueBefore = Stringifier.stringifyCellValue(cache, columnType, cellValueBefore, collation);
                        String stringifiedCellValueAfter = Stringifier.stringifyCellValue(cache, columnType, cellValueAfter, collation);

                        stringifiedCellValues.put(columnName, new HashMap<>());
                        stringifiedCellValues.get(columnName).put("value_before", stringifiedCellValueBefore);
                        stringifiedCellValues.get(columnName).put("value_after", stringifiedCellValueAfter);

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

                        String stringifiedCellValue = Stringifier.stringifyCellValue(cache, columnType, cellValue, collation);

                        stringifiedCellValues.put(columnName, new HashMap<>());
                        stringifiedCellValues.get(columnName).put("value_before", stringifiedCellValue);
                    }
                }
            } else {
                throw new RuntimeException("Invalid event type in stringifier: " + eventType);
            }
        }
        else {
            throw new RuntimeException("Invalid data. Columns list cannot be null!");
        }
        return stringifiedCellValues;
    }

    // todo use cellValueDeserializer
    public static String stringifyCellValue(Map<String, String[]> cache, String columnType, Serializable cellValue, String collation) {
        String stringifiedCellValue = null;

        if (cellValue == null) {
            stringifiedCellValue = "NULL"; // TODO: option for configuring null handling
        } else {
            if (collation != null && (cellValue instanceof byte[])) {
                byte[] bytes = (byte[]) cellValue;
                if (collation.contains("latin1")) {
                    stringifiedCellValue = new String(bytes, StandardCharsets.ISO_8859_1);
                } else {
                    // Currently handle all the other character set as UTF8, extend this to handle specific character sets
                    stringifiedCellValue = new String(bytes, StandardCharsets.UTF_8);
                }
                return stringifiedCellValue;
            }

            if (cellValue instanceof BitSet) {
                final BitSet data = (BitSet) cellValue;
                final StringBuilder buffer = new StringBuilder(data.length());
                IntStream.range(0, data.length()).mapToObj(i -> data.get(i) ? '1' : '0').forEach(buffer::append);
                stringifiedCellValue = buffer.reverse().toString();
            }

            switch (columnType) {

                case "date": // created as java.util.Date in binlog connector
                    stringifiedCellValue = cellValue.toString();
                    break;

                case "timestamp": // created as java.sql.Timestamp in binlog connector
                    if (! (cellValue instanceof java.sql.Timestamp) ) {
                       LOG.warn("binlog parser has changed java type for timestamp!");
                    }

                    // a workaround for UTC-enforcement by mysql-binlog-connector
                    String tzId = ZonedDateTime.now().getZone().toString();
                    ZoneId zoneId = ZoneId.of(tzId);
                    Long timestamp =  ((java.sql.Timestamp) cellValue).getTime();
                    LocalDateTime aLDT = Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDateTime();
                    Integer offset  = ZonedDateTime.from(aLDT.atZone(ZoneId.of(tzId))).getOffset().getTotalSeconds();
                    timestamp = timestamp - offset * 1000;
                    stringifiedCellValue = String.valueOf(timestamp);

                    break;

                case "datetime":  // <- this is not reliable outside of UTC
                case "time":      // <- this is not reliable outside of UTC
                    break;

                default: break;
            }

            if (columnType.contains("tiny")) {
                if (columnType.contains("unsigned")) {
                    stringifiedCellValue = String.valueOf(
                            Byte.toUnsignedLong(
                                    (Integer.valueOf((int) cellValue)).byteValue()
                            )
                    );
                } else {
                    stringifiedCellValue = String.valueOf(Number.class.cast(cellValue).longValue());
                }
            } else if (columnType.contains("smallint")) {
                if (columnType.contains("unsigned")) {
                    stringifiedCellValue = String.valueOf(
                            (Integer.parseInt(cellValue.toString()) & 0xffff)
                    );
                } else {
                    stringifiedCellValue = String.valueOf(Number.class.cast(cellValue).longValue());
                }
            } else if (columnType.contains("mediumint")) {
                if (columnType.contains("unsigned")) {
                    stringifiedCellValue = String.valueOf(
                            ((Integer) cellValue) & 0xffffff
                    );
                } else {
                    stringifiedCellValue = String.valueOf(Number.class.cast(cellValue).longValue());
                }
            } else if (columnType.contains("bigint")) {
                if (columnType.contains("unsigned")) {
                    long i = (Long) cellValue;
                    int upper = (int) (i >>> 32);
                    int lower = (int) i;
                    stringifiedCellValue = String.valueOf(
                            BigInteger.valueOf(
                                    Integer.toUnsignedLong(upper)
                            )
                            .shiftLeft(32)
                            .add(
                                BigInteger.valueOf(Integer.toUnsignedLong(lower))
                            )
                    );
                } else {
                    stringifiedCellValue = String.valueOf(Number.class.cast(cellValue).longValue());
                }
            } else if (columnType.contains("int")) {
                if (columnType.contains("unsigned")) {
                    stringifiedCellValue = String.valueOf(Long.valueOf(((Integer) cellValue)) & 0x00000000FFFFFFFFl);
                } else {
                    stringifiedCellValue =  String.valueOf(Number.class.cast(cellValue).intValue());
                }
            }


            if (cache.containsKey(columnType)) {
                if (columnType.startsWith("enum")) {
                    int index = Number.class.cast(cellValue).intValue();

                    if (index > 0) {
                        stringifiedCellValue = String.valueOf(cache.get(columnType)[index - 1]);
                    } else {
                        stringifiedCellValue = null;
                    }
                } else if (columnType.startsWith("set")) {
                    long bits = Number.class.cast(cellValue).longValue();

                    if (bits > 0) {
                        String[] members = cache.get(columnType);
                        List<String> items = new ArrayList<>();

                        for (int index = 0; index < members.length; index++) {
                            if (((bits >> index) & 1) == 1) {
                                items.add(members[index]);
                            }
                        }
                        stringifiedCellValue = String.valueOf(items.toArray(new String[0]));
                    } else {
                        stringifiedCellValue = null;
                    }
                }
            }
        }

        if (stringifiedCellValue == null) {
            stringifiedCellValue = "NULL";
        }

        return stringifiedCellValue;
    }
}
