package com.booking.replication.augmenter.model.deserializer;

import com.booking.replication.augmenter.model.schema.ColumnSchema;

import javax.xml.bind.DatatypeConverter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class CellValueDeserializer {

    private static String binaryToHexString(byte[] value, Integer displayWidth) {
        if (displayWidth != null && value.length != displayWidth) {
            // zerofill rest of the bytes
            byte[] zerofilledValue = new byte[displayWidth];
            for (int i = 0; i < displayWidth; i++) {
                if (i >= value.length) zerofilledValue[i] = 0;
                else zerofilledValue[i] = value[i];
            }
            return DatatypeConverter.printHexBinary(zerofilledValue);
        }
        return DatatypeConverter.printHexBinary(value);
    }

    public static Object deserialize(Map<String, String[]> cache, ColumnSchema columnSchema, Serializable cellValue, String collation) {
        Object deserializedCellValue = null;
        String columnType = columnSchema.getType();

        if (cellValue == null) {
            return null; // TODO: option for configuring null handling
        }

        if (columnType.contains("binary") && (cellValue instanceof byte[])) {
            Integer displayWidth = getDisplayWidth(columnType);
            deserializedCellValue = CellValueDeserializer.binaryToHexString((byte[]) cellValue, displayWidth).toLowerCase();
            return deserializedCellValue;
        }

        if (collation != null && (cellValue instanceof byte[])) {
            deserializedCellValue = getStringValue((byte[]) cellValue, collation);
            return deserializedCellValue;
        }

        if (columnType.contains("decimal") ||
                columnType.contains("numeric")) {
            //todo: get precision and decide data type
            BigDecimal cellValue1 = (BigDecimal) cellValue;
            deserializedCellValue = cellValue1.toPlainString();
            ;
        }

        if (columnType.contains("timestamp") ||
                columnType.contains("date") ||
                columnType.contains("time")) {
            //todo: Change it to timestamp (long).
            deserializedCellValue = cellValue.toString();
        }

        if (cellValue instanceof BitSet) {
            final BitSet data = (BitSet) cellValue;
            final StringBuilder buffer = new StringBuilder(data.length());
            IntStream.range(0, data.length()).mapToObj(i -> data.get(i) ? '1' : '0').forEach(buffer::append);
            deserializedCellValue = buffer.reverse().toString();
        }

        if (columnType.contains("tinyint")) {
            if (columnType.contains("unsigned")) {
                int a = (Integer) cellValue;
                byte x1 = (byte) a;
                deserializedCellValue = Byte.toUnsignedInt(x1);
            } else {
                deserializedCellValue = cellValue;
            }
        } else if (columnType.contains("smallint")) {
            if (columnType.contains("unsigned")) {
                int a = (Integer) cellValue;
                short x1 = (short) a;
                deserializedCellValue = Short.toUnsignedInt(x1);
            } else {
                deserializedCellValue = cellValue;
            }
        } else if (columnType.contains("mediumint")) {
            if (columnType.contains("unsigned")) {
                int a = (Integer) cellValue;
                deserializedCellValue = a & 0xffffff;
            } else {
                deserializedCellValue = cellValue;
            }
        } else if (columnType.contains("bigint")) {
            if (columnType.contains("unsigned")) {
                long a = (Long) cellValue;
                deserializedCellValue = Long.toUnsignedString(a);
            } else {
                deserializedCellValue = cellValue;
            }
        } else if (columnType.contains("int")) {
            if (columnType.contains("unsigned")) {
                int a = (Integer) cellValue;
                deserializedCellValue = Integer.toUnsignedLong(a);
            } else {
                deserializedCellValue = cellValue;
            }
        } else if (columnType.contains("float")) {
            deserializedCellValue = (Float) cellValue;
        } else if (columnType.contains("double")) {
            deserializedCellValue = (Double) cellValue;
        }

        if (deserializedCellValue == null) {
            deserializedCellValue = cellValue.toString();
        }

        if (cache.containsKey(columnType)) {
            if (columnType.startsWith("enum")) {
                int index = Number.class.cast(cellValue).intValue();

                if (index > 0) {
                    deserializedCellValue = String.valueOf(cache.get(columnType)[index - 1]);
                } else {
                    deserializedCellValue = null;
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
                    deserializedCellValue = String.valueOf(items.toArray(new String[0]));
                } else {
                    deserializedCellValue = null;
                }
            }
        }


        return deserializedCellValue;
    }

    private static Integer getDisplayWidth(String columnType) {
        String pattern = "binary\\((\\d+)\\)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(columnType);

        if (m.find()) {
            return Integer.parseInt(m.group(1));
        } else {
            return null;
        }
    }

    private static Object getStringValue(byte[] cellValue, String collation) {
        Object deserializedCellValue;
        byte[] bytes = cellValue;
        if (collation.contains("latin1")) {
            deserializedCellValue = new String(bytes, StandardCharsets.ISO_8859_1);
        } else {
            // Currently handle all the other character set as UTF8, extend this to handle specific character sets
            deserializedCellValue = new String(bytes, StandardCharsets.UTF_8);
        }
        return deserializedCellValue;
    }
}
