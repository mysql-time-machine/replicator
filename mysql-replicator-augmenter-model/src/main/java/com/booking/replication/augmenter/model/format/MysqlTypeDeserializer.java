package com.booking.replication.augmenter.model.format;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.DataType;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.IntStream;

import javax.xml.bind.DatatypeConverter;

public class MysqlTypeDeserializer {

    private static final Logger LOG = LogManager.getLogger(MysqlTypeDeserializer.class);

    private static final SimpleDateFormat DATE_FORMAT       = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat TIME_FORMAT       = new SimpleDateFormat("HH:mm:ss.SSS");

    private static final Long UNSIGNED_TINYINT_MASK     = 0x00000000000000FFL;
    private static final Long UNSIGNED_SMALLINT_MASK    = 0x000000000000FFFFL;
    private static final Long UNSIGNED_MEDIUMINT_MASK   = 0x0000000000FFFFFFL;
    private static final Long UNSIGNED_INT_MASK         = 0x00000000FFFFFFFFL;
    private static final Long DEFAULT_MASK              = 0xFFFFFFFFFFFFFFFFL;

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static Object convertToObject(Serializable cellValue, ColumnSchema columnSchema, String[] groupValues) {

        if (cellValue == null) {
            return null;
        }

        String collation    = columnSchema.getCollation();
        String columnType   = columnSchema.getColumnType();
        DataType dataType   = columnSchema.getDataType();
        boolean isUnsigned  = columnType.contains("unsigned");

        switch (dataType) {
            case BINARY:
            case VARBINARY: {
                byte[] bytes = (byte[]) cellValue;

                if (bytes.length == columnSchema.getCharMaxLength()) {
                    return DatatypeConverter.printHexBinary(bytes);
                } else {
                    byte[] bytesWithPadding = new byte[columnSchema.getCharMaxLength()];

                    for (int i = 0; i < bytesWithPadding.length; ++i) {
                        bytesWithPadding[i] = (i < bytes.length) ? bytes[i] : 0;
                    }

                    return DatatypeConverter.printHexBinary(bytesWithPadding);
                }
            }

            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB: {
                byte[] bytes = (byte[]) cellValue;
                return DatatypeConverter.printHexBinary(bytes);
            }

            case CHAR:
            case VARCHAR:
            case TEXT:
            case MEDIUMTEXT:
            case TINYTEXT: {
                byte[] bytes = (byte[]) cellValue;

                if (collation.contains("latin1")) {
                    return new String(bytes, StandardCharsets.ISO_8859_1);
                } else {
                    return new String(bytes, StandardCharsets.UTF_8);
                }
            }

            case JSON: {
                byte[] bytes = (byte[]) cellValue;
                try {
                    return JsonBinary.parseAsString(bytes);
                } catch (IOException ex) {
                    LOG.error(
                            String.format("Could not parse JSON string Column Name : %s, byte[]%s",
                                    columnSchema.getName(), Arrays.toString(bytes)), ex);
                    return  null;
                }
            }

            case BIT: {
                final BitSet data = (BitSet) cellValue;

                if (data.length() == 0) {
                    return "0";
                }

                final StringBuilder buffer = new StringBuilder(data.length());
                IntStream.range(0, data.length()).mapToObj(i -> data.get(i) ? '1' : '0').forEach(buffer::append);
                return buffer.reverse().toString();
            }

            case DATE: {
                return DATE_FORMAT.format(cellValue);
            }

            case TIME: {
                return TIME_FORMAT.format(cellValue);
            }

            case DATETIME:
            case TIMESTAMP: {
                Long timestamp = (Long) cellValue;

                ZoneId zoneId = ZonedDateTime.now().getZone();
                LocalDateTime aLDT = Instant.ofEpochMilli(timestamp).atZone(zoneId).toLocalDateTime();

                Integer offset  = ZonedDateTime.from(aLDT.atZone(zoneId)).getOffset().getTotalSeconds();
                timestamp = timestamp - offset * 1000;

                return String.valueOf(timestamp);
            }

            case ENUM: {
                int index = (Integer) cellValue;

                if (index > 0) {
                    return String.valueOf(groupValues[index - 1]);
                } else {
                    return null;
                }
            }

            case SET: {
                long bits = (Long) cellValue;

                if (bits > 0) {
                    List<String> items = new ArrayList<>();

                    for (int index = 0; index < groupValues.length; index++) {
                        if (((bits >> index) & 1) == 1) {
                            items.add(groupValues[index]);
                        }
                    }

                    return String.join(",", items.toArray(new String[0]));
                } else {
                    return null;
                }
            }

            case TINYINT: {
                Long mask = isUnsigned ? UNSIGNED_TINYINT_MASK : DEFAULT_MASK;
                return maskAndGet(cellValue, mask);
            }

            case SMALLINT: {
                Long mask = isUnsigned ? UNSIGNED_SMALLINT_MASK : DEFAULT_MASK;
                return maskAndGet(cellValue, mask);
            }

            case MEDIUMINT: {
                Long mask = isUnsigned ? UNSIGNED_MEDIUMINT_MASK : DEFAULT_MASK;
                return maskAndGet(cellValue, mask);
            }

            case INT: {
                Long mask = isUnsigned ? UNSIGNED_INT_MASK : DEFAULT_MASK;
                return maskAndGet(cellValue, mask);
            }

            case BIGINT: {
                if (isUnsigned) {
                    long longValue = (Long) cellValue;

                    int upper = (int) (longValue >>> 32);
                    int lower = (int) longValue;

                    BigInteger bigInteger = BigInteger.valueOf(Integer.toUnsignedLong(upper))
                            .shiftLeft(32)
                            .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));

                    return bigInteger;
                } else {
                    return maskAndGet(cellValue, DEFAULT_MASK);
                }
            }
            case FLOAT:
            case DOUBLE: {
                //FLOT      converted as java.lang.Float
                //Double    converted as java.lang.Double
                return cellValue;
            }

            case DECIMAL: {
                BigDecimal decimal = (BigDecimal) cellValue;
                return decimal.toPlainString();
            }

            case UNKNOWN:
            default: {

                if (cellValue instanceof byte[]) {
                    byte[] bytes = (byte[]) cellValue;
                    return DatatypeConverter.printHexBinary(bytes);
                }

                LOG.error(String.format("The datatype is %s hence returning null", dataType.getCode()));
                return null;
            }
        }
    }

    private static Long maskAndGet(Serializable cellValue, Long mask) {
        Long longValue = ((Number) cellValue).longValue();
        return longValue & mask;
    }
}