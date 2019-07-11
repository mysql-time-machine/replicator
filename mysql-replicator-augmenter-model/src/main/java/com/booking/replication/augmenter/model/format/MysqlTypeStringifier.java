package com.booking.replication.augmenter.model.format;

import com.booking.replication.augmenter.model.schema.DataType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

public class MysqlTypeStringifier {

    private static final Logger LOG = LogManager.getLogger(MysqlTypeStringifier.class);

    private static final String NULL_STRING = "NULL";

    private static final Long UNSIGNED_TINYINT_MASK     = 0x00000000000000FFL;
    private static final Long UNSIGNED_SMALLINT_MASK    = 0x000000000000FFFFL;
    private static final Long UNSIGNED_MEDIUMINT_MASK   = 0x0000000000FFFFFFL;
    private static final Long UNSIGNED_INT_MASK         = 0x00000000FFFFFFFFL;
    private static final Long DEFAULT_MASK              = 0xFFFFFFFFFFFFFFFFL;

    public static String convertToString(Serializable cellValue, String collation, DataType dataType, String columnType, String[] groupValues) {

        if (cellValue == null) {
            return NULL_STRING;
        }

        boolean isUnsigned = columnType.contains("unsigned");

        switch (dataType) {
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

            case BIT: {
                final BitSet data = (BitSet) cellValue;
                final StringBuilder buffer = new StringBuilder(data.length());
                IntStream.range(0, data.length()).mapToObj(i -> data.get(i) ? '1' : '0').forEach(buffer::append);
                return buffer.reverse().toString();
            }

            case DATE: {
                // created as java.util.Date in binlog connector
                return cellValue.toString();
            }

            case TIMESTAMP: {
                // created as java.sql.Timestamp in binlog connector
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

                return String.valueOf(timestamp);
            }

            case DATETIME:
            case TIME: {
                // this is not reliable outside of UTC
                return NULL_STRING;
            }

            case ENUM: {
                int index = (Integer) cellValue;

                if (index > 0) {
                    return String.valueOf(groupValues[index - 1]);
                } else {
                    return NULL_STRING;
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
                    return NULL_STRING;
                }
            }

            case TINYINT: {
                Long mask = isUnsigned ? UNSIGNED_TINYINT_MASK : DEFAULT_MASK;
                return String.valueOf(maskAndGet(cellValue, mask));
            }

            case SMALLINT: {
                Long mask = isUnsigned ? UNSIGNED_SMALLINT_MASK : DEFAULT_MASK;
                return String.valueOf(maskAndGet(cellValue, mask));
            }

            case MEDIUMINT: {
                Long mask = isUnsigned ? UNSIGNED_MEDIUMINT_MASK : DEFAULT_MASK;
                return String.valueOf(maskAndGet(cellValue, mask));
            }

            case INT: {
                Long mask = isUnsigned ? UNSIGNED_INT_MASK : DEFAULT_MASK;
                return String.valueOf(maskAndGet(cellValue, mask));
            }

            case BIGINT: {
                if (isUnsigned) {
                    long longValue = (Long) cellValue;

                    int upper = (int) (longValue >>> 32);
                    int lower = (int) longValue;

                    BigInteger bigInteger = BigInteger.valueOf(Integer.toUnsignedLong(upper))
                            .shiftLeft(32)
                            .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));

                    return String.valueOf(bigInteger);
                } else {
                    return String.valueOf(maskAndGet(cellValue, DEFAULT_MASK));
                }
            }

            case UNKNOWN:
            default: {
                LOG.error(String.format("The datatype is %s hence returning %s ", dataType.getCode(), NULL_STRING));
                return NULL_STRING;
            }
        }
    }

    private static Long maskAndGet(Serializable cellValue, Long mask) {
        Long longValue = ((Number) cellValue).longValue();
        return longValue & mask;
    }
}
