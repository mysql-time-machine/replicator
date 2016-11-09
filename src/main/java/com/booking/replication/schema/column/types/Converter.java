package com.booking.replication.schema.column.types;

import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.exception.TableMapException;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.*;

import com.google.code.or.common.util.MySQLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.regex.Pattern;

/**
 * Created by bdevetak on 23/11/15.
 */
public class Converter {

    private static final int MASK = 0xff;

    private static String unsignedPattern = "unsigned";

    private static Pattern isUnsignedPattern = Pattern.compile(unsignedPattern, Pattern.CASE_INSENSITIVE);

    private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

    private static final Date ZERO_DATE = MySQLUtils.toDate(0);

    // --------------------------------------------------------------------
    // This function was taken from linked-in databus and adapted to output
    // strings instead of avro types.
    //
    // Extracts string representation from or typed column. For now just
    // calls toString. Later if needed some type specific processing
    // can be added
    public static String  orTypeToString(Column column, ColumnSchema columnSchema)
        throws TableMapException {

        // ================================================================
        // Bit
        // ================================================================
        if (column instanceof BitColumn) {
            BitColumn bc = (BitColumn) column;
            return bc.toString();
        } else if (column instanceof BlobColumn) {
            // ================================================================
            // Blob and Text column types
            // ================================================================
            BlobColumn bc = (BlobColumn) column;
            byte[] bytes = bc.getValue();

            // TINYTEXT, TEXT, MEDIUMTEXT, and LONGTEXT
            if (columnSchema.getColumnType().contains("text")) {

                String charSetName = columnSchema.getCharacterSetName();

                if (charSetName == null) {
                    // TODO: defualt to TABLE/DB charset; in the meantime return HEX-fied blob
                    return blobToHexString(bytes);
                } else if (charSetName.contains("utf8")) {
                    String utf8Value = null;
                    try {
                        utf8Value = new String(bytes,"UTF8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    return utf8Value;
                } else if (charSetName.contains("latin1")) {
                    String latin1Value = null;
                    try {
                        latin1Value = new String(bytes,"ISO-8859-1");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    return latin1Value;
                } else {
                    // TODO: handle other encodings; in the meantime return HEX-fied blob
                    return blobToHexString(bytes);
                }
            } else {
                // Ordinary Binary BLOB - convert to HEX string
                return blobToHexString(bytes);
            }
        } else if (column instanceof StringColumn) {
            // ================================================================
            // Varchar column type
            // ================================================================
            StringColumn sc = (StringColumn) column;

            String charSetName = columnSchema.getCharacterSetName();

            // Open replicator provides raw bytes. We need encoding from
            // the schema info in order to know how to decode the value
            // before sending it to HBase which will encode it to bytes
            // with its internal encoding (Bytes.toBytes())
            if (charSetName == null) {
                // TODO: defualt to TABLE/DB charset; in the meantime return HEX-fied blob
                byte[] bytes = sc.getValue();
                return blobToHexString(bytes);
            } else if (charSetName.contains("utf8")) {
                byte[] bytes = sc.getValue();
                String utf8Value = null;
                try {
                    utf8Value = new String(bytes,"UTF8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                return utf8Value;
            } else if (charSetName.contains("latin1")) {
                byte[] bytes = sc.getValue();
                String latin1Value = null;
                try {
                    latin1Value = new String(bytes,"ISO-8859-1");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                return latin1Value;
            } else {
                // TODO: handle other encodings; in the meantime return HEX-fied blob
                byte[] bytes = sc.getValue();
                return blobToHexString(bytes);
            }
        } else if (column instanceof NullColumn) {
            return "NULL";
        } else if (column instanceof SetColumn) {
            // ================================================================
            // Set and Enum types
            // ================================================================
            SetColumn sc = (SetColumn) column;
            long setValue = sc.getValue();
            if (columnSchema instanceof SetColumnSchema) {
                try {
                    return ((SetColumnSchema) columnSchema).getSetMembersFromNumericValue(setValue);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new TableMapException("Wrong mapping of set csv");
                }
            } else {
                throw new TableMapException("Got set colum, but the ColumnSchema instance is of wrong type");
            }
        } else if (column instanceof EnumColumn) {
            EnumColumn ec = (EnumColumn) column;
            int enumIntValue = ec.getValue();
            if (columnSchema instanceof EnumColumnSchema) {

                try {
                    return ((EnumColumnSchema) columnSchema).getEnumValueFromIndex(enumIntValue);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new TableMapException("Probaly wrong mapping of indexes for enum array");
                }
            } else {
                throw new TableMapException("Got enum colum, but the ColumnSchema instance is of wrong type");
            }
        } else if (column instanceof DecimalColumn) {
            // ================================================================
            // Floating point types
            // ================================================================
            DecimalColumn dc = (DecimalColumn) column;
            return dc.toString();
        } else if (column instanceof DoubleColumn) {
            DoubleColumn dc = (DoubleColumn) column;
            return dc.toString();
        } else if (column instanceof FloatColumn) {
            FloatColumn fc = (FloatColumn) column;
            return Float.toString(fc.getValue());
        } else if (column instanceof TinyColumn) {
            // ================================================================
            // Whole numbers (with unsigned option) types
            // ================================================================
            // 1 byte
            if (columnSchema.getDataType().equals("tinyint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getColumnType()).find();
                if (isUnsigned) {

                    TinyColumn tc = (TinyColumn) column;

                    int signedValue = tc.getValue();
                    long unsignedValue = (byte) (signedValue) & 0xff;
                    return Long.toString(unsignedValue);

                } else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    TinyColumn tc = (TinyColumn) column;
                    return tc.toString();
                }
            } else {
                throw new TableMapException("Unknown MySQL type in the event" + column.getClass() + " Object = " + column);
            }
        } else if (column instanceof ShortColumn) {
            // 2 bytes
            ShortColumn sc = (ShortColumn) column;
            if (columnSchema.getDataType().equals("smallint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getColumnType()).find();
                if (isUnsigned) {
                    return Long.toString(((long) sc.getValue()) & 0xffff);
                } else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return sc.toString();
                }
            } else {
                throw new TableMapException("Unknown MySQL type in the event" + column.getClass() + " Object = " + column);
            }
        } else if (column instanceof Int24Column) {
            // medium-int (3 bytes) in MySQL
            Int24Column ic = (Int24Column) column;
            if (columnSchema.getDataType().equals("mediumint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getColumnType()).find();
                if (isUnsigned) {
                    return Long.toString(((long) ic.getValue()) & 0xffffff);
                } else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return ic.toString();
                }
            } else {
                throw new TableMapException("Unknown MySQL type in the event" + column.getClass() + " Object = " + column);
            }
        } else if (column instanceof LongColumn) {
            // MySQL int (4 bytes)
            LongColumn lc = (LongColumn) column;
            if (columnSchema.getDataType().equals("int")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getColumnType()).find();
                if (isUnsigned) {
                    byte[] bytes = ByteBuffer.allocate(4).putInt(lc.getValue()).array();
                    BigInteger big = new BigInteger(1,bytes);
                    return big.toString();
                    //return Long.toString(((long) lc.getValue()) & 0xffffffffL);
                } else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return lc.toString();
                }
            } else {
                throw new TableMapException("Unknown MySQL type in the event" + column.getClass() + " Object = " + column);
            }
        } else if (column instanceof LongLongColumn) {
            // MySQL BigInt (8 bytes)
            LongLongColumn llc = (LongLongColumn) column;
            if (columnSchema.getDataType().equals("bigint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getColumnType()).find();
                if (isUnsigned) {
                    byte[] bytes = ByteBuffer.allocate(8).putLong(llc.getValue()).array();
                    BigInteger big = new BigInteger(1,bytes);
                    return big.toString();
                } else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return llc.toString();
                }
            } else {
                throw new TableMapException("Unknown"
                        + " MySQL type " + columnSchema.getDataType()
                        + " in the event " + column.getClass()
                        + " Object = " + column
                );
            }
        } else if (column instanceof YearColumn) {
            // ================================================================
            // Date&Time types
            // ================================================================
            YearColumn yc = (YearColumn) column;
            return yc.toString();
        } else if (column instanceof DateColumn) {
            DateColumn dc =  (DateColumn) column;

            /** A workaround for the bug in the open replicator's "0000-00-00" date parsing logic: according to MySQL
             * spec, this date is invalid and has a special treatment in jdbc
             */
            return dc.getValue().equals(ZERO_DATE) ? "NULL" : dc.toString();
        } else if (column instanceof DatetimeColumn) {
            DatetimeColumn dc = (DatetimeColumn) column;
            return dc.toString();
            // TODO: check if this bug is fixed in zendesk fork
            // Bug in OR for DateTIme and Time data-types.
            // MilliSeconds is not available for these columns but is set with currentMillis() wrongly.
        } else if (column instanceof Datetime2Column) {
            Datetime2Column d2c = (Datetime2Column) column;
            return d2c.toString();
        } else if (column instanceof TimeColumn) {
            TimeColumn tc = (TimeColumn) column;
            return tc.toString();
            // TODO: check if this bug is fixed in zendesk fork
            /**
             * There is a bug in OR where instead of using the default year as 1970, it is using 0070.
             * This is a temporary measure to resolve it by working around at this layer.
             * The value obtained from OR is subtracted from "0070-00-01 00:00:00"
             */
        } else if (column instanceof  Time2Column) {
            Time2Column t2c = (Time2Column) column;
            return t2c.toString();
        } else if (column instanceof TimestampColumn) {
            TimestampColumn tsc = (TimestampColumn) column;
            Long timestampValue = tsc.getValue().getTime();
            return String.valueOf(timestampValue);
        } else if (column instanceof Timestamp2Column) {
            Timestamp2Column ts2c = (Timestamp2Column) column;
            Long timestamp2Value = ts2c.getValue().getTime();
            return String.valueOf(timestamp2Value);
        } else {
            throw new TableMapException("Unknown MySQL type in the event" + column.getClass() + " Object = " + column);
        }
    }

    public static String blobToHexString( byte [] raw ) {
        if ( raw == null ) {
            return "NULL";
        }
        final StringBuilder hex = new StringBuilder( 2 * raw.length );
        for ( final byte b : raw ) {
            int ivalue = b & 0xFF;
            if (ivalue < 16 ) {
                hex.append("0").append(Integer.toHexString(ivalue).toUpperCase());
            } else {
                hex.append(Integer.toHexString(ivalue).toUpperCase());
            }
        }
        return hex.toString();
    }
}
