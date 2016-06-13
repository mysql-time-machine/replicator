package com.booking.replication.schema.column.types;

import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.exception.TableMapException;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

/**
 * Created by bdevetak on 23/11/15.
 */
public class Converter {

    private static final int MASK = 0xff;

    private static String unsignedPattern = "unsigned";

    private static Pattern isUnsignedPattern = Pattern.compile(unsignedPattern, Pattern.CASE_INSENSITIVE);

    private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

    // --------------------------------------------------------------------
    // This function was taken from linked-in databus and adapted to output
    // strings instead of avro types.
    //
    // Extracts string representation from or typed column. For now just
    // calls toString. Later if needed some type specific processing
    // can be added
    public static String orTypeToString(Column s, ColumnSchema columnSchema) throws TableMapException
    {

        // ================================================================
        // Bit
        // ================================================================
        if (s instanceof BitColumn)
        {
            BitColumn bc = (BitColumn) s;
            return bc.toString();
        }

        // ================================================================
        // Blob and Text column types
        // ================================================================
        else if (s instanceof BlobColumn)
        {
            BlobColumn bc = (BlobColumn) s;
            byte[] bytes = bc.getValue();

            // TINYTEXT, TEXT, MEDIUMTEXT, and LONGTEXT
            if (columnSchema.getCOLUMN_TYPE().contains("text")) {

                String charSetName = columnSchema.getCHARACTER_SET_NAME();

                if (charSetName == null) {
                    // TODO: defualt to TABLE/DB charset; in the meantime return HEX-fied blob
                    return blobToHexString(bytes);
                }
                else if (charSetName.contains("utf8")) {
                    String utf8Value = null;
                    try {
                        utf8Value = new String(bytes,"UTF8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    return utf8Value;
                }
                else if (charSetName.contains("latin1")) {
                    String latin1Value = null;
                    try {
                        latin1Value = new String(bytes,"ISO-8859-1");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    return latin1Value;
                }
                else {
                    // TODO: handle other encodings; in the meantime return HEX-fied blob
                    return blobToHexString(bytes);
                }
            }
            else {
                // Ordinary Binary BLOB - convert to HEX string
                return blobToHexString(bytes);
            }
        }

        // ================================================================
        // Varchar column type
        // ================================================================
        else if (s instanceof StringColumn)
        {
            StringColumn sc = (StringColumn) s;

            String charSetName = columnSchema.getCHARACTER_SET_NAME();

            // Open replicator provides raw bytes. We need encoding from
            // the schema info in order to know how to decode the value
            // before sending it to HBase which will encode it to bytes
            // with its internal encoding (Bytes.toBytes())
            if (charSetName == null) {
                // TODO: defualt to TABLE/DB charset; in the meantime return HEX-fied blob
                byte[] bytes = sc.getValue();
                return blobToHexString(bytes);
            }
            else if (charSetName.contains("utf8")) {
                byte[] bytes = sc.getValue();
                String utf8Value = null;
                try {
                    utf8Value = new String(bytes,"UTF8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                return utf8Value;
            }
            else if (charSetName.contains("latin1")) {
                byte[] bytes = sc.getValue();
                String latin1Value = null;
                try {
                    latin1Value = new String(bytes,"ISO-8859-1");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                return latin1Value;
            }
            else {
                // TODO: handle other encodings; in the meantime return HEX-fied blob
                byte[] bytes = sc.getValue();
                return blobToHexString(bytes);
            }
        }
        else if (s instanceof NullColumn)
        {
            return "NULL";
        }

        // ================================================================
        // Set and Enum types
        // ================================================================
        else if (s instanceof SetColumn)
        {
            SetColumn sc = (SetColumn) s;
            long setValue = sc.getValue();
            if (columnSchema instanceof SetColumnSchema) {
                try {
                    return ((SetColumnSchema) columnSchema).getSetMembersFromNumericValue(setValue);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    throw new TableMapException("Wrong mapping of set csv");
                }
            }
            else {
                throw new TableMapException("Got set colum, but the ColumnSchema instance is of wrong type");
            }
        }
        else if (s instanceof EnumColumn)
        {
            EnumColumn ec = (EnumColumn) s;
            int enumIntValue = ec.getValue();
            if (columnSchema instanceof EnumColumnSchema) {

                try {
                    return ((EnumColumnSchema) columnSchema).getEnumValueFromIndex(enumIntValue);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    throw new TableMapException("Probaly wrong mapping of indexes for enum array");
                }
            }
            else {
                throw new TableMapException("Got enum colum, but the ColumnSchema instance is of wrong type");
            }

        }

        // ================================================================
        // Floating point types
        // ================================================================
        else if (s instanceof DecimalColumn)
        {
            DecimalColumn dc = (DecimalColumn) s;
            return dc.toString();
        }
        else if (s instanceof DoubleColumn)
        {

            DoubleColumn dc = (DoubleColumn) s;
            return dc.toString();

        }
        else if (s instanceof FloatColumn)
        {
            FloatColumn fc = (FloatColumn) s;
            return Float.toString(fc.getValue());
        }

        // ================================================================
        // Whole numbers (with unsigned option) types
        // ================================================================
        else if (s instanceof TinyColumn)
        {
            // 1 byte
            if (columnSchema.getDATA_TYPE().equals("tinyint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getCOLUMN_TYPE()).find();
                if (isUnsigned) {

                    TinyColumn tc = (TinyColumn) s;

                    int signedValue = tc.getValue();
                    byte x = (byte) (signedValue);
                    long unsignedValue = x & 0xff;
                    return Long.toString(unsignedValue);

                }
                else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    TinyColumn tc = (TinyColumn) s;
                    return tc.toString();
                }
            }
            else {
                throw new TableMapException("Unknown MySQL type in the event" + s.getClass() + " Object = " + s);
            }
        }
        else if (s instanceof ShortColumn)
        {
            // 2 bytes
            ShortColumn sc = (ShortColumn) s;
            if (columnSchema.getDATA_TYPE().equals("smallint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getCOLUMN_TYPE()).find();
                if (isUnsigned) {
                    return Long.toString(((long) sc.getValue()) & 0xffff);
                }
                else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return sc.toString();
                }
            }
            else {
                throw new TableMapException("Unknown MySQL type in the event" + s.getClass() + " Object = " + s);
            }
        }
        else if (s instanceof Int24Column)
        {
            // medium-int (3 bytes) in MySQL
            Int24Column ic = (Int24Column) s;
            if (columnSchema.getDATA_TYPE().equals("mediumint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getCOLUMN_TYPE()).find();
                if (isUnsigned) {
                    return Long.toString(((long) ic.getValue()) & 0xffffff);
                }
                else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return ic.toString();
                }
            }
            else {
                throw new TableMapException("Unknown MySQL type in the event" + s.getClass() + " Object = " + s);
            }
        }
        else if (s instanceof LongColumn)
        {
            // MySQL int (4 bytes)
            LongColumn lc = (LongColumn) s;
            if (columnSchema.getDATA_TYPE().equals("int")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getCOLUMN_TYPE()).find();
                if (isUnsigned) {
                    byte[] bytes = ByteBuffer.allocate(4).putInt(lc.getValue()).array();
                    BigInteger big = new BigInteger(1,bytes);
                    return big.toString();
                    //return Long.toString(((long) lc.getValue()) & 0xffffffffL);
                }
                else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return lc.toString();
                }
            }
            else {
                throw new TableMapException("Unknown MySQL type in the event" + s.getClass() + " Object = " + s);
            }
        }
        else if (s instanceof LongLongColumn)
        {
            // MySQL BigInt (8 bytes)
            LongLongColumn llc = (LongLongColumn) s;
            if (columnSchema.getDATA_TYPE().equals("bigint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getCOLUMN_TYPE()).find();
                if (isUnsigned) {
                    byte[] bytes = ByteBuffer.allocate(8).putLong(llc.getValue()).array();
                    BigInteger big = new BigInteger(1,bytes);
                    return big.toString();
                }
                else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return llc.toString();
                }
            }
            else {
                throw new TableMapException("Unknown"
                        + " MySQL type " + columnSchema.getDATA_TYPE()
                        + " in the event " + s.getClass()
                        + " Object = " + s
                );
            }
        }

        // ================================================================
        // Date&Time types
        // ================================================================
        else if (s instanceof YearColumn)
        {
            YearColumn yc = (YearColumn) s;
            return yc.toString();
        }
        else if (s instanceof DateColumn)
        {
            DateColumn dc =  (DateColumn) s;
            return dc.toString();
        }
        else if (s instanceof DatetimeColumn)
        {
            DatetimeColumn dc = (DatetimeColumn) s;
            return dc.toString();
            // TODO: check if this bug is fixed in zendesk fork
            //Bug in OR for DateTIme and Time data-types. MilliSeconds is not available for these columns but is set with currentMillis() wrongly.
        }
        else if (s instanceof Datetime2Column) {
            Datetime2Column d2c = (Datetime2Column) s;
            return d2c.toString();
        }
        else if (s instanceof TimeColumn)
        {
            TimeColumn tc = (TimeColumn) s;
            return tc.toString();
            // TODO: check if this bug is fixed in zendesk fork
            /**
             * There is a bug in OR where instead of using the default year as 1970, it is using 0070.
             * This is a temporary measure to resolve it by working around at this layer. The value obtained from OR is subtracted from "0070-00-01 00:00:00"
             */
        }
        else if (s instanceof  Time2Column) {
            Time2Column t2c = (Time2Column) s;
            return t2c.toString();
        }
        else if (s instanceof TimestampColumn)
        {
            TimestampColumn tsc = (TimestampColumn) s;
            Long timestampValue = tsc.getValue().getTime();
            return String.valueOf(timestampValue);
        }
        else if (s instanceof Timestamp2Column) {
            Timestamp2Column ts2c = (Timestamp2Column) s;
            Long timestamp2Value = ts2c.getValue().getTime();
            return String.valueOf(timestamp2Value);
        }
        else
        {
            throw new TableMapException("Unknown MySQL type in the event" + s.getClass() + " Object = " + s);
        }
    }

    public static String blobToHexString( byte [] raw ) {
        if ( raw == null ) {
            return "NULL";
        }
        final StringBuilder hex = new StringBuilder( 2 * raw.length );
        for ( final byte b : raw ) {
            int i = b & 0xFF;
            if (i < 16 ) {
                 hex.append("0").append(Integer.toHexString(i).toUpperCase());
            }
            else {
                hex.append(Integer.toHexString(i).toUpperCase());
            }
        }
        return hex.toString();
    }
}
