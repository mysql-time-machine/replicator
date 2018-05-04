package com.booking.replication.schema.column.types;
import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.cell.*;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.exception.TableMapException;

import com.booking.replication.schema.column.types.TypeConversionRules;

import com.booking.replication.util.MySQLUtils;

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
    // This function was taken from linked-in Databus and adapted to output
    // strings instead of avro types.
    //
    // Extracts string representation from typed column.
    public static String cellValueToString(
            Cell cell,
            ColumnSchema columnSchema,
            TypeConversionRules typeConversionRules
        ) throws TableMapException {

        // ================================================================
        // Workaround for type resolution loss in mysql-binlog-connector:
        //
        // Basically, mysql-binlog-connector does not have separate classes
        // for different mysql int types, so mysql tiny_int, small_int,
        // medium_int and int are all mapped to java int type. This is
        // different from open replicator which gets the type from
        // table_map event and wraps the value into the corresponding class.
        //
        // There are two ways to solve this:
        //
        //      1. Write a custom deserializer which will return typed
        //         values instead of Serializable
        //
        //      2. Use type information from active_schema
        //
        // Here we go with 2nd approach.
        if (cell instanceof LongCell) {
            if (columnSchema.getDataType().equals("tinyint")) {
                cell = TinyCell.valueOf(((LongCell) cell).getValue());
            }
            if (columnSchema.getDataType().equals("smallint")) {
                cell = ShortCell.valueOf(((LongCell) cell).getValue());
            }
            if (columnSchema.getDataType().equals("mediumint")) {
                cell = Int24Cell.valueOf(((LongCell) cell).getValue());
            }
        }
        // ================================================================
        // Bit
        // ================================================================
        if (cell instanceof BitCell) {
            BitCell bc = (BitCell) cell;
            return bc.toString();
        } else if (cell instanceof BlobCell) {
            // ================================================================
            // Blob and Text column types
            // ================================================================
            BlobCell bc = (BlobCell) cell;
            byte[] bytes = bc.getValue();

            // TINYTEXT, TEXT, MEDIUMTEXT, and LONGTEXT
            if (columnSchema.getColumnType().contains("text")) {

                String charSetName = columnSchema.getCharacterSetName();

                if (charSetName == null) {
                    // TODO: default to TABLE/DB charset; in the meantime return
                    //       HEX string representation of the blob
                    return typeConversionRules.blobToHexString(bytes);
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
                    // TODO: handle other encodings; in the meantime return
                    //       HEX string representation of the blob
                    return typeConversionRules.blobToHexString(bytes);
                }
            } else {
                // Ordinary Binary BLOB - convert to HEX string
                return typeConversionRules.blobToHexString(bytes);
            }
        } else if (cell instanceof StringCell) {
            // ================================================================
            // Varchar column type
            // ================================================================
            StringCell sc = (StringCell) cell;

            String charSetName = columnSchema.getCharacterSetName();

            // Open replicator provides raw bytes. We need encoding from
            // the schema info in order to know how to decode the value
            // before sending it to HBase which will encode it to bytes
            // with its internal encoding (Bytes.toBytes())
            if (charSetName == null) {
                // TODO: defualt to TABLE/DB charset; in the meantime return HEX-fied blob
                byte[] bytes = sc.getValue();
                return typeConversionRules.blobToHexString(bytes);
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
                return typeConversionRules.blobToHexString(bytes);
            }
        } else if (cell instanceof NullCell) {
            return "NULL";
        } else if (cell instanceof SetCell) {
            // ================================================================
            // Set and Enum types
            // ================================================================
            SetCell sc = (SetCell) cell;
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
        } else if (cell instanceof EnumCell) {
            EnumCell ec = (EnumCell) cell;
            int enumIntValue = ec.getValue();
            if (columnSchema instanceof EnumColumnSchema) {

                try {
                    return ((EnumColumnSchema) columnSchema).getEnumValueFromIndex(enumIntValue);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new TableMapException("Probably wrong mapping of indexes for enum array");
                }
            } else {
                throw new TableMapException("Got enum column, but the ColumnSchema instance is of wrong type");
            }
        } else if (cell instanceof DecimalCell) {
            // ================================================================
            // Floating point types
            // ================================================================
            DecimalCell dc = (DecimalCell) cell;
            return dc.toString();
        } else if (cell instanceof DoubleCell) {
            DoubleCell dc = (DoubleCell) cell;
            return dc.toString();
        } else if (cell instanceof FloatCell) {
            FloatCell fc = (FloatCell) cell;
            return Float.toString(fc.getValue());
        } else if (cell instanceof TinyCell) {
            // ================================================================
            // Whole numbers (with unsigned option) types
            // ================================================================
            // 1 byte
            if (columnSchema.getDataType().equals("tinyint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getColumnType()).find();
                if (isUnsigned) {

                    TinyCell tc = (TinyCell) cell;

                    int signedValue = tc.getValue();
                    long unsignedValue = (byte) (signedValue) & 0xff;
                    return Long.toString(unsignedValue);

                } else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    TinyCell tc = (TinyCell) cell;
                    return tc.toString();
                }
            } else {
                throw new TableMapException("Unknown MySQL type in the event" + cell.getClass() + " Object = " + cell);
            }
        } else if (cell instanceof ShortCell) {
            // 2 bytes
            ShortCell sc = (ShortCell) cell;
            if (columnSchema.getDataType().equals("smallint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getColumnType()).find();
                if (isUnsigned) {
                    return Long.toString(((long) sc.getValue()) & 0xffff);
                } else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return sc.toString();
                }
            } else {
                throw new TableMapException("Unknown MySQL type in the event" + cell.getClass() + " Object = " + cell);
            }
        } else if (cell instanceof Int24Cell) {
            // medium-int (3 bytes) in MySQL
            Int24Cell ic = (Int24Cell) cell;
            if (columnSchema.getDataType().equals("mediumint")) {
                boolean isUnsigned = isUnsignedPattern.matcher(columnSchema.getColumnType()).find();
                if (isUnsigned) {
                    return Long.toString(((long) ic.getValue()) & 0xffffff);
                } else {
                    // Default OpenReplicator/Java behaviour (signed numbers)
                    return ic.toString();
                }
            } else {
                throw new TableMapException("Type mismatch for: { cell: " + cell.getClass() + ", column: " + columnSchema.getDataType());
            }
        } else if (cell instanceof LongCell) {
            // MySQL int (4 bytes)
            LongCell lc = (LongCell) cell;
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
                throw new TableMapException("Unknown MySQL type in the event" + cell.getClass() + " Object = " + cell);
            }
        } else if (cell instanceof LongLongCell) {
            // MySQL BigInt (8 bytes)
            LongLongCell llc = (LongLongCell) cell;
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
                        + " in the event " + cell.getClass()
                        + " Object = " + cell
                );
            }
        } else if (cell instanceof YearCell) {
            // ================================================================
            // Date&Time types
            // ================================================================
            YearCell yc = (YearCell) cell;
            return yc.toString();
        } else if (cell instanceof DateCell) {
            DateCell dc =  (DateCell) cell;

            /** A workaround for the bug in the open replicator's "0000-00-00" date parsing logic:
             *  according to MySQL spec, this date is invalid and has a special treatment in jdbc
             */
            return dc.getValue().equals(ZERO_DATE) ? "NULL" : dc.toString();
        } else if (cell instanceof DatetimeCell) {
            DatetimeCell dc = (DatetimeCell) cell;
            return dc.toString();
            // Bug in OR for DateTIme and Time data-types.
            // MilliSeconds is not available for these columns but is set with currentMillis() wrongly.
            // TODO: check if this bug exists in binlog connector
        } else if (cell instanceof Datetime2Cell) {
            Datetime2Cell d2c = (Datetime2Cell) cell;
            return d2c.toString();
        } else if (cell instanceof TimeCell) {
            TimeCell tc = (TimeCell) cell;
            return tc.toString();
            /**
             * There is a bug in OR where instead of using the default year as 1970, it is using 0070.
             * This is a temporary measure to resolve it by working around at this layer.
             * The value obtained from OR is subtracted from "0070-00-01 00:00:00"
             */
            // TODO: check if this bug is exists in binlog connector
        } else if (cell instanceof  Time2Cell) {
            Time2Cell t2c = (Time2Cell) cell;
            return t2c.toString();
        } else if (cell instanceof TimestampCell) {
            TimestampCell tsc = (TimestampCell) cell;
            Long timestampValue = tsc.getValue().getTime();

            return String.valueOf(timestampValue);
        } else if (cell instanceof Timestamp2Cell   ) {
            Timestamp2Cell ts2c = (Timestamp2Cell) cell;
            Long timestamp2Value = ts2c.getValue().getTime();
            return String.valueOf(timestamp2Value);
        } else {
            if (cell != null) {
                throw new TableMapException("Unknown MySQL type in the event" + cell.getClass() + " Object = " + cell);
            } else {
                throw new TableMapException("cell object is null");
            }
        }
    }
}
