package com.booking.replication.model.cell;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.*;
import com.google.code.or.io.ExceedLimitException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.BitSet;

/**
 * Created by bosko on 7/13/17.
 */
public class CellExtractor {

    public static Cell extractCellFromBinlogConnectorColumn(Serializable serializable) throws Exception {
        Cell cell = null;

        // Columns can have one of the following types:
        //
        //  Integer
        //  Long
        //  Float
        //  Double
        //  java.util.BitSet
        //  java.util.Date
        //  java.math.BigDecimal
        //  java.sql.Timestamp
        //  java.sql.Date
        //  java.sql.Time
        //  String
        //  byte[]
        //
        // More details at: https://github.com/shyiko/mysql-binlog-connector-java/blob/3709c9668ffc732e053e0f93ca3a3610789b152c/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java

        Serializable column = serializable;

        if(column instanceof Integer){
            //  This can correspond to these MySQL types
            //
            //      {@link ColumnType#TINY}: Integer
            //      {@link ColumnType#SHORT}: Integer
            //      {@link ColumnType#LONG}: Integer
            //      {@link ColumnType#INT24}: Integer
            //      {@link ColumnType#YEAR}: Integer
            //      {@link ColumnType#ENUM}: Integer

            Integer cval = (Integer)column;

            // mysql-binlog-connector does not have separate classes for different mysql
            // int types, so mysql tiny_int, small_int, medium_int and int are all mapped
            // to java int type. This is different from open replicator which gets the type
            // from table_map event and wraps the value into the corresponding class.
            // There are two ways to solve this:
            //      1. Write a custom deserializer which will return typed values instead of
            //         Serializable
            //      2. Use type information from active_schema
            // Here we go with 2nd approach. That is why all int family values are
            // wrapped in LongCell (4bytes used) but later in schema map step they
            // will be interpreted according to mysql type from active_schema. This means
            // that atm. TinyCell, ShortCell and Int24Cell are not used.
            if (cval >= -128 && cval <=127){
                // tiny int
                cell = LongCell.valueOf(cval);
            }
            else if (cval >= -32768	&& cval <= 32767) {
                // small int
                cell = LongCell.valueOf(cval);
            }
            else if (cval >=-8388608 && cval <=	8388607) {
                // medium int
                cell = LongCell.valueOf(cval);
            }
            else if (cval >= -2147483648 && cval <= 2147483647) {
                // int
                cell = LongCell.valueOf(cval);
            }
            else {
                throw new ExceedLimitException("Impossible case for:" + column);
            }
        }
        else if(column instanceof Long){
            // This can correspond to these MySQL types
            //
            //      {@link ColumnType#SET}: Long
            //      {@link ColumnType#LONGLONG}: Long
            //
            // We can not determine that based on value allone so we store
            // it as LongLongCell, but later during schema matching we can check
            // for this type is it mysql long or mysql set.
            //
            // Note: mysql-binlog-connector uses the type info from TableMapEvent
            //       to determine how to deserialize event, but deserialization
            //       will only give us the the final value, not the type of the
            //       column.

            Long cval = (Long)column;
            cell = LongLongCell.valueOf(cval);
        }
        else if(column instanceof Float){
            // This can correspond to these MySQL types
            //      {@link ColumnType#FLOAT}: Float
            cell = new FloatCell(((Float)column));
        }
        else if(column instanceof Double){
            // This can correspond to these MySQL types
            //      {@link ColumnType#DOUBLE}: Double
            cell = new DoubleCell((Double)column);
        }
        else if(column instanceof BitSet){
            // This can correspond to these MySQL types
            //      {@link ColumnType#BIT}: java.util.BitSet
            cell =  BitCell.valueOf(
                    ((BitSet) column).length(),
                    ((BitSet) column).toByteArray()
            );
        }
        else if(column instanceof java.util.Date){
            // This can correspond to these MySQL types
            //
            //      {@link ColumnType#DATETIME}: java.util.Date
            //      {@link ColumnType#DATETIME_V2}: java.util.Date
            //
            // DATETIME_V2 is the storage format with support for fractional part.
            // For now we treat them both as DATETIME_V2.
            // TODO: add a check if there is fractional seconds part to distinguish between DATETIME_V2 and DATETIME
            cell = new Datetime2Cell((java.util.Date)column);
        }
        else if(column instanceof BigDecimal){
            // This can correspond to these MySQL types
            //      {@link ColumnType#NEWDECIMAL}:
            BigDecimal bd = (BigDecimal)column;
            int precision = bd.precision();
            int scale = bd.scale();
            cell = new DecimalCell(bd,precision,scale);
        }
        else if(column instanceof java.sql.Timestamp){
            // This can correspond to these MySQL types
            //      {@link ColumnType#TIMESTAMP}: java.sql.Timestamp
            //      {@link ColumnType#TIMESTAMP_V2}: java.sql.Timestamp

            // TODO: add integration test for case when timezone status variable is used
            cell = new TimestampCell((java.sql.Timestamp)column);
        }
        else if(column instanceof java.sql.Date){
            cell = new DateCell((java.sql.Date)column);
        }
        else if(column instanceof java.sql.Time){
            // TODO: add integration test for case when timezone status variable is used
            cell = new Time2Cell((java.sql.Time)column);
        }
        else if(column instanceof String){
            // This can correspond to these MySQL types
            cell = StringCell.valueOf(((String) column).getBytes());
            // TODO: tests & explicit handling for different mysql encodings
        }
        else if(column instanceof byte[]){

            // This can correspond to these MySQL types
            //
            //      {@link ColumnType#BLOB}: byte[]
            //      {@link ColumnType#GEOMETRY}: byte[]
            //
            // and
            //      ColumnType#JSON: byte[]
            //
            // We treat all cases as Blobs (no explicit support for
            // JSON and Geometry)
            // TODO: add support for JSON and Geometry types
            cell = new BlobCell((byte[]) column);

        } else {
            throw new Exception("Unknown MySQL type in the BinlogConnector event" + column.getClass() + " Object = " + column);
        }
        return cell;
    }

    public static Cell extractCellFromOpenReplicatorColumn(Column column) throws Exception {
        Cell cell;
        if (column instanceof BitColumn) {
            cell =  BitCell.valueOf(((BitColumn)column).getLength(), ((BitColumn)column).getValue());
        }
        else if (column instanceof BlobColumn) {
            cell = new BlobCell((byte[]) ((BlobColumn)column).getValue());
        }
        else if (column instanceof StringColumn) {
            cell = StringCell.valueOf((byte[])((StringColumn)column).getValue());
        }
        else if (column instanceof DecimalColumn) {
            BigDecimal value = ((DecimalColumn)column).getValue();
            int precision = ((DecimalColumn)column).getPrecision();
            int scale = ((DecimalColumn)column).getScale();
            cell = new DecimalCell(value,precision,scale);
        }
        else if (column instanceof NullColumn) {
            cell = NullCell.valueOf(((NullColumn)column).getType());
        }
        else if (column instanceof SetColumn) {
            cell = new SetCell(((SetColumn)column).getValue());
        }
        else  if (column instanceof  EnumColumn) {
            cell = new EnumCell(((EnumColumn)column).getValue());
        }
        else if (column instanceof DoubleColumn) {
            cell = new DoubleCell(((DoubleColumn)column).getValue());
        }
        else if (column instanceof FloatColumn) {
            cell = new FloatCell(((FloatColumn)column).getValue());
        }
        else if (column instanceof TinyColumn) {
            cell = TinyCell.valueOf(((TinyColumn)column).getValue());
        }
        else if (column instanceof  ShortColumn) {
            cell = ShortCell.valueOf(((ShortColumn)column).getValue());
        }
        else if (column instanceof Int24Column) {
            cell = Int24Cell.valueOf(((Int24Column)column).getValue());
        }
        else if (column instanceof LongColumn) {
            cell = LongCell.valueOf(((LongColumn)column).getValue());
        }
        else if (column instanceof LongLongColumn) {
            cell = LongLongCell.valueOf(((LongLongColumn)column).getValue());
        }
        else if (column instanceof YearColumn) {
            cell = YearCell.valueOf(((YearColumn)column).getValue());
        }
        else if (column instanceof DateColumn) {
            cell = new DateCell(((DateColumn)column).getValue());
        }
        else if (column instanceof DatetimeColumn) {
            cell = new DatetimeCell(((DatetimeColumn)column).getValue());
        }
        else if (column instanceof Datetime2Column) {
            cell = new Datetime2Cell(((Datetime2Column)column).getValue());
        }
        else if (column instanceof TimeColumn) {
            cell = new TimeCell(((TimeColumn)column).getValue());
        }
        else if (column instanceof  Time2Column) {
            cell = new Time2Cell(((Time2Column)column).getValue());
        }
        else if (column instanceof TimestampColumn) {
            cell = new TimestampCell(((TimestampColumn)column).getValue());
        }
        else if (column instanceof Timestamp2Column) {
            cell = new Timestamp2Cell(((Timestamp2Column)column).getValue());
        } else {
            throw new Exception("Unknown MySQL type in the Open Replicator event" + column.getClass() + " Object = " + column);
        }
        return cell;
    }
}
