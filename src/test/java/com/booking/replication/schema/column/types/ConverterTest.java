package com.booking.replication.schema.column.types;

import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.cell.*;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.exception.TableMapException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

public class ConverterTest {

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // INTEGER CELLS

    @Test
    public void tinyintSignedCell() throws TableMapException {
        int x = -125; // inside single byte
        Cell c = TinyCell.valueOf(x);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("tinyint");
        s.setDataType("tinyint"); // TODO : Remove

        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s));
    }

    // TODO: Failing!!!
//    @Test
//    public void tinyintUnsignedCell() throws TableMapException {
//        int x = 225; // inside single byte
//        Cell c = TinyCell.valueOf(x);
//        ColumnSchema s = new ColumnSchema();
//        s.setColumnType("unsigned tinyint");
//        s.setDataType("tinyint"); // TODO : Remove
//
//        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s));
//    }

    @Test
    public void smallintCell() throws TableMapException {
        int x = -30000;
        Cell c = ShortCell.valueOf(x);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("smallint");
        s.setDataType("smallint"); // TODO: Remove

        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s));
    }

    @Test
    public void smallintUnsignedCell() {
        // TODO
    }

    @Test
    public void mediumintCell() throws TableMapException {
        int x = -2000000;
        Cell c = Int24Cell.valueOf(x);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("mediumint");
        s.setDataType("mediumint"); // TODO: Remove

        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s));
    }

    @Test
    public void mediumintUnsignedCell() {
        // TODO
    }

    @Test
    public void intCell() throws TableMapException {
        int x = -2000000000;
        Cell c = LongCell.valueOf(x);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("int");
        s.setDataType("int");

        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s));
    }

    @Test
    public void intUnsignedCell() {
        // TODO
    }

    @Test
    public void bigintCell() throws TableMapException {
        long x = -9000000000000000000L;
        Cell c = LongLongCell.valueOf(x);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("bigint");
        s.setDataType("bigint");

        assertEquals(Long.toString(x), Converter.cellValueToString(c, s));
    }

    @Test
    public void bigintUnsignedCell() {
        // TODO
    }

    // TODO : Add tests for overflows

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // REAL NUMBERS

    @Test
    public void doubleCell() throws TableMapException {
        Cell c = new DoubleCell(1.5);
        ColumnSchema s = new ColumnSchema();

        assertEquals("1.5", Converter.cellValueToString(c, s));
    }

    @Test
    public void floatCell() throws TableMapException {
        Cell c = new FloatCell((float)1.5);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("float");

        assertEquals("1.5", Converter.cellValueToString(c, s));
    }
    
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TIME AND DATE

    @Test
    public void datetimeCell() throws ParseException, TableMapException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date d = sdf.parse("16/01/2018");
        Cell c = new DatetimeCell(d);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("datetime");

        assertEquals(d.toString(), Converter.cellValueToString(c, s));
    }

    @Test
    public void datetime2Cell() throws ParseException, TableMapException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date d = sdf.parse("16/01/2018");
        Cell c = new Datetime2Cell(d);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("datetime");

        assertEquals(d.toString(), Converter.cellValueToString(c, s));
    }

    @Test
    public void dateCell() throws ParseException, TableMapException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date d = sdf.parse("16/01/2018");
        Cell c = new DatetimeCell(d);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("datetime");

        assertEquals(d.toString(), Converter.cellValueToString(c, s));
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // BLOB VARIATIONS

    @Test
    public void blobWithTextSchema() {
        Cell textCell = new BlobCell(null);

        // TODO: Add test
    }

    @Test
    public void blobWithTinytextSchema() {
        // TODO
    }

    @Test
    public void blobWithMediumtextSchema() {
        // TODO
    }

    @Test
    public void blobWithLongtextSchema() {
        // TODO
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // STRING CELLS
    
    @Test
    public void stringToString() throws TableMapException {
        String testString = "test";
        Cell stringCell = StringCell.valueOf(testString.getBytes(StandardCharsets.UTF_8));
        ColumnSchema stringSchema = new ColumnSchema();
        stringSchema.setCharacterSetName("utf8");

        assertEquals(testString, Converter.cellValueToString(stringCell, stringSchema));
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // NULL
    
    @Test
    public void nullToString() throws TableMapException {
        Cell nullCell = NullCell.valueOf(0);
        ColumnSchema emptySchema = new ColumnSchema();

        assertEquals("NULL", Converter.cellValueToString(nullCell, emptySchema));
    }
    
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // CELL TYPE AND SCHEMA CONFLICTS

//    @Test(expected = Exception.class) // TODO: Specify exception type
//    public void doubleCellWithTextSchema() throws TableMapException {
//        Cell c = new DoubleCell(1.0);
//        ColumnSchema s = new ColumnSchema();
//        s.setColumnType("text");
//
//        Converter.cellValueToString(c, s);
//    }
}