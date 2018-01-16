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

    // Variations of text cell schema
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
    public void stringToString() throws TableMapException {
        String testString = "test";
        Cell stringCell = StringCell.valueOf(testString.getBytes(StandardCharsets.UTF_8));
        ColumnSchema stringSchema = new ColumnSchema();
        stringSchema.setCharacterSetName("utf8");

        assertEquals(testString, Converter.cellValueToString(stringCell, stringSchema));
    }

    @Test
    public void nullToString() throws TableMapException {
        Cell nullCell = NullCell.valueOf(0);
        ColumnSchema emptySchema = new ColumnSchema();

        assertEquals("NULL", Converter.cellValueToString(nullCell, emptySchema));
    }

//    @Test(expected = Exception.class) // TODO: Specify exception type
//    public void doubleCellWithTextSchema() throws TableMapException {
//        Cell c = new DoubleCell(1.0);
//        ColumnSchema s = new ColumnSchema();
//        s.setColumnType("text");
//
//        Converter.cellValueToString(c, s);
//    }
}