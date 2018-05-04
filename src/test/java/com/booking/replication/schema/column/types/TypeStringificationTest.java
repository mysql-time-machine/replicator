package com.booking.replication.schema.column.types;

import com.booking.replication.Configuration;
import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.cell.*;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.exception.TableMapException;

import com.booking.replication.schema.column.types.TypeConversionRules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

public class TypeStringificationTest {

    ////////////////////////////////////////////
    // INTEGER CELLS
    private static TypeConversionRules typeConversionRules =
        new TypeConversionRules(
            getConfiguration()
        );

    private static Configuration getConfiguration() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        String config =
                "replication_schema:\n" +
                        "    name:      'test'\n" +
                        "    username:  '__USER__'\n" +
                        "    password:  '__PASS__'\n" +
                        "    host_pool: ['localhost2', 'localhost']\n" +
                        "metadata_store:\n" +
                        "    username: '__USER__'\n" +
                        "    password: '__PASS__'\n" +
                        "    host:     'localhost'\n" +
                        "    database: 'test_active_schema'\n" +
                        "    file:\n" +
                        "        path: '/opt/replicator/replicator_metadata'\n" +
                        "kafka:\n" +
                        "    broker: \"kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092\"\n" +
                        "    topic:  test\n" +
                        "    tables: [\"sometable\"]\n" +
                        "converter:\n" +
                        "    stringify_null: 1\n" +
                        "mysql_failover:\n" +
                        "    pgtid:\n" +
                        "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                        "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        Configuration configuration = null;
        try {
            InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
            configuration = mapper.readValue(in, Configuration.class);
        } catch (Exception e) {
            assertTrue(false);
        }
        return configuration;
    }

    @Test
    public void tinyintSignedCell() throws TableMapException {
        int x = -125; // inside single byte
        Cell c = TinyCell.valueOf(x);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("tinyint");
        s.setDataType("tinyint"); // TODO : Remove

        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s, typeConversionRules));
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

        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s, typeConversionRules));
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

        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s, typeConversionRules));
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

        assertEquals(Integer.toString(x), Converter.cellValueToString(c, s, typeConversionRules));
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

        assertEquals(Long.toString(x), Converter.cellValueToString(c, s, typeConversionRules));
    }

    @Test
    public void bigintUnsignedCell() {
        // TODO
    }

    // TODO : Add tests for overflows

    ///////////////////////////////////////
    // REAL NUMBERS

    @Test
    public void doubleCell() throws TableMapException {
        Cell c = new DoubleCell(1.5);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("double");

        assertEquals("1.5", Converter.cellValueToString(c, s, typeConversionRules));
    }

    @Test
    public void floatCell() throws TableMapException {
        Cell c = new FloatCell((float)1.5);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("float");

        assertEquals("1.5", Converter.cellValueToString(c, s, typeConversionRules));
    }

    /////////////////////////////////////////////////
    // TIME AND DATE

    @Test
    public void datetimeCell() throws ParseException, TableMapException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date d = sdf.parse("16/01/2018");
        Cell c = new DatetimeCell(d);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("datetime");

        assertEquals(d.toString(), Converter.cellValueToString(c, s, typeConversionRules));
    }

    @Test
    public void datetime2Cell() throws ParseException, TableMapException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date d = sdf.parse("16/01/2018");
        Cell c = new Datetime2Cell(d);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("datetime");

        assertEquals(d.toString(), Converter.cellValueToString(c, s, typeConversionRules));
    }

    @Test
    public void dateCell() throws ParseException, TableMapException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date d = sdf.parse("16/01/2018");
        Cell c = new DatetimeCell(d);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("date");

        assertEquals(d.toString(), Converter.cellValueToString(c, s, typeConversionRules));
    }

    @Test
    public void yearCell() throws TableMapException {
        int year = 2018;
        Cell c = YearCell.valueOf(year);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("year");

        assertEquals(Integer.toString(year), Converter.cellValueToString(c, s, typeConversionRules));
    }

    @Test
    public void timeCell() throws TableMapException {
        long epoch = 1000000;
        java.sql.Time time = new java.sql.Time(epoch);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("time");

        assertEquals(Long.toString(epoch), Converter.cellValueToString(new TimeCell(time), s, typeConversionRules));
    }

    @Test
    public void time2Cell() throws TableMapException {
        long epoch = 555555;
        java.sql.Time time = new java.sql.Time(epoch);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("time");

        assertEquals(Long.toString(epoch), Converter.cellValueToString(new Time2Cell(time), s, typeConversionRules));
    }

    @Test
    public void timestampCell() throws TableMapException {
        long epoch = 666666;
        java.sql.Timestamp t = new java.sql.Timestamp(epoch);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("timestamp");

        assertEquals(
                Long.toString(epoch),
                Converter.cellValueToString(new TimestampCell(t), s, typeConversionRules)
        );
    }

    @Test
    public void timestamp2Cell() throws TableMapException {
        long epoch = 777777;
        java.sql.Timestamp t = new java.sql.Timestamp(epoch);
        ColumnSchema s = new ColumnSchema();
        s.setColumnType("timestamp");

        assertEquals(
                Long.toString(epoch),
                Converter.cellValueToString(new Timestamp2Cell(t), s, typeConversionRules));
    }

    ////////////////////////////////
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

    //////////////////////////////////////////
    // STRING CELLS

    @Test
    public void stringToString() throws TableMapException {
        String testString = "test";
        Cell stringCell = StringCell.valueOf(testString.getBytes(StandardCharsets.UTF_8));
        ColumnSchema stringSchema = new ColumnSchema();
        stringSchema.setCharacterSetName("utf8");

        assertEquals(testString, Converter.cellValueToString(stringCell, stringSchema, typeConversionRules));
    }

    //////////////////////////////
    // NULL
    
    @Test
    public void nullToString() throws TableMapException {
        Cell nullCell = NullCell.valueOf(0);
        ColumnSchema emptySchema = new ColumnSchema();

        assertEquals("NULL", Converter.cellValueToString(nullCell, emptySchema, typeConversionRules));
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