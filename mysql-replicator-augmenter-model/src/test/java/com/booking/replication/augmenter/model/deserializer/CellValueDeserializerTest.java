package com.booking.replication.augmenter.model.deserializer;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class CellValueDeserializerTest {
    @Test
    public void deserializeTinyInt() throws Exception {
        ColumnSchema columnSchema = new ColumnSchema("id", "tinyint(3) unsigned", "NULL", false, "", "NULL", "");
        HashMap<String, String[]> cache = new HashMap<>();
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, -35, "NULL");
        assertEquals(221, result);

        ColumnSchema columnSchema1 = new ColumnSchema("id", "tinyint(3) unsigned", "NULL", false, "", "NULL", "");
        HashMap<String, String[]> cache1 = new HashMap<>();
        Object result1 = CellValueDeserializer.deserialize(cache1, columnSchema1, -128, "NULL");
        assertEquals(128, result1);

        ColumnSchema columnSchema2 = new ColumnSchema("id", "tinyint(3)", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache2 = new HashMap<>();
        Object result2 = CellValueDeserializer.deserialize(cache2, columnSchema2, 127, "NULL");
        assertEquals(127, result2);
    }

    @Test
    public void deserializeSmallInt() throws Exception {
        ColumnSchema columnSchema = new ColumnSchema("id", "smallint(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache = new HashMap<>();
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, -32768, "NULL");
        assertEquals(32768, result);

        ColumnSchema columnSchema1 = new ColumnSchema("id", "smallint(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache1 = new HashMap<>();
        Object result1 = CellValueDeserializer.deserialize(cache1, columnSchema1, -32766, "NULL");
        assertEquals(32770, result1);

        ColumnSchema columnSchema2 = new ColumnSchema("id", "smallint(10)", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache2 = new HashMap<>();
        Object result2 = CellValueDeserializer.deserialize(cache2, columnSchema2, 32000, "NULL");
        assertEquals(32000, result2);
    }

    @Test
    public void deserializeMediumInt() throws Exception {
        ColumnSchema columnSchema = new ColumnSchema("id", "mediumint(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache = new HashMap<>();
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, -8388608, "NULL");
        assertEquals(8388608, result);

        ColumnSchema columnSchema1 = new ColumnSchema("id", "mediumint(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache1 = new HashMap<>();
        Object result1 = CellValueDeserializer.deserialize(cache1, columnSchema1, -8388607, "NULL");
        assertEquals(8388609, result1);

        ColumnSchema columnSchema2 = new ColumnSchema("id", "mediumint(10)", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache2 = new HashMap<>();
        Object result2 = CellValueDeserializer.deserialize(cache2, columnSchema2, 8388600, "NULL");
        assertEquals(8388600, result2);
    }

    @Test
    public void deserializeInt() throws Exception {
        ColumnSchema columnSchema = new ColumnSchema("id", "int(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache = new HashMap<>();
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, -2147483648, "NULL");
        assertEquals(2147483648L, result);

        ColumnSchema columnSchema1 = new ColumnSchema("id", "int(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache1 = new HashMap<>();
        Object result1 = CellValueDeserializer.deserialize(cache1, columnSchema1, -2147483647, "NULL");
        assertEquals(2147483649L, result1);

        ColumnSchema columnSchema2 = new ColumnSchema("id", "int(10)", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache2 = new HashMap<>();
        Object result2 = CellValueDeserializer.deserialize(cache2, columnSchema2, 2147483600, "NULL");
        assertEquals(2147483600, result2);
    }

    @Test
    public void deserializeBigInt() throws Exception {
        ColumnSchema columnSchema = new ColumnSchema("id", "bigint(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache = new HashMap<>();
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, 0x8000000000000000L, "NULL");
        assertEquals("9223372036854775808", result);

        ColumnSchema columnSchema1 = new ColumnSchema("id", "bigint(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache1 = new HashMap<>();
        Object result1 = CellValueDeserializer.deserialize(cache1, columnSchema1, 0x800000000000000fL, "NULL");
        assertEquals("9223372036854775823", result1);

        ColumnSchema columnSchema2 = new ColumnSchema("id", "bigint(10)", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache2 = new HashMap<>();
        Object result2 = CellValueDeserializer.deserialize(cache2, columnSchema2, 8388600, "NULL");
        assertEquals(8388600, result2);

        ColumnSchema columnSchema3 = new ColumnSchema("id", "bigint(10) unsigned", "NULL", false, "PRI", "NULL", "");
        HashMap<String, String[]> cache3 = new HashMap<>();
        Object result3 = CellValueDeserializer.deserialize(cache3, columnSchema3, 8388600L, "NULL");
        assertEquals("8388600", result3);
    }

    @Test
    public void deserializeString() throws Exception {

        ColumnSchema columnSchema = new ColumnSchema("name", "char(2)", "latin1_swedish_ci", true, "", "NULL", "");
        HashMap<String, String[]> cache = new HashMap<>();
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, "fr".getBytes(), "NULL");
        assertEquals("fr", result);

        ColumnSchema columnSchema1 = new ColumnSchema("name", "varchar(255)", "utf8_general_ci", true, "", "NULL", "");
        HashMap<String, String[]> cache1 = new HashMap<>();
        Object result1 = CellValueDeserializer.deserialize(cache1, columnSchema1, "foobar".getBytes(), "NULL");
        assertEquals("foobar", result1);

        ColumnSchema columnSchema3 = new ColumnSchema("name", "varchar(255)", "utf8_general_ci", true, "", "NULL", "");
        HashMap<String, String[]> cache3 = new HashMap<>();
        Object result3 = CellValueDeserializer.deserialize(cache3, columnSchema3, "sÃƒÂ¥".getBytes(), "NULL");
        assertEquals("sÃƒÂ¥", result3);

        ColumnSchema columnSchema2 = new ColumnSchema("name", "mediumtext(255)", "utf8_general_ci", true, "", "NULL", "");
        HashMap<String, String[]> cache2 = new HashMap<>();
        Object result2 = CellValueDeserializer.deserialize(cache2, columnSchema2, "Hello World".getBytes(), "NULL");
        assertEquals("Hello World", result2);
    }


    @Test
    public void deserializeBinary() throws Exception {
        ColumnSchema columnSchema = new ColumnSchema("name", "binary", "NULL", true, "", "NULL", "");
        HashMap<String, String[]> cache = new HashMap<>();
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, DatatypeConverter.parseHexBinary("008FFE"), "NULL");
        assertEquals("008ffe", result);

        ColumnSchema columnSchema1 = new ColumnSchema("name", "binary", "NULL", true, "", "NULL", "");
        HashMap<String, String[]> cache1 = new HashMap<>();
        Object result1 = CellValueDeserializer.deserialize(cache1, columnSchema1, DatatypeConverter.parseHexBinary("629472559AFB71431B9CEF17017AB620"), "NULL");
        assertEquals("629472559afb71431b9cef17017ab620", result1);

        ColumnSchema columnSchema2 = new ColumnSchema("name", "binary", "NULL", true, "", "NULL", "");
        HashMap<String, String[]> cache2 = new HashMap<>();
        Object result2 = CellValueDeserializer.deserialize(cache2, columnSchema2, DatatypeConverter.parseHexBinary("081287A7842247DD8EA2CFC11E8FE700"), "NULL");
        assertEquals("081287a7842247dd8ea2cfc11e8fe700", result2);

        ColumnSchema columnSchema3 = new ColumnSchema("name", "binary(4)", "NULL", true, "", "NULL", "");
        HashMap<String, String[]> cache3 = new HashMap<>();
        // Test padding zeros. if inserted value is not display width size( 4 in this case), mysql pads zeros at the end. But in binlog stream its still 3 bytes.
        // Test case for the fix
        Object result3 = CellValueDeserializer.deserialize(cache3, columnSchema3, new byte[]{1, 11, 15}, "NULL");
        assertEquals("010b0f00", result3);
    }

    @Test
    public void deserializeColumnTypeWithMysqlKeywords() throws Exception {
        ColumnSchema columnSchema = new ColumnSchema("name", "enum('boolean','integer','string','date','datetime','boolarray','intarray','stringarray','datearray','enum')", "NULL", true, "", "string", "");
        HashMap<String, String[]> cache = new HashMap<>();
        cache.put("enum('boolean','integer','string','date','datetime','boolarray','intarray','stringarray','datearray','enum')",
                new String[]{"boolean", "integer", "string", "date", "datetime", "boolarray", "intarray", "stringarray", "datearray", "enum"}
        );
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, 1, "NULL");
        assertEquals("boolean", result);

    }

    @Test
    public void deserializeUnhandled() throws Exception {

        ColumnSchema columnSchema = new ColumnSchema("name", "json", "NULL", true, "", "NULL", "");
        HashMap<String, String[]> cache = new HashMap<>();
        Object result = CellValueDeserializer.deserialize(cache, columnSchema, "{blha}hlab", "NULL");
        assertEquals("{blha}hlab", result);
    }


}