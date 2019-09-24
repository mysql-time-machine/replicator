package com.booking.replication.augmenter.model.format;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.DataType;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class MysqlTypeDeserializerTest {

    @Test
    public void testBinaryType() {
        ColumnSchema schema = new ColumnSchema("code", DataType.BINARY, "binary(10)", true, "", "");
        schema.setCharMaxLength(10);

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "6F72616E676500000000";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "4F72616E676500000000";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testVarBinaryType() {
        ColumnSchema schema = new ColumnSchema("code", DataType.VARBINARY, "binary(10)", true, "", "");
        schema.setCharMaxLength(10);

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "6F72616E676500000000";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "4F72616E676500000000";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testTinyBlobType() {
        ColumnSchema schema = new ColumnSchema("code", DataType.TINYBLOB, "tinyblob", true, "", "");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "6F72616E6765";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testMediumBlobType() {
        ColumnSchema schema = new ColumnSchema("code", DataType.MEDIUMBLOB, "mediumblob", true, "", "");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "6F72616E6765";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testBlobType() {
        ColumnSchema schema = new ColumnSchema("code", DataType.BLOB, "blob", true, "", "");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "6F72616E6765";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testLongBlobType() {
        ColumnSchema schema = new ColumnSchema("code", DataType.LONGBLOB, "longblob", true, "", "");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "6F72616E6765";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testCharTypeLatinCharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.CHAR, "char(30)", true, "", "");
        schema.setCollation("latin1_swedish_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -19, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -23, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testCharTypeUtf8CharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.CHAR, "char(30)", true, "", "");
        schema.setCollation("utf8_general_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -61, -83, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -61, -87, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {-26, -105, -87, -28, -72, -118, -27, -91, -67};
            expected    = "早上好";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testVarcharTypeLatinCharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.VARCHAR, "varchar(30)", true, "", "");
        schema.setCollation("latin1_swedish_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -19, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -23, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testVarcharTypeUtf8CharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.VARCHAR, "varchar(30)", true, "", "");
        schema.setCollation("utf8_general_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -61, -83, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -61, -87, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {-26, -105, -87, -28, -72, -118, -27, -91, -67};
            expected    = "早上好";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testTextTypeLatinCharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.TEXT, "text", true, "", "");
        schema.setCollation("latin1_swedish_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema , null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -19, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -23, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testTextTypeUtf8CharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.TEXT, "text", true, "", "");
        schema.setCollation("utf8_general_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -61, -83, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -61, -87, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {-26, -105, -87, -28, -72, -118, -27, -91, -67};
            expected    = "早上好";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testMediumTextTypeLatinCharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.MEDIUMTEXT, "mediumtext", true, "", "");
        schema.setCollation("latin1_swedish_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -19, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -23, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testMediumTextTypeUtf8CharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.MEDIUMTEXT, "mediumtext", true, "", "");
        schema.setCollation("utf8_general_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -61, -83, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -61, -87, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {-26, -105, -87, -28, -72, -118, -27, -91, -67};
            expected    = "早上好";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testTinyTextTypeLatinCharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.TINYTEXT, "tinytext", true, "", "");
        schema.setCollation("latin1_swedish_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -19, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -23, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testTinyTextTypeUtf8CharacterSet() {
        ColumnSchema schema = new ColumnSchema("name", DataType.TINYTEXT, "tinytext", true, "", "");
        schema.setCollation("utf8_general_ci");

        byte[] testByteArr;
        String expected;
        Object actual;

        {
            testByteArr = new byte[] {111, 114, 97, 110, 103, 101};
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 114, 97, 110, 103, 101};
            expected    = "Orange";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {79, 82, 65, 78, 71, 69};
            expected    = "ORANGE";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {66, 117, 101, 110, 111, 115, 32, 100, -61, -83, 97, 115};
            expected    = "Buenos días";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {98, 111, 110, 110,101, 32, 106, 111, 117, 114, 110, -61, -87, 101};
            expected    = "bonne journée";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }

        {
            testByteArr = new byte[] {-26, -105, -87, -28, -72, -118, -27, -91, -67};
            expected    = "早上好";

            actual = MysqlTypeDeserializer.convertToObject(testByteArr, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testBitType() {
        ColumnSchema schema = new ColumnSchema("id", DataType.BIT, "bit(5)", true, "", "");

        BitSet testBit;
        String expected;
        Object actual;

        {
            testBit     = new BitSet();
            testBit.set(0);
            testBit.set(3);
            testBit.set(4);

            expected    = "11001";

            actual = MysqlTypeDeserializer.convertToObject(testBit, schema, null);
            assertEquals(expected, actual);
        }

        {
            testBit     = new BitSet();
            expected    = "0";

            actual = MysqlTypeDeserializer.convertToObject(testBit, schema, null);
            assertEquals(expected, actual);
        }
    }
/*
    TODO: Rethink these tests as they were written with the DATE_AND_TIME_AS_LONG configuration
          option in mind.
    @Test
    public void testDateType() {
        DataType dataType = DataType.DATE;
        String columnType = "date";
        ColumnSchema schema = new ColumnSchema("date",dataType,columnType, true, "","");
        Date testDate;
        String expected;
        Object actual;
        {
            testDate   = new Date(2019 - 1900, Calendar.FEBRUARY, 1);
            expected    = "2019-02-01";
            actual = MysqlTypeDeserializer.convertToObject(testDate, schema, null);
            assertEquals(expected, actual);
        }
        {
            testDate   = new Date(2019 - 1900, Calendar.DECEMBER, 31);
            expected    = "2019-12-31";
            actual = MysqlTypeDeserializer.convertToObject(testDate, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testTimeType() {
        ColumnSchema schema = new ColumnSchema("ts", DataType.TIME, "time(3)", true, "", "");

        Long testTime;
        String expected;
        Object actual;

        {
            testTime    = 42972123L;
            expected    = "11:56:12.123";

            actual = MysqlTypeDeserializer.convertToObject(testTime, schema, null);
            assertEquals(expected, actual);
        }
    }
*/

    @Test
    public void testDateTimeType() {
        ColumnSchema schema = new ColumnSchema("ts", DataType.DATETIME, "datetime", true, "", "");
        Long epochUTC = 1548982800000L;

        TimeZone tz = TimeZone.getDefault();
        int offset = tz.getOffset(new Date(epochUTC).getTime() );

        String expected;
        Object actual;

        {
            expected        = String.valueOf( epochUTC - offset );
            actual = MysqlTypeDeserializer.convertToObject(epochUTC, schema, null);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void testTimestampType() {
        ColumnSchema schema = new ColumnSchema("ts", DataType.TIMESTAMP, "timestamp(3)", true, "", "");
        Long epochUTC = 1548982800000L;

        TimeZone tz = TimeZone.getDefault();
        int offset = tz.getOffset(new Date(epochUTC).getTime() );

        String expected;
        Object actual;

        {
            expected        = String.valueOf( epochUTC - offset );
            actual = MysqlTypeDeserializer.convertToObject(epochUTC, schema, null);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void testEnumType() {
        ColumnSchema schema = new ColumnSchema("fruit", DataType.ENUM, "enum('apple','banana','orange')", true, "", "");

        String[] groupValues = new String[] {"apple", "banana", "orange"};

        Integer testValue;
        String expected;
        Object actual;

        {
            testValue   = 1;
            expected    = "apple";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 2;
            expected    = "banana";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 3;
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSetType() {
        ColumnSchema schema = new ColumnSchema("fruit", DataType.SET, "set('apple','banana','orange')", true, "", "");

        String[] groupValues = new String[] {"apple", "banana", "orange"};

        Long testValue;
        String expected;
        Object actual;

        {
            testValue   = 1L;
            expected    = "apple";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 2L;
            expected    = "banana";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 3L;
            expected    = "apple,banana";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 4L;
            expected    = "orange";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 5L;
            expected    = "apple,orange";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 6L;
            expected    = "banana,orange";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 7L;
            expected    = "apple,banana,orange";

            actual = MysqlTypeDeserializer.convertToObject(testValue, schema, groupValues);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedTinyInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.TINYINT, "tinyint(4)", true, "", "");

        Integer testTinyInteger;
        Long expected;
        Object actual;

        {
            testTinyInteger = 0;
            expected        = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testTinyInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = 127;
            expected        = 127L;

            actual = MysqlTypeDeserializer.convertToObject(testTinyInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = -128;
            expected        = -128L;

            actual = MysqlTypeDeserializer.convertToObject(testTinyInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedTinyInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.TINYINT, "tinyint(4) unsigned", true, "", "");

        Integer testTinyInteger;
        Long expected;
        Object actual;

        {
            testTinyInteger = 0;
            expected        = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testTinyInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = 127;
            expected        = 127L;

            actual = MysqlTypeDeserializer.convertToObject(testTinyInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = -128;
            expected        = 128L;

            actual = MysqlTypeDeserializer.convertToObject(testTinyInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = -1;
            expected        = 255L;

            actual = MysqlTypeDeserializer.convertToObject(testTinyInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedSmallInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.SMALLINT, "smallint(6)", true, "", "");

        Integer testSmallInteger;
        Long expected;
        Object actual;

        {
            testSmallInteger    = 0;
            expected            = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testSmallInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = 32767;
            expected            = 32767L;

            actual = MysqlTypeDeserializer.convertToObject(testSmallInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = -32768;
            expected            = -32768L;

            actual = MysqlTypeDeserializer.convertToObject(testSmallInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedSmallInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.SMALLINT, "smallint(6) unsigned", true, "", "");

        Integer testSmallInteger;
        Long expected;
        Object actual;

        {
            testSmallInteger    = 0;
            expected            = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testSmallInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = 32767;
            expected            = 32767L;

            actual = MysqlTypeDeserializer.convertToObject(testSmallInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = -32768;
            expected            = 32768L;

            actual = MysqlTypeDeserializer.convertToObject(testSmallInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = -1;
            expected            = 65535L;

            actual = MysqlTypeDeserializer.convertToObject(testSmallInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedMediumInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.MEDIUMINT, "mediumint(9)", true, "", "");

        Integer testMediumInteger;
        Long expected;
        Object actual;

        {
            testMediumInteger   = 0;
            expected            = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testMediumInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = 8388607;
            expected            = 8388607L;

            actual = MysqlTypeDeserializer.convertToObject(testMediumInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = -8388608;
            expected            = -8388608L;

            actual = MysqlTypeDeserializer.convertToObject(testMediumInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedMediumInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.MEDIUMINT, "mediumint(9) unsigned", true, "", "");

        Integer testMediumInteger;
        Long expected;
        Object actual;

        {
            testMediumInteger   = 0;
            expected            = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testMediumInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = 8388607;
            expected            = 8388607L;

            actual = MysqlTypeDeserializer.convertToObject(testMediumInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = -8388608;
            expected            = 8388608L;

            actual = MysqlTypeDeserializer.convertToObject(testMediumInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = -1;
            expected            = 16777215L;

            actual = MysqlTypeDeserializer.convertToObject(testMediumInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.INT, "int(11)", true, "", "");

        Integer testInteger;
        Long expected;
        Object actual;

        {
            testInteger = 0;
            expected    = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testInteger = 2147483647;
            expected    = 2147483647L;

            actual = MysqlTypeDeserializer.convertToObject(testInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testInteger = -2147483648;
            expected    = -2147483648L;

            actual = MysqlTypeDeserializer.convertToObject(testInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.INT, "int(10) unsigned", true, "", "");

        Integer testInteger;
        Long expected;
        Object actual;

        {
            testInteger = 0;
            expected    = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testInteger = 2147483647;
            expected    = 2147483647L;

            actual = MysqlTypeDeserializer.convertToObject(testInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testInteger = -2147483648;
            expected    = 2147483648L;

            actual = MysqlTypeDeserializer.convertToObject(testInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testInteger = -1;
            expected    = 4294967295L;

            actual = MysqlTypeDeserializer.convertToObject(testInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedBigInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.BIGINT, "bigint(20)", true, "", "");

        Long testBigInteger;
        Long expected;
        Object actual;

        {
            testBigInteger  = 0L;
            expected        = 0L;

            actual = MysqlTypeDeserializer.convertToObject(testBigInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = 9223372036854775807L;
            expected        = 9223372036854775807L;

            actual = MysqlTypeDeserializer.convertToObject(testBigInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = -9223372036854775808L;
            expected        = -9223372036854775808L;

            actual = MysqlTypeDeserializer.convertToObject(testBigInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedBigInt() {
        ColumnSchema schema = new ColumnSchema("id", DataType.BIGINT, "bigint(20) unsigned", true, "", "");

        Long testBigInteger;
        BigInteger expected;
        Object actual;

        {
            testBigInteger  = 0L;
            expected        = new BigInteger("0");

            actual = MysqlTypeDeserializer.convertToObject(testBigInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = 9223372036854775807L;
            expected        = new BigInteger("9223372036854775807");

            actual = MysqlTypeDeserializer.convertToObject(testBigInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = -9223372036854775808L;
            expected        = new BigInteger("9223372036854775808");

            actual = MysqlTypeDeserializer.convertToObject(testBigInteger, schema, null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = -1L;
            expected        = new BigInteger("18446744073709551615");

            actual = MysqlTypeDeserializer.convertToObject(testBigInteger, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testBigDecimal() {
        ColumnSchema schema = new ColumnSchema("currency", DataType.DECIMAL, "decimal(5,3)", true, "", "");

        BigDecimal testBigDecimal;
        String expected;
        Object actual;

        {
            testBigDecimal  = new BigDecimal("99.122");
            expected        = "99.122";

            actual = MysqlTypeDeserializer.convertToObject(testBigDecimal, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testFloat() {
        ColumnSchema schema = new ColumnSchema("currency", DataType.FLOAT, "float(5,3)", true, "", "");

        Float testFloat;
        Float expected;
        Object actual;

        {
            testFloat   = new Float("99.122");
            expected    = new Float(99.122);

            actual = MysqlTypeDeserializer.convertToObject(testFloat, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testDouble() {
        ColumnSchema schema = new ColumnSchema("currency", DataType.DOUBLE, "double(5,3)", true, "", "");

        Double testDouble;
        Double expected;
        Object actual;

        {
            testDouble  = new Double("99.122");
            expected    = 99.122;

            actual = MysqlTypeDeserializer.convertToObject(testDouble, schema, null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testJson() {
        ColumnSchema schema = new ColumnSchema("jsn", DataType.JSON, "json", true, "", "");

        byte[] testJson;
        String expected;
        Object actual;

        {
            testJson  = new byte[]{0, 3, 0, 77, 0, 25, 0, 2, 0, 27, 0, 4, 0, 31, 0, 10, 0, 12, 41, 0, 12, 49, 0, 0, 57,
                    0, 111, 115, 110, 97, 109, 101, 114, 101, 115, 111, 108, 117, 116, 105, 111, 110, 7, 87, 105, 110,
                    100, 111, 119, 115, 7, 70, 105, 114, 101, 102, 111, 120, 2, 0, 20, 0, 18, 0, 1, 0, 19, 0, 1, 0, 5,
                    0, 10, 5, 64, 6, 120, 121};

            expected    = "{\"os\":\"Windows\",\"name\":\"Firefox\",\"resolution\":{\"x\":2560,\"y\":1600}}";

            actual = MysqlTypeDeserializer.convertToObject(testJson, schema, null);
            assertEquals(expected, actual);
        }


    }
}
