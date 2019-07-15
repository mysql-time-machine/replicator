package com.booking.replication.augmenter.model.format;

import com.booking.replication.augmenter.model.schema.DataType;

import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.BitSet;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;

public class MysqlTypeStringifierTest {

    @Test
    public void testBitType() {
        DataType dataType = DataType.BIT;
        String columnType = "bit(5)";

        BitSet testBit;
        String expected, actual;

        {
            testBit     = new BitSet();
            testBit.set(0);
            testBit.set(3);
            testBit.set(4);

            expected    = "11001";

            actual = MysqlTypeStringifier.convertToString(testBit, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testBit     = new BitSet();
            expected    = "0";

            actual = MysqlTypeStringifier.convertToString(testBit, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testDateType() {
        DataType dataType = DataType.DATE;
        String columnType = "date";

        Date testDate;
        String expected, actual;

        {
            testDate   = new Date(2019 - 1900, Calendar.FEBRUARY, 1);
            expected    = "2019-02-01";

            actual = MysqlTypeStringifier.convertToString(testDate, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testDate   = new Date(2019 - 1900, Calendar.DECEMBER, 31);
            expected    = "2019-12-31";

            actual = MysqlTypeStringifier.convertToString(testDate, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testTimestampType() {
        DataType dataType = DataType.TIMESTAMP;
        String columnType = "timestamp";

        Timestamp testTimestamp;
        String expected, actual;

        {
            Long timeStamp = 1577797200000L;
            testTimestamp = new Timestamp(timeStamp);

            String tzId = ZonedDateTime.now().getZone().toString();
            ZoneId zoneId = ZoneId.of(tzId);
            LocalDateTime aLDT = Instant.ofEpochMilli(timeStamp).atZone(zoneId).toLocalDateTime();
            Integer offset  = ZonedDateTime.from(aLDT.atZone(ZoneId.of(tzId))).getOffset().getTotalSeconds();
            expected = String.valueOf(timeStamp - offset * 1000);

            actual = MysqlTypeStringifier.convertToString(testTimestamp, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testEnumType() {
        DataType dataType = DataType.ENUM;
        String columnType = "enum('apple','banana','orange')";

        String[] groupValues = new String[] {"apple", "banana", "orange"};

        Integer testValue;
        String expected, actual;

        {
            testValue   = 1;
            expected    = "apple";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 2;
            expected    = "banana";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 3;
            expected    = "orange";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSetType() {
        DataType dataType = DataType.SET;
        String columnType = "set('apple','banana','orange')";

        String[] groupValues = new String[] {"apple", "banana", "orange"};

        Long testValue;
        String expected, actual;

        {
            testValue   = 1L;
            expected    = "apple";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 2L;
            expected    = "banana";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 3L;
            expected    = "apple,banana";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 4L;
            expected    = "orange";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 5L;
            expected    = "apple,orange";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 6L;
            expected    = "banana,orange";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }

        {
            testValue   = 7L;
            expected    = "apple,banana,orange";

            actual = MysqlTypeStringifier.convertToString(testValue, null, dataType, columnType , groupValues);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedTinyInt() {
        DataType dataType = DataType.TINYINT;
        String columnType = "tinyint(4)";

        Integer testTinyInteger;
        String expected, actual;

        {
            testTinyInteger = 0;
            expected        = "0";

            actual = MysqlTypeStringifier.convertToString(testTinyInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = 127;
            expected        = "127";

            actual = MysqlTypeStringifier.convertToString(testTinyInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = -128;
            expected        = "-128";

            actual = MysqlTypeStringifier.convertToString(testTinyInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedTinyInt() {
        DataType dataType = DataType.TINYINT;
        String columnType = "tinyint(3) unsigned";

        Integer testTinyInteger;
        String expected, actual;

        {
            testTinyInteger = 0;
            expected        = "0";

            actual = MysqlTypeStringifier.convertToString(testTinyInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = 127;
            expected        = "127";

            actual = MysqlTypeStringifier.convertToString(testTinyInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = -128;
            expected        = "128";

            actual = MysqlTypeStringifier.convertToString(testTinyInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testTinyInteger = -1;
            expected        = "255";

            actual = MysqlTypeStringifier.convertToString(testTinyInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedSmallInt() {
        DataType dataType = DataType.SMALLINT;
        String columnType = "smallint(6)";

        Integer testSmallInteger;
        String expected, actual;

        {
            testSmallInteger    = 0;
            expected            = "0";

            actual = MysqlTypeStringifier.convertToString(testSmallInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = 32767;
            expected            = "32767";

            actual = MysqlTypeStringifier.convertToString(testSmallInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = -32768;
            expected            = "-32768";

            actual = MysqlTypeStringifier.convertToString(testSmallInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedSmallInt() {
        DataType dataType = DataType.SMALLINT;
        String columnType = "smallint(5) unsigned";

        Integer testSmallInteger;
        String expected, actual;

        {
            testSmallInteger    = 0;
            expected            = "0";

            actual = MysqlTypeStringifier.convertToString(testSmallInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = 32767;
            expected            = "32767";

            actual = MysqlTypeStringifier.convertToString(testSmallInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = -32768;
            expected            = "32768";

            actual = MysqlTypeStringifier.convertToString(testSmallInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testSmallInteger    = -1;
            expected            = "65535";

            actual = MysqlTypeStringifier.convertToString(testSmallInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedMediumInt() {
        DataType dataType = DataType.MEDIUMINT;
        String columnType = "mediumint(9)";

        Integer testMediumInteger;
        String expected, actual;

        {
            testMediumInteger   = 0;
            expected            = "0";

            actual = MysqlTypeStringifier.convertToString(testMediumInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = 8388607;
            expected            = "8388607";

            actual = MysqlTypeStringifier.convertToString(testMediumInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = -8388608;
            expected            = "-8388608";

            actual = MysqlTypeStringifier.convertToString(testMediumInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedMediumInt() {
        DataType dataType = DataType.MEDIUMINT;
        String columnType = "mediumint(8) unsigned";

        Integer testMediumInteger;
        String expected, actual;

        {
            testMediumInteger   = 0;
            expected            = "0";

            actual = MysqlTypeStringifier.convertToString(testMediumInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = 8388607;
            expected            = "8388607";

            actual = MysqlTypeStringifier.convertToString(testMediumInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = -8388608;
            expected            = "8388608";

            actual = MysqlTypeStringifier.convertToString(testMediumInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testMediumInteger   = -1;
            expected            = "16777215";

            actual = MysqlTypeStringifier.convertToString(testMediumInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedInt() {
        DataType dataType = DataType.INT;
        String columnType = "int(11)";

        Integer testInteger;
        String expected, actual;

        {
            testInteger = 0;
            expected    = "0";

            actual = MysqlTypeStringifier.convertToString(testInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testInteger = 2147483647;
            expected    = "2147483647";

            actual = MysqlTypeStringifier.convertToString(testInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testInteger = -2147483648;
            expected    = "-2147483648";

            actual = MysqlTypeStringifier.convertToString(testInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedInt() {
        DataType dataType = DataType.INT;
        String columnType = "int(10) unsigned";

        Integer testInteger;
        String expected, actual;

        {
            testInteger = 0;
            expected    = "0";

            actual = MysqlTypeStringifier.convertToString(testInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testInteger = 2147483647;
            expected    = "2147483647";

            actual = MysqlTypeStringifier.convertToString(testInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testInteger = -2147483648;
            expected    = "2147483648";

            actual = MysqlTypeStringifier.convertToString(testInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testInteger = -1;
            expected    = "4294967295";

            actual = MysqlTypeStringifier.convertToString(testInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testSignedBigInt() {
        DataType dataType = DataType.BIGINT;
        String columnType = "bigint(20)";

        Long testBigInteger;
        String expected, actual;

        {
            testBigInteger  = 0L;
            expected        = "0";

            actual = MysqlTypeStringifier.convertToString(testBigInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = 9223372036854775807L;
            expected        = "9223372036854775807";

            actual = MysqlTypeStringifier.convertToString(testBigInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = -9223372036854775808L;
            expected        = "-9223372036854775808";

            actual = MysqlTypeStringifier.convertToString(testBigInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testUnsignedBigInt() {
        DataType dataType = DataType.BIGINT;
        String columnType = "bigint(20) unsigned";

        Long testBigInteger;
        String expected, actual;

        {
            testBigInteger  = 0L;
            expected        = "0";

            actual = MysqlTypeStringifier.convertToString(testBigInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = 9223372036854775807L;
            expected        = "9223372036854775807";

            actual = MysqlTypeStringifier.convertToString(testBigInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = -9223372036854775808L;
            expected        = "9223372036854775808";

            actual = MysqlTypeStringifier.convertToString(testBigInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }

        {
            testBigInteger  = -1L;
            expected        = "18446744073709551615";

            actual = MysqlTypeStringifier.convertToString(testBigInteger, null, dataType, columnType , null);
            assertEquals(expected, actual);
        }
    }
}
