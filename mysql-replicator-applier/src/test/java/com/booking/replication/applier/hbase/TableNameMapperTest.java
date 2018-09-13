package com.booking.replication.applier.hbase;

import com.booking.replication.applier.hbase.schema.TableNameMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by bosko on 3/29/16.
 */
public class TableNameMapperTest {

    @Test
    public void testNameGenCaseInitialSnapshot() throws Exception {

        long   timestampMicrosec  = 1459246480123456L;
        String dbName             = "TestDB";
        String tableName          = "Sometable";
        boolean isInitialSnapshot = true;

        String deltaTableName = TableNameMapper.getCurrentDeltaTableName(
                timestampMicrosec,
                dbName,
                tableName,
                isInitialSnapshot
        );

        assertEquals("delta:" + dbName.toLowerCase() + "_" + tableName.toLowerCase() + "_" + "initial", deltaTableName);
    }

    @Test
    public void testNameGenCaseNotInitialSnapshot() throws Exception {

        long   timestampMicrosec  = 1459246480123456L;
        String dbName             = "TestDB";
        String tableName          = "Sometable";
        boolean isInitialSnapshot = false;

        String deltaTableName = TableNameMapper.getCurrentDeltaTableName(
                timestampMicrosec,
                dbName,
                tableName,
                isInitialSnapshot
        );

        assertEquals("delta:" + dbName.toLowerCase() + "_" + tableName.toLowerCase() + "_" + "20160329", deltaTableName);
    }
}
