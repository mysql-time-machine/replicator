package com.booking.replication.augmenter;

import com.booking.replication.augmenter.EventAugmenter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by bdevetak on 3/12/18.
 */
public class EventAugmenterTest {

    @Test
    public void rewriteActiveSchemaNameTest() throws Exception {

        String query_1    = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_1 = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";

        String query_2    = "CREATE TABLE test.test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_2 = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";

        String query_3    = "CREATE TABLE `test`.`test` (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_3 = "CREATE TABLE `test` (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";


        String replicantDbName = "test";

        String rewritten_1 = EventAugmenter.rewriteActiveSchemaName(query_1,replicantDbName);
        String rewritten_2 = EventAugmenter.rewriteActiveSchemaName(query_2,replicantDbName);
        String rewritten_3 = EventAugmenter.rewriteActiveSchemaName(query_3,replicantDbName);


        assertEquals(expected_1, rewritten_1);
        assertEquals(expected_2, rewritten_2);
        assertEquals(expected_3, rewritten_3);

    }
}

