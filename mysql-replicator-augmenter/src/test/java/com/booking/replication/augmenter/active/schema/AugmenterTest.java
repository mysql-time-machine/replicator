package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.ActiveSchemaHelpers;
import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.filters.TableNameMergePatternFilter;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class AugmenterTest {

    @Test
    public void testAugmenterFilter()  {

               String TABLE_NAME = "MyTable_201912";

        String AUGMENTER_FILTER_TYPE = "TABLE_MERGE_PATTERN";
        String AUGMENTER_FILTER_CONFIGURATION = "([_][12]\\d{3}(0[1-9]|1[0-2]))";


        Map<String, Object> configuration = new HashMap<>();

        configuration.put(AugmenterFilter.Configuration.FILTER_TYPE, AUGMENTER_FILTER_TYPE);
        configuration.put(AugmenterFilter.Configuration.FILTER_CONFIGURATION, AUGMENTER_FILTER_CONFIGURATION);

        TableNameMergePatternFilter augmenterFilter = new TableNameMergePatternFilter(configuration);

        String rewritenName = augmenterFilter.getRewrittenName(TABLE_NAME);

        assertEquals(rewritenName, "MyTable");

    }

    @Test
    public void rewriteActiveSchemaNameTest() throws Exception {

        String query_1    = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_1 = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";

        String query_2    = "CREATE TABLE test.test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_2 = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";

        String query_3    = "CREATE TABLE `test`.`test` (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_3 = "CREATE TABLE `test` (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";

        String query_4 = "RENAME TABLE `test`.`SomeTable` TO `test`.`SomeTable_old`, `test`.`SomeTable_new` TO `test`.`SomeTable`";
        String expected_4 = "RENAME TABLE `SomeTable` TO `SomeTable_old`, `SomeTable_new` TO `SomeTable`";

        String replicantDbName = "test";

        String rewritten_1 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_1,replicantDbName);
        String rewritten_2 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_2,replicantDbName);
        String rewritten_3 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_3,replicantDbName);
        String rewritten_4 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_4,replicantDbName);


        assertEquals(expected_1, rewritten_1);
        assertEquals(expected_2, rewritten_2);
        assertEquals(expected_3, rewritten_3);
        assertEquals(expected_4, rewritten_4);

    }
}
