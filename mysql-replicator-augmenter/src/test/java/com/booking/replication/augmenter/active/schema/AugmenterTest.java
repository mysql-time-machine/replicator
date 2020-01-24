package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.ActiveSchemaHelpers;
import com.booking.replication.augmenter.AugmenterContext;
import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.filters.TableNameMergePatternFilter;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

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

        String replicantDbName = "test";

        String query_1    = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_1 = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";

        String query_2    = "CREATE TABLE test.test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_2 = "CREATE TABLE test (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";

        String query_3    = "CREATE TABLE `test`.`test` (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";
        String expected_3 = "CREATE TABLE `test` (i INT, c CHAR(10)) ENGINE = BLACKHOLE;";

        // table swap cases
        String query_4 = "RENAME TABLE `test`.`SomeTable` TO `test`.`SomeTable_old`, `test`.`SomeTable_new` TO `test`.`SomeTable`";
        String expected_4 = "RENAME TABLE `SomeTable` TO `SomeTable_old`, `SomeTable_new` TO `SomeTable`";

        String query_5 = "use `test`; RENAME TABLE `test`.`SomeTable` TO `test`.`SomeTable_old`, `test`.`SomeTable_new` TO `test`.`SomeTable`";
        String expected_5 = "RENAME TABLE `SomeTable` TO `SomeTable_old`, `SomeTable_new` TO `SomeTable`";

        String query_6 = "use test; RENAME TABLE `test`.`SomeTable` TO `test`.`SomeTable_old`, `test`.`SomeTable_new` TO `test`.`SomeTable`";
        String expected_6 = "RENAME TABLE `SomeTable` TO `SomeTable_old`, `SomeTable_new` TO `SomeTable`";

        String rewritten_1 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_1,replicantDbName);
        String rewritten_2 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_2,replicantDbName);
        String rewritten_3 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_3,replicantDbName);
        String rewritten_4 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_4,replicantDbName);
        String rewritten_5 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_5,replicantDbName);
        String rewritten_6 = ActiveSchemaHelpers.rewriteActiveSchemaName(query_6,replicantDbName);

        assertEquals(expected_1, rewritten_1);
        assertEquals(expected_2, rewritten_2);
        assertEquals(expected_3, rewritten_3);
        assertEquals(expected_4, rewritten_4);
        assertEquals(expected_5, rewritten_5);
        assertEquals(expected_6, rewritten_6);

    }

    @Test
    public void renameMultiSchemaPattern() {

        String replicatedSchema_1 = "test";
        String query_1 = "RENAME TABLE `test1` TO `a_second_test`.`test1_old_rename`, `a_second_test`.test1_new TO `test1`;";
        Pattern renameMultiSchemaPattern_1 = Pattern.compile(AugmenterContext.RENAME_MULTISCHEMA_PATTERN, Pattern.CASE_INSENSITIVE);
        boolean shouldProcess_1 = ActiveSchemaHelpers.getShouldProcess(query_1, renameMultiSchemaPattern_1, replicatedSchema_1);

        assertTrue("Should not process cross db renames", shouldProcess_1 == false);

        String replicatedSchema_2 = "test";
        String query_2 = "rename table a_second_test.test1 to test1_other_db;";
        Pattern renameMultiSchemaPattern_2 = Pattern.compile(AugmenterContext.RENAME_MULTISCHEMA_PATTERN, Pattern.CASE_INSENSITIVE);
        boolean shouldProcess_2 = ActiveSchemaHelpers.getShouldProcess(query_2, renameMultiSchemaPattern_2, replicatedSchema_2);

        assertTrue("Should not process cross db renames", shouldProcess_2 == false);

        String replicatedSchema_3 = "test";
        String query_3 = "rename table test.test1 to test1_old;";
        Pattern renameMultiSchemaPattern_3 = Pattern.compile(AugmenterContext.RENAME_MULTISCHEMA_PATTERN, Pattern.CASE_INSENSITIVE);
        boolean shouldProcess_3 = ActiveSchemaHelpers.getShouldProcess(query_3, renameMultiSchemaPattern_3, replicatedSchema_3);

        assertTrue("Should not process cross db renames", shouldProcess_3 == true);
    }
}
