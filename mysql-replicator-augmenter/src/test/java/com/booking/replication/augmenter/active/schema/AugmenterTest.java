package com.booking.replication.augmenter.active.schema;

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
}
