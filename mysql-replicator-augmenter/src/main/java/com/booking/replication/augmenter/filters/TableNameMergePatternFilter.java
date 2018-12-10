package com.booking.replication.augmenter.filters;

import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Table name rewrite filter - removes part of the table name that
 * matches the provided regex.
 *
 * Example:
 *
 *      Some_Table_201812
 *          with ([_][12]\d{3}(0[1-9]|1[0-2]))
 *      becomes
 *          Some_Table
 *
 *  */
public class TableNameMergePatternFilter implements AugmenterFilter {

    private final String pattern;
    private final Pattern tableNameMergePattern;

    public TableNameMergePatternFilter(Map<String, Object> configuration) {
        pattern = (String) configuration.get(Configuration.FILTER_CONFIGURATION);
        tableNameMergePattern = Pattern.compile(pattern);
    }

    @Override
    public Collection<AugmentedEvent> apply(Collection<AugmentedEvent> augmentedEvents) {

      // TODO
        return null;
    }

    private String getRewrittenName(String originalTableName) {
        Matcher m = tableNameMergePattern.matcher(originalTableName); // TODO: <- cache this
        if (m.find()) {
            String mergedTableName = originalTableName.replaceAll(pattern, "");
            return mergedTableName;
        } else {
            return  originalTableName;
        }
    }
}
