package com.booking.replication.augmenter.filters;

import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.*;
import com.booking.replication.augmenter.model.schema.SchemaSnapshot;
import com.booking.replication.commons.metrics.Metrics;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private final Pattern tableNameSufixPattern;
    private Metrics<?> metrics;

    public TableNameMergePatternFilter(Map<String, Object> configuration) {
        pattern = (String) configuration.get(Configuration.FILTER_CONFIGURATION);
        tableNameSufixPattern = Pattern.compile(pattern);
        metrics = Metrics.getInstance(configuration);
    }

    @Override
    public Collection<AugmentedEvent> apply(Collection<AugmentedEvent> augmentedEvents) {
        metrics.getRegistry()
                .counter("hbase.augmenter.filter.table_name_merge.attempt").inc(1L);

        Collection<AugmentedEvent> filtered = augmentedEvents.stream()
                .map(
                        ev -> {
                            if (ev.getHeader().getEventType() == AugmentedEventType.WRITE_ROWS) {
                                WriteRowsAugmentedEventData writeEv = ((WriteRowsAugmentedEventData) ev.getData());
                                String originalName = writeEv.getEventTable().getName();
                                String rewrittenName = getRewrittenName(originalName);
                                // override
                                writeEv.getEventTable().setName(rewrittenName);
                                writeEv.getAugmentedRows().stream().forEach(au -> au.setTableName(rewrittenName));
                            }
                            if (ev.getHeader().getEventType() == AugmentedEventType.UPDATE_ROWS) {
                                UpdateRowsAugmentedEventData updateEv = ((UpdateRowsAugmentedEventData) ev.getData());
                                String originalName = updateEv.getEventTable().getName();
                                String rewrittenName = getRewrittenName(originalName);
                                // override
                                updateEv.getEventTable().setName(rewrittenName);
                                updateEv.getEventTable().setName(rewrittenName);
                                updateEv.getAugmentedRows().stream().forEach(au -> au.setTableName(rewrittenName));
                            }
                            if (ev.getHeader().getEventType() == AugmentedEventType.DELETE_ROWS) {
                                DeleteRowsAugmentedEventData deleteEv = ((DeleteRowsAugmentedEventData) ev.getData());
                                String originalName = deleteEv.getEventTable().getName();
                                String rewrittenName = getRewrittenName(originalName);
                                // override
                                deleteEv.getEventTable().setName(rewrittenName);
                                deleteEv.getEventTable().setName(rewrittenName);
                                deleteEv.getAugmentedRows().stream().forEach(au -> au.setTableName(rewrittenName));
                            }
                            return ev;
                        }
                )
                .collect(Collectors.toList());

        metrics.getRegistry()
                .counter("hbase.augmenter.filter.table_name_merge.success").inc(1L);

        metrics.getRegistry()
                .counter("hbase.augmenter.filter.table_name_merge.filtered_length").inc( filtered.size() );

        return filtered;
    }

    public String getRewrittenName(String originalTableName) {
        Matcher m = tableNameSufixPattern.matcher(originalTableName); // TODO: <- cache this
        if (m.find()) {
            String mergedTableName = originalTableName.replaceAll(pattern, "");
            return mergedTableName;
        } else {
            return  originalTableName;
        }
    }
}
