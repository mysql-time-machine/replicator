package com.booking.replication.augmenter.filters;

import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.event.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.EventMetadata;
import com.booking.replication.augmenter.model.event.RowEventMetadata;
import com.booking.replication.augmenter.model.event.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.commons.metrics.Metrics;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Table name rewrite filter - removes part of the table name that
 * matches the provided regex.
 * Example:
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
                .counter("augmenter.filter.table_name_merge.attempt").inc(1L);

        Collection<AugmentedEvent> filtered = augmentedEvents.stream()
                .map(ev -> {
                    if (ev.getHeader().getEventType() == AugmentedEventType.INSERT) {
                        WriteRowsAugmentedEventData writeEv = ((WriteRowsAugmentedEventData) ev.getData());
                        EventMetadata metadata = writeEv.getMetadata();
                        String originalName = metadata.getEventTable().getName();
                        String rewrittenName = getRewrittenName(originalName);
                        // override
                        metadata.getEventTable().setName(rewrittenName);
                        writeEv.getRows().stream().forEach(au -> au.setTableName(rewrittenName));
                        //writeEv.getRows().stream().forEach(au -> au.setOriginalTableName(originalName));
                    }
                    if (ev.getHeader().getEventType() == AugmentedEventType.UPDATE) {
                        UpdateRowsAugmentedEventData updateEv = ((UpdateRowsAugmentedEventData) ev.getData());
                        EventMetadata metadata = updateEv.getMetadata();
                        String originalName = metadata.getEventTable().getName();
                        String rewrittenName = getRewrittenName(originalName);
                        // override
                        metadata.getEventTable().setName(rewrittenName);
                        updateEv.getRows().stream().forEach(au -> au.setTableName(rewrittenName));
                        //updateEv.getRows().stream().forEach(au -> au.setOriginalTableName(originalName));
                    }
                    if (ev.getHeader().getEventType() == AugmentedEventType.DELETE) {
                        DeleteRowsAugmentedEventData deleteEv = ((DeleteRowsAugmentedEventData) ev.getData());
                        EventMetadata metadata = deleteEv.getMetadata();

                        String originalName = metadata.getEventTable().getName();
                        String rewrittenName = getRewrittenName(originalName);
                        // override
                        metadata.getEventTable().setName(rewrittenName);
                        deleteEv.getRows().stream().forEach(au -> au.setTableName(rewrittenName));
                       // deleteEv.getRows().stream().forEach(au -> au.setOriginalTableName(originalName));
                    }
                    return ev;
                })
                .collect(Collectors.toList());

        metrics.getRegistry()
                .counter("augmenter.filter.table_name_merge.success").inc(1L);

        metrics.getRegistry()
                .counter("augmenter.filter.table_name_merge.filtered_length").inc( filtered.size() );

        return filtered;
    }

    public String getRewrittenName(String originalTableName) {
        Matcher matcher = tableNameSufixPattern.matcher(originalTableName); // TODO: <- cache this
        if (matcher.find()) {
            String mergedTableName = originalTableName.replaceAll(pattern, "");
            return mergedTableName;
        } else {
            return  originalTableName;
        }
    }
}
