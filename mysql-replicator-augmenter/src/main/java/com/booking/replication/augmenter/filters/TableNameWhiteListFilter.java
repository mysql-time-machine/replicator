package com.booking.replication.augmenter.filters;

import com.booking.replication.augmenter.Augmenter;
import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.*;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class TableNameWhiteListFilter implements AugmenterFilter {

    private final List<String> tableWhiteList;

    public TableNameWhiteListFilter(Map<String, Object> configuration) {
        tableWhiteList = Arrays.asList(((String) configuration.get(Configuration.FILTER_CONFIGURATION)).split(","));
    }

    @Override
    public Collection<AugmentedEvent> apply(Collection<AugmentedEvent> augmentedEvents) {

        return augmentedEvents.stream()
                .map(
                        ev -> {
                            if (ev.getHeader().getEventType() == AugmentedEventType.WRITE_ROWS) {
                                WriteRowsAugmentedEventData writeEv = ((WriteRowsAugmentedEventData) ev.getData());
                                if (tableWhiteList.contains(writeEv.getEventTable())) {
                                    return ev;
                                } else {
                                    return null;
                                }
                            }
                            if (ev.getHeader().getEventType() == AugmentedEventType.UPDATE_ROWS) {
                                UpdateRowsAugmentedEventData updateEv = ((UpdateRowsAugmentedEventData) ev.getData());
                                if (tableWhiteList.contains(updateEv.getEventTable())) {
                                    return ev;
                                } else {
                                    return null;
                                }
                            }
                            if (ev.getHeader().getEventType() == AugmentedEventType.DELETE_ROWS) {
                                DeleteRowsAugmentedEventData deleteEv = ((DeleteRowsAugmentedEventData) ev.getData());
                                if (tableWhiteList.contains(deleteEv.getEventTable())) {
                                    return ev;
                                } else {
                                    return null;
                                }
                            }
                            return ev;
                        }

                )
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
