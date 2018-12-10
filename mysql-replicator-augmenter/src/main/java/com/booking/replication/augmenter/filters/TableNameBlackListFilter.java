package com.booking.replication.augmenter.filters;

import com.booking.replication.augmenter.AugmenterFilter;
import com.booking.replication.augmenter.model.event.*;

import java.util.*;
import java.util.stream.Collectors;

public class TableNameBlackListFilter implements AugmenterFilter {

    private final List<String> tableBlackList;

    public TableNameBlackListFilter(Map<String, Object> configuration) {
        tableBlackList = Arrays.asList(
                ((String) configuration.get(Configuration.FILTER_CONFIGURATION))
                .replaceAll("\\s","").split(","))
        ;
    }

    @Override
    public Collection<AugmentedEvent> apply(Collection<AugmentedEvent> augmentedEvents) {
        return augmentedEvents.stream()
                .map(
                        ev -> {
                            if (ev.getHeader().getEventType() == AugmentedEventType.WRITE_ROWS) {
                                WriteRowsAugmentedEventData writeEv = ((WriteRowsAugmentedEventData) ev.getData());
                                if (!tableBlackList.contains(writeEv.getEventTable())) {
                                    return ev;
                                }
                            }
                            if (ev.getHeader().getEventType() == AugmentedEventType.UPDATE_ROWS) {
                                UpdateRowsAugmentedEventData updateEv = ((UpdateRowsAugmentedEventData) ev.getData());
                                if (!tableBlackList.contains(updateEv.getEventTable())) {
                                    return ev;
                                }
                            }
                            if (ev.getHeader().getEventType() == AugmentedEventType.DELETE_ROWS) {
                                DeleteRowsAugmentedEventData deleteEv = ((DeleteRowsAugmentedEventData) ev.getData());
                                if (!tableBlackList.contains(deleteEv.getEventTable())) {
                                    return ev;
                                }
                            }
                            return null;
                        }
                )
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
