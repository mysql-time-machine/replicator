package com.booking.replication.applier.count;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.model.event.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *  This applier simply aggregates the counts of all the events received (to use in tests)
 */
public class CountApplier implements Applier {
    private Map<AugmentedEventType, Long> eventCounts;

    public Map<AugmentedEventType, Long> getEventCounts() {
        return eventCounts;
    }

    public CountApplier(Map<String, Object> configuration) {
        eventCounts = new HashMap<>();
    }

    @Override
    public boolean forceFlush() {
        return false;
    }

    @Override
    public Boolean apply(Collection<AugmentedEvent> events) {
        events.forEach(
            event -> {
                System.out.println("Count Applier got " + event.getHeader().getEventType());

                System.out.println("Count Applier got " + event.getData().toString());

                if (event.getHeader().getEventType().equals(AugmentedEventType.INSERT)) {
                    ((WriteRowsAugmentedEventData) event.getData()).getRows().stream().forEach(row -> {
                        System.out.println(row);
                        System.out.println(row.getTransactionUUID());
                    });
                }
                if (event.getHeader().getEventType().equals(AugmentedEventType.UPDATE)) {
                    ((UpdateRowsAugmentedEventData) event.getData()).getRows().stream().forEach(row -> {
                        System.out.println(row.getTransactionUUID());
                    });
                }
                if (event.getHeader().getEventType().equals(AugmentedEventType.DELETE)) {
                    ((DeleteRowsAugmentedEventData) event.getData()).getRows().stream().forEach(row -> {
                        System.out.println(row.getTransactionUUID());
                    });
                }

                eventCounts.put(
                        event.getHeader().getEventType(),
                        eventCounts.get(event.getHeader().getEventType()) == null ? 1 :
                        eventCounts.get(event.getHeader().getEventType()) + 1
                );
            }
        );

        return true;
    }
}
