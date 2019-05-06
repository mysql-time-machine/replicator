package com.booking.replication.applier.count;

import com.booking.replication.applier.Applier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *  This applier simply aggregates the counts of all the events received (to use in tests)
 */
public class CountApplier implements Applier {
    private Map<String, Long> eventCounts;

    public Map<String, Long> getEventCounts() {
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
                    event -> eventCounts.put(event.getHeader().getEventType().name(),
                            eventCounts.get(event.getHeader().getEventType().name()) == null ? 1 :
                            eventCounts.get(event.getHeader().getEventType().name()) + 1)
            );
            return true;
    }
}
