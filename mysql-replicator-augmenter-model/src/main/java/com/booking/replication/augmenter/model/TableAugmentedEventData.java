package com.booking.replication.augmenter.model;

public interface TableAugmentedEventData extends AugmentedEventData {
    AugmentedEventTable getEventTable();
}
