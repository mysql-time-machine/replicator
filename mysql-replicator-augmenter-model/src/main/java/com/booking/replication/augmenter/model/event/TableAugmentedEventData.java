package com.booking.replication.augmenter.model.event;

@SuppressWarnings("unused")
public interface TableAugmentedEventData extends AugmentedEventData {
    EventMetadata getMetadata();
}
