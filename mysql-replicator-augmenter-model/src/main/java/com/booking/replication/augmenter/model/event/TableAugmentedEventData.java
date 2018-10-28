package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.schema.FullTableName;


@SuppressWarnings("unused")
public interface TableAugmentedEventData extends AugmentedEventData {
    FullTableName getEventTable();
}
