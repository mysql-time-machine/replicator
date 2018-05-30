package com.booking.replication.augmenter.model;

import java.io.Serializable;

public class AugmentedEvent implements Serializable {
    private final AugmentedEventHeader header;
    private final AugmentedEventData data;

    public AugmentedEvent(AugmentedEventHeader header, AugmentedEventData data) {
        this.header = header;
        this.data = data;
    }

    public AugmentedEventHeader getHeader() {
        return this.header;
    }

    public AugmentedEventData getData() {
        return this.data;
    }
}
