package com.booking.replication.augmenter.model;

import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEvent implements Serializable {
    private AugmentedEventHeader header;
    private AugmentedEventData data;

    public AugmentedEvent() {
    }

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
