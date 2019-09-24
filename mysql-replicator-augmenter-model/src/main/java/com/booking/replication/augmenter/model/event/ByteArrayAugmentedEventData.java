package com.booking.replication.augmenter.model.event;

@SuppressWarnings("unused")
public class ByteArrayAugmentedEventData implements AugmentedEventData {
    private byte[] data;
    private AugmentedEventType eventType;

    public ByteArrayAugmentedEventData() { }

    public ByteArrayAugmentedEventData(AugmentedEventType eventType, byte[] data) {
        this.eventType  = eventType;
        this.data       = data;
    }

    public byte[] getData() {
        return this.data;
    }
}
