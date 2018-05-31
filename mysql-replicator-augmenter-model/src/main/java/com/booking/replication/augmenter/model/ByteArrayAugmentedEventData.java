package com.booking.replication.augmenter.model;

public class ByteArrayAugmentedEventData implements AugmentedEventData {
    private final byte[] data;

    public ByteArrayAugmentedEventData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return this.data;
    }
}
