package com.booking.replication.augmenter.model;

public class ByteArrayAugmentedEventData implements AugmentedEventData {
    private byte[] data;

    public ByteArrayAugmentedEventData() {
    }

    public ByteArrayAugmentedEventData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return this.data;
    }
}
