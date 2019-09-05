package com.booking.replication.augmenter.model.event;

@SuppressWarnings("unused")
public class ByteArrayAugmentedEventData implements AugmentedEventData {
    private byte[] data;

    public ByteArrayAugmentedEventData() {
    }

    public ByteArrayAugmentedEventData(byte[] data) {
        this.data = data.clone();
    }

    public byte[] getData() {
        return this.data.clone();
    }
}
