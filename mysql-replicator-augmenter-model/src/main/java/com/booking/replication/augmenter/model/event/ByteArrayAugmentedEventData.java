package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.event.AugmentedEventData;

@SuppressWarnings("unused")
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
