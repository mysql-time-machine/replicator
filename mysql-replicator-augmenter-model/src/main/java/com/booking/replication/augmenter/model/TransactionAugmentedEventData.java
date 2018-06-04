package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public class TransactionAugmentedEventData implements AugmentedEventData {
    private AugmentedEventData[] data;

    public TransactionAugmentedEventData() {
    }

    public TransactionAugmentedEventData(AugmentedEventData[] data) {
        this.data = data;
    }

    public AugmentedEventData[] getData() {
        return this.data;
    }
}
