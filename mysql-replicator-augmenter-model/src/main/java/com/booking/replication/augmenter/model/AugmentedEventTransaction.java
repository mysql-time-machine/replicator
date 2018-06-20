package com.booking.replication.augmenter.model;

import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEventTransaction implements Serializable {
    private long timestamp;
    private String identifier;
    private long xxid;

    public AugmentedEventTransaction() {
    }

    public AugmentedEventTransaction(long timestamp, String identifier, long xxid) {
        this.timestamp = timestamp;
        this.identifier = identifier;
        this.xxid = xxid;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public String getIdentifier() {
        return this.identifier;
    }

    public long getXXID() {
        return this.xxid;
    }

    @Override
    public boolean equals(Object object) {
        if (AugmentedEventTransaction.class.isInstance(object)) {
            AugmentedEventTransaction transaction = AugmentedEventTransaction.class.cast(object);

            if (this.timestamp == transaction.timestamp && this.identifier.equals(transaction.identifier) && this.xxid == transaction.xxid) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
