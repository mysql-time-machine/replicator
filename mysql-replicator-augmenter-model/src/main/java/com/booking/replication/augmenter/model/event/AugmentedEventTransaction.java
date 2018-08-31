package com.booking.replication.augmenter.model.event;

import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEventTransaction implements Serializable, Comparable<AugmentedEventTransaction> {
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
            return this.compareTo(AugmentedEventTransaction.class.cast(object)) == 0;
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(AugmentedEventTransaction transaction) {
        if (transaction != null) {
            if (this.timestamp == transaction.timestamp) {
                return Long.compare(this.xxid, transaction.xxid);
            } else {
                return Long.compare(this.timestamp, transaction.timestamp);
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }
}
