package com.booking.replication.augmenter.model.event;

import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEventTransaction implements Serializable, Comparable<AugmentedEventTransaction> {

    private final long commitTimestamp;
    private final String identifier;
    private final long xxid;
    private final long transactionSequenceNumber;

    public AugmentedEventTransaction(long commitTimestamp, String identifier, long xxid, long transactionSequenceNumber) {
        this.commitTimestamp = commitTimestamp;
        this.identifier = identifier;
        this.xxid = xxid;
        this.transactionSequenceNumber = transactionSequenceNumber;
    }

    public long getCommitTimestamp() {
        return this.commitTimestamp;
    }

    public String getIdentifier() {
        return this.identifier;
    }

    public long getXXID() {
        return this.xxid;
    }

    public long getTransactionSequenceNumber() {
        return  transactionSequenceNumber;
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
            if (this.commitTimestamp == transaction.commitTimestamp) {
                return Long.compare(this.xxid, transaction.xxid);
            } else {
                return Long.compare(this.commitTimestamp, transaction.commitTimestamp);
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }
}
