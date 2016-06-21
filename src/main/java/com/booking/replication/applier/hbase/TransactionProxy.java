package com.booking.replication.applier.hbase;

import com.booking.replication.applier.TransactionStatus;
import com.booking.replication.augmenter.AugmentedRow;

import java.util.ArrayList;
import java.util.HashMap;

class TransactionProxy extends HashMap<String, ArrayList<AugmentedRow>> {
    private int status = TransactionStatus.OPEN;

    int getStatus() {
        return status;
    }

    void setStatus(int status) {
        this.status = status;
    }
}
