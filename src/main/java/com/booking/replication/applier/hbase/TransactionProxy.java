package com.booking.replication.applier.hbase;

import com.booking.replication.augmenter.AugmentedRow;

import java.util.ArrayList;
import java.util.HashMap;

class TransactionProxy extends HashMap<String, ArrayList<AugmentedRow>> {
    private Boolean isReady = false;

    Boolean isReadyForCommit() {
        return isReady;
    }

    void setReadyForCommit() {
        this.isReady = true;
    }
}
