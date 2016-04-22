package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;

/**
 * Created by bosko on 4/21/16.
 */
public class HBasePreparedAugmentedRowsEvent {

    private String             hbaseTableName;
    private String             hbaseNamespace;
    private AugmentedRowsEvent augmentedRowsEvent;

    public HBasePreparedAugmentedRowsEvent(String hbaseNamespace, AugmentedRowsEvent augmentedRowsEvent) {
        this.hbaseNamespace     = hbaseNamespace;
        this.hbaseTableName     = hbaseNamespace + ":" + augmentedRowsEvent.getMysqlTableName().toLowerCase();
        this.augmentedRowsEvent = augmentedRowsEvent;
    }

    public String getHbaseNamespace() {
        return hbaseNamespace;
    }

    public AugmentedRowsEvent getAugmentedRowsEvent() {
        return augmentedRowsEvent;
    }

    public String getHbaseTableName() {
        return hbaseTableName;
    }
}
