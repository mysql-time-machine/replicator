package com.booking.replication.augmenter.model;

import java.util.List;

/**
 * Created by bdevetak on 4/20/18.
 */
public class PseudoGTIDEventData implements AugmentedEventData {

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public void addSingleRowEvent(AugmentedRow au) {

    }

    @Override
    public List<AugmentedRow> getSingleRowEvents() {
        return null;
    }

    @Override
    public String getBinlogFileName() {
        return null;
    }
}
