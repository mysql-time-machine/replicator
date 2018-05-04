package com.booking.replication.augmenter.model;

import com.booking.replication.augmenter.model.AugmentedRow;
import com.booking.replication.supplier.model.TableNameEventData;

import java.util.List;


public interface AugmentedEventData extends TableNameEventData {

    public void addSingleRowEvent(AugmentedRow au);

    public List<AugmentedRow> getSingleRowEvents();

    public String getBinlogFileName();

}
