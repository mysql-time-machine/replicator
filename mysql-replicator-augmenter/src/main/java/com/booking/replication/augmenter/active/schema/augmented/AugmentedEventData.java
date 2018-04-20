package com.booking.replication.augmenter.active.schema.augmented;

import com.booking.replication.model.TableNameEventData;

import java.util.List;


public interface AugmentedEventData extends TableNameEventData {

    public void addSingleRowEvent(AugmentedRow au);

    public List<AugmentedRow> getSingleRowEvents();

    public String getBinlogFileName();

}
