package com.booking.replication.augmenter.model;

import com.booking.replication.supplier.model.EventData;
import com.booking.replication.supplier.model.EventHeader;
import com.booking.replication.supplier.model.EventHeaderV4;

/**
 * Created by bdevetak on 5/4/18.
 */
public class AugmentedEventImplementation implements AugmentedEvent {

    EventHeader header;
    EventData   data;

    public AugmentedEventImplementation(EventHeader header, EventData data) {
        this.header = header;
        this.data   = data;
    }

    @Override
    public <Header extends AugmentedEventHeader> Header getHeader() {
        return null;
    }

    @Override
    public <Data extends AugmentedEventData> Data getData() {
        return null;
    }

    @Override
    public String getTableName() {
        return null; // TODO
    }
}
