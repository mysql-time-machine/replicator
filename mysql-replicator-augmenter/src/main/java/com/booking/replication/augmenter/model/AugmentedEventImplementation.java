package com.booking.replication.augmenter.model;

import com.booking.replication.supplier.model.EventData;
import com.booking.replication.supplier.model.EventHeader;
import com.booking.replication.supplier.model.EventHeaderV4;

/**
 * Created by bdevetak on 5/4/18.
 */
public class AugmentedEventImplementation<Header extends AugmentedEventHeader, Data extends AugmentedEventData> implements AugmentedEvent {

    private Header header;
    private Data   data;

    public AugmentedEventImplementation(Header header, Data data) {
        this.header = header;
        this.data   = data;
    }

    @Override
    public Header getHeader() {
        return this.header;
    }

    @Override
    public Data getData() {
        return null;
    }
}
