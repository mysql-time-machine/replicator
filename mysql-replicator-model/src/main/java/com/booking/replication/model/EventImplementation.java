package com.booking.replication.model;

import com.booking.replication.model.Event;
import com.booking.replication.model.EventData;
import com.booking.replication.model.EventHeader;

public class EventImplementation<Header extends EventHeader, Data extends EventData> implements Event {
    private Header header;
    private Data data;

    @Override
    @SuppressWarnings("unchecked")
    public Header getHeader() {
        return this.header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Data getData() {
        return this.data;
    }

    public void setData(Data data) {
        this.data = data;
    }
}
