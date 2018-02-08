package com.booking.replication.model;

public class EventImplementation<Header extends EventHeader, Data extends EventData> implements Event {
    private final Header header;
    private final Data data;

    public EventImplementation(Header header, Data data) {
        this.header = header;
        this.data = data;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Header getHeader() {
        return this.header;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Data getData() {
        return this.data;
    }
}
