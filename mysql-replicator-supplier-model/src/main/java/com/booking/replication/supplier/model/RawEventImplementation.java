package com.booking.replication.supplier.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UncheckedIOException;

public class RawEventImplementation<Header extends EventHeader, Data extends EventData> implements RawEvent {
    private static ObjectMapper MAPPER = new ObjectMapper();

    private final Header header;
    private final Data data;

    public RawEventImplementation(Header header, Data data) {
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

    @Override
    public void overrideTimestamp(long timestamp) {
        // TODO:
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.overrideTimestamp(timestamp);
    }

    @Override
    public Long getTimestamp() {
        return this.header.getTimestamp();
    }

    @Override
    public String toString() {
        try {
            return RawEventImplementation.MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
