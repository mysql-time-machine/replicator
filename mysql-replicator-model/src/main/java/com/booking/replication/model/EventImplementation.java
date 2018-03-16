package com.booking.replication.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UncheckedIOException;

public class EventImplementation<Header extends EventHeader, Data extends EventData> implements Event {
    private static ObjectMapper MAPPER = new ObjectMapper();

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

    @Override
    public String toString() {
        try {
            return EventImplementation.MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
