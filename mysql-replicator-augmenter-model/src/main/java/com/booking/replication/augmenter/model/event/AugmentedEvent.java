package com.booking.replication.augmenter.model.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

@SuppressWarnings("unused")
public class AugmentedEvent implements Serializable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AugmentedEventHeader header;
    private AugmentedEventData data;

    private Object optionalPayload;

    public AugmentedEvent() {
    }

    public AugmentedEvent(AugmentedEventHeader header, AugmentedEventData data) {
        this.header = header;
        this.data = data;
    }

    public AugmentedEventHeader getHeader() {
        return this.header;
    }

    public AugmentedEventData getData() {
        return this.data;
    }

    @JsonIgnore
    public byte[] toJSON() throws IOException {
        return AugmentedEvent.MAPPER.writeValueAsBytes(this);
    }

    public byte[] toJSONPrettyPrint() throws IOException {
        return AugmentedEvent.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
    }

    @JsonIgnore
    public static AugmentedEvent fromJSON(byte[] header, byte[] data) throws IOException {
        AugmentedEventHeader augmentedEventHeader = AugmentedEvent.MAPPER.readValue(header, AugmentedEventHeader.class);
        AugmentedEventData augmentedEventData = AugmentedEvent.MAPPER.readValue(data, augmentedEventHeader.getEventType().getDefinition());

        return new AugmentedEvent(augmentedEventHeader, augmentedEventData);
    }

    public Object getOptionalPayload() {
        return optionalPayload;
    }

    public void setOptionalPayload(Object optionalPayload) {
        this.optionalPayload = optionalPayload;
    }
}
