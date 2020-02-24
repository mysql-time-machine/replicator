package com.booking.replication.applier.message.format.avro;

public class SerializedEvent {

    private final Integer schemaId;
    private final byte[] eventDataAvroBlob;

    public SerializedEvent(Integer schemaId, byte[] eventDataAvroBlob) {
        this.schemaId = schemaId;
        this.eventDataAvroBlob = eventDataAvroBlob;
    }

    public Integer getSchemaId() {
        return schemaId;
    }

    public byte[] getEventDataAvroBlob() {
        return eventDataAvroBlob;
    }
}
