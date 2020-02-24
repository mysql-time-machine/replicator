package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.event.format.avro.AugmentedEventAvroWrapper;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("unused")
public class AugmentedEvent implements Serializable {

    private AugmentedEventHeader header;
    private AugmentedEventData data;
    private Object optionalPayload;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    {
        Set<String> includeInColumns = new HashSet<>();
        Collections.addAll(includeInColumns, "name", "columnType", "key", "valueDefault", "collation", "nullable");
        SimpleFilterProvider filterProvider = new SimpleFilterProvider();
        filterProvider.addFilter("column", SimpleBeanPropertyFilter.filterOutAllExcept(includeInColumns));
        MAPPER.setFilterProvider(filterProvider);
    }

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


    public List<GenericRecord> dataToAvro() throws IOException {
        AugmentedEventAvroWrapper eventDataPresenterAvro = new AugmentedEventAvroWrapper(this);
        return eventDataPresenterAvro.convertAugmentedEventDataToAvro();
    }


    public byte[] toJSONPrettyPrint() throws IOException {
        return AugmentedEvent.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
    }

    public Object getOptionalPayload() {
        return optionalPayload;
    }

    public void setOptionalPayload(Object optionalPayload) {
        this.optionalPayload = optionalPayload;
    }
}
