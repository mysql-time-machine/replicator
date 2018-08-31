package com.booking.replication.augmenter.model.event;

import java.io.Serializable;

@SuppressWarnings("unused")
public enum AugmentedEventType implements Serializable {
    BYTE_ARRAY(0, ByteArrayAugmentedEventData.class),
    WRITE_ROWS(1, WriteRowsAugmentedEventData.class),
    UPDATE_ROWS(2, UpdateRowsAugmentedEventData.class),
    DELETE_ROWS(3, DeleteRowsAugmentedEventData.class),
    QUERY(4, QueryAugmentedEventData.class);

    private final int code;
    private final Class<? extends AugmentedEventData> definition;

    AugmentedEventType(int code, Class<? extends AugmentedEventData> definition) {
        this.code = code;
        this.definition = definition;
    }

    public int getCode() {
        return this.code;
    }

    // used by clients to deserialize the data
    public Class<? extends AugmentedEventData> getDefinition() {
        return this.definition;
    }
}

