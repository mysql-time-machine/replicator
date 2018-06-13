package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public enum AugmentedEventType {
    BYTE_ARRAY(0, ByteArrayAugmentedEventData.class),
    WRITE_ROWS(1, null),
    UPDATE_ROWS(2, null),
    DELETE_ROWS(3, null),
    QUERY(4, null);

    private final int code;
    private final Class<? extends AugmentedEventData> definition;

    AugmentedEventType(int code, Class<? extends AugmentedEventData> definition) {
        this.code = code;
        this.definition = definition;
    }

    public int getCode() {
        return this.code;
    }

    public Class<? extends AugmentedEventData> getDefinition() {
        return this.definition;
    }
}

