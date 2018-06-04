package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public enum AugmentedEventType {
    BYTE_ARRAY(0, ByteArrayAugmentedEventData.class),
    TRANSACTION(4, TransactionAugmentedEventData.class),
    WRITE_ROWS(2, null),
    UPDATE_ROWS(3, null),
    DELETE_ROWS(4, null),
    QUERY(5, null);

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

