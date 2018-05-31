package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public enum AugmentedEventType {
    BYTE_ARRAY(0, ByteArrayAugmentedEventData.class);

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

