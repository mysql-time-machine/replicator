package com.booking.replication.augmenter.model;

@SuppressWarnings("unused")
public enum AugmentedEventType {
    BYTE_ARRAY(0),
    WRITE_ROWS(1),
    UPDATE_ROWS(2),
    DELETE_ROWS(3),
    QUERY(4);

    private final int code;

    AugmentedEventType(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }
}

