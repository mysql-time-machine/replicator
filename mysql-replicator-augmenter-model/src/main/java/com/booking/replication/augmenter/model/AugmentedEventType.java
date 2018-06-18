package com.booking.replication.augmenter.model;

import java.io.Serializable;

@SuppressWarnings("unused")
public enum AugmentedEventType implements Serializable {
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

