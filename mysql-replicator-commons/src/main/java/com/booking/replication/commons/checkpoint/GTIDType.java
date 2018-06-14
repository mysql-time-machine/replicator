package com.booking.replication.commons.checkpoint;

@SuppressWarnings("unused")
public enum GTIDType {
    REAL(0),
    PSEUDO(1);

    private final int code;

    GTIDType(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }
}
