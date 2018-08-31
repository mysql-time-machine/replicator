package com.booking.replication.augmenter.model.event;

import java.io.Serializable;

@SuppressWarnings("unused")
public enum QueryAugmentedEventDataType implements Serializable {
    BEGIN(0),
    COMMIT(1),
    DDL_DEFINER(2),
    DDL_TABLE(3),
    DDL_TEMPORARY_TABLE(4),
    DDL_VIEW(5),
    DDL_ANALYZE(6),
    GTID(7),
    PSEUDO_GTID(8);

    private final int code;

    QueryAugmentedEventDataType(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }
}
