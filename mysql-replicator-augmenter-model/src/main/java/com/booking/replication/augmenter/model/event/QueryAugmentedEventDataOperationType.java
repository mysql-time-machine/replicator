package com.booking.replication.augmenter.model.event;

import java.io.Serializable;

@SuppressWarnings("unused")
public enum QueryAugmentedEventDataOperationType implements Serializable {
    CREATE(0),
    ALTER(1),
    DROP(2),
    RENAME(3),
    TRUNCATE(4),
    MODIFY(5),
    ANALYZE(6);

    private final int code;

    QueryAugmentedEventDataOperationType(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }
}
