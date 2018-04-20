package com.booking.replication.augmenter.cell;

/**
 * Extracted from: https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/TimeColumn.java
 */
public class TimeCell implements Cell {

    private final java.sql.Time value;

    public TimeCell(java.sql.Time value) {
        this.value = value;
    }

    @Override
    public java.sql.Time getValue() {
        return value;
    }
}
