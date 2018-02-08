package com.booking.replication.model.cell;

/**
 * Extracted from: https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/Time2Column.java
 */
public class Time2Cell implements Cell {

    private final java.sql.Time value;

    public Time2Cell(java.sql.Time value) {
        this.value = value;
    }

    @Override
    public java.sql.Time getValue() {
        return value;
    }
}
