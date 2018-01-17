package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

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

    @Override
    public String toString() {
        return Long.toString(value.getTime());
    }
}
