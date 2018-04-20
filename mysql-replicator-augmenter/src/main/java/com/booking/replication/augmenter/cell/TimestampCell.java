package com.booking.replication.augmenter.cell;

/**
 * Extracted from: https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/TimestampColumn.java
 */

public final class TimestampCell implements Cell {

    private java.sql.Timestamp value;

    public TimestampCell(java.sql.Timestamp value) {
        this.value = value;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    /**
     *
     */
    public java.sql.Timestamp getValue() {
        return this.value;
    }
}