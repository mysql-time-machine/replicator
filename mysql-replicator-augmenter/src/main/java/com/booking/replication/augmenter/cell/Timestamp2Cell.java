package com.booking.replication.augmenter.cell;

/**
 * Extracted from: https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/Timestamp2Column.java
 */
public class Timestamp2Cell implements Cell {

    private final java.sql.Timestamp value;

    public Timestamp2Cell(java.sql.Timestamp value) {
        this.value = value;
    }

    @Override
    public java.sql.Timestamp getValue() {
        return value;
    }
}
