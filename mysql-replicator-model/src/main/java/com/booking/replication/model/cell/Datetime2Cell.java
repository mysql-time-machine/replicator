package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

import java.util.Date;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/Datetime2Column.java
 */
public class Datetime2Cell implements Cell {

    private final java.util.Date value;

    public Datetime2Cell(java.util.Date value) {
        this.value = value;
    }

    @Override
    public Date getValue() {
        return value;
    }
}
