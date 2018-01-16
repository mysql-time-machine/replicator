package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

import java.sql.Date;

/**
 * Extracted from https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/DateColumn.java
 */
public class DateCell implements Cell {

    private final java.sql.Date value;

    /**
     *
     */
    public DateCell(java.sql.Date value) {
        this.value = value;
    }

    @Override
    public Date getValue() {
        return value;
    }

    @Override
    public String toString() { return value.toString(); }
}
